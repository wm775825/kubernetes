package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	transport2 "k8s.io/client-go/transport"
)

type FleetClientSet struct {
	clusterGetter  func(string) (*clusterv1alpha1.Cluster, error)
	clustersLister func() ([]*clusterv1alpha1.Cluster, error)
	secretGetter   func(string, string) (*corev1.Secret, error)

	LoopbackConfig   *rest.Config
	karmadaLocation  *url.URL
	karmadaTransport http.RoundTripper

	codec runtime.Codec

	NewFunc     func() runtime.Object
	NewListFunc func() runtime.Object
	Versioner   storage.APIObjectVersioner
}

func (f *FleetClientSet) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no request info found from ctx")
	}

	resourceName, clusterName := ParseNameFromResourceName(info.Name, info.Clusterspace != request.ClusterspaceNone)
	info.Name = resourceName
	if info.Clusterspace != "" && clusterName != "" && info.Clusterspace != clusterName {
		return nil, fmt.Errorf("inconsistent cluster name parsed from path and resource name")
	}
	if clusterName == "" {
		clusterName = info.Clusterspace
	}

	req, ok := request.RequestFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no http request found from ctx")
	}

	location, transport, err := f.location(clusterName)
	if err != nil {
		return nil, err
	}
	newURL := constructURLPath(location, info)
	if options != nil && len(options.ResourceVersion) != 0 {
		rv := newMultiClusterResourceVersionFromString(options.ResourceVersion).get(clusterName)
		if len(rv) != 0 {
			newURL += fmt.Sprintf("?resourceVersion=%s", rv)
		}
	}

	newReq, err := http.NewRequest(http.MethodGet, newURL, nil)
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header.Clone()
	newReq.Header.Set("Accept", "application/json")

	resp, err := transport.RoundTrip(newReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		status := metav1.Status{}
		if err = decode(f.codec, b, &status); err != nil {
			return nil, fmt.Errorf("failed to decode status: %v", err)
		}
		return nil, InterpretGetError(&apierrors.StatusError{ErrStatus: status}, schema.GroupResource{Group: info.APIGroup, Resource: info.Resource}, resourceName, "")
	}
	objPtr := f.NewFunc()
	if err = decode(f.codec, b, objPtr); err != nil {
		return nil, err
	}

	accessor, err := meta.Accessor(objPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}
	if err = f.validateMinimumResourceVersion(options.ResourceVersion, accessor.GetResourceVersion()); err != nil {
		return nil, err
	}
	if clusterName != KarmadaCluster {
		accessor.SetName(accessor.GetName() + ".clusterspace." + clusterName)
	}
	mrv := newMultiClusterResourceVersionWithCapacity(1)
	mrv.set(clusterName, accessor.GetResourceVersion())
	accessor.SetResourceVersion(mrv.String())
	return objPtr, nil
}

// referenceFieldSelectors contains field selectors that refer to resources
// collected from k8s.io/kubernetes/pkg/apis/<group>/<version>/conversion.go
var referenceFieldSelectors = sets.NewString("metadata.name", "metadata.namespace", // common
	"spec.nodeName", "spec.serviceAccountName", "status.nominatedNodeName", // pod
	"involvedObject.name", "involvedObject.namespace") // event

func removeClusterNameInReferenceField(fs fields.Selector) fields.Selector {
	requirements := fs.Requirements()
	selectors := make([]fields.Selector, 0, len(requirements))
	for _, require := range requirements {
		if referenceFieldSelectors.Has(require.Field) {
			require.Value, _ = ParseNameFromResourceName(require.Value, false)
		}
		if require.Operator == selection.NotEquals {
			selectors = append(selectors, fields.OneTermNotEqualSelector(require.Field, require.Value))
		} else {
			selectors = append(selectors, fields.OneTermEqualSelector(require.Field, require.Value))
		}
	}
	return fields.AndSelectors(selectors...)
}

func (f *FleetClientSet) ListPredicate(ctx context.Context, p storage.SelectionPredicate, options *metainternalversion.ListOptions) (runtime.Object, error) {
	// specifying resource version(match) is not allowed when using continue
	if len(options.Continue) != 0 && len(options.ResourceVersion)+len(options.ResourceVersionMatch) > 0 {
		return nil, apierrors.NewBadRequest("specifying resource version(match) is not allowed when using continue")
	}

	info, ok := request.RequestInfoFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no request info found from ctx")
	}

	clusters, err := f.clustersLister()
	if err != nil {
		return nil, err
	}
	clusters = append(clusters, &clusterv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: KarmadaCluster}})
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	listOptions := &metav1.ListOptions{
		FieldSelector:        removeClusterNameInReferenceField(p.Field).String(),
		LabelSelector:        p.Label.String(),
		Limit:                options.Limit,
		Continue:             options.Continue,
		ResourceVersion:      options.ResourceVersion,
		ResourceVersionMatch: options.ResourceVersionMatch,
	}
	requestCluster, listOptions, mrv := prepareBeforeList(listOptions)
	responseRV := mrv.clone()
	responseContinue := multiClusterContinue{}

	items := make([]runtime.Object, 0, int(math.Min(float64(listOptions.Limit), 1024)))
	listFunc := func(cluster *clusterv1alpha1.Cluster) (num int, cont string, err error) {
		clusterName := cluster.Name
		if requestCluster != "" && requestCluster != clusterName {
			return 0, "", nil
		}
		defer func() {
			// clear the requestContinue to allow list resources from other clusters
			requestCluster = ""
			listOptions.Continue = ""
		}()

		if listOptions.Continue != "" {
			// specifying resource version is not allowed when using continue
			listOptions.ResourceVersion = ""
		} else {
			// if no continue, resourceVersion may set
			listOptions.ResourceVersion = mrv.get(clusterName)
		}

		objList, err := f.list(clusterName, info, listOptions)
		if err != nil {
			return 0, "", err
		}
		listAccessor, err := meta.ListAccessor(objList)
		if err != nil {
			return 0, "", err
		}
		responseRV.set(clusterName, listAccessor.GetResourceVersion())

		if err = meta.EachListItem(objList, func(object runtime.Object) error {
			clone := object.DeepCopyObject()
			accessor, err := meta.Accessor(clone)
			if err != nil {
				return err
			}
			if clusterName != KarmadaCluster {
				accessor.SetName(accessor.GetName() + ".clusterspace." + clusterName)
			}
			accessor.SetResourceVersion(responseRV.String())

			items = append(items, clone)
			num++
			return nil
		}); err != nil {
			return 0, "", err
		}
		return num, listAccessor.GetContinue(), nil
	}

	if listOptions.Limit == 0 {
		// no limit, list from the specified cluster or first cluster to the end
		for _, cluster := range clusters {
			if _, _, err = listFunc(cluster); err != nil {
				return nil, err
			}
		}
	} else {
		for index, cluster := range clusters {
			n, cont, err := listFunc(cluster)
			if err != nil {
				return nil, err
			}
			listOptions.Limit -= int64(n)
			if listOptions.Limit <= 0 {
				// limit <= 0  =>  stop
				if cont != "" {
					// continue != ""  =>  current cluster has remaining items, return this cluster name and continue for next list.
					responseContinue.Cluster = cluster.Name
					responseContinue.Continue = cont
				} else if index < len(clusters)-1 {
					// Current cluster has no remaining items. But we don't know whether next cluster has.
					// So return the next cluster name for continue listing.
					responseContinue.Cluster = clusters[index+1].Name
				}
				// No more items remain. Break the chuck list.
				break
			}
		}
	}

	result := f.NewListFunc()
	if err = meta.SetList(result, items); err != nil {
		return nil, err
	}
	accessor, err := meta.ListAccessor(result)
	if err != nil {
		return nil, err
	}
	accessor.SetResourceVersion(responseRV.String())
	accessor.SetContinue(responseContinue.String())
	return result, nil
}

func (f *FleetClientSet) list(clusterName string, info *request.RequestInfo, opts *metav1.ListOptions) (runtime.Object, error) {
	location, transport, err := f.location(clusterName)
	if err != nil {
		return nil, err
	}
	path := constructURLPath(location, info)
	values, err := metav1.ParameterCodec.EncodeParameters(opts, schema.GroupVersion{})
	if err != nil {
		return nil, err
	}
	queries := values.Encode()
	if queries != "" {
		path += "?" + queries
	}
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	obj := f.NewListFunc()
	if err = decode(f.codec, b, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (f *FleetClientSet) validateMinimumResourceVersion(minimumResourceVersion, actualResourceVersion string) error {
	if minimumResourceVersion == "" {
		return nil
	}
	minimumRV, err := f.Versioner.ParseResourceVersion(minimumResourceVersion)
	if err != nil {
		return apierrors.NewBadRequest(fmt.Sprintf("invalid resource version: %v", err))
	}
	actualRV, err := f.Versioner.ParseResourceVersion(actualResourceVersion)
	if err != nil {
		return apierrors.NewInternalError(fmt.Errorf("invalid resource version: %v", err))
	}
	// Enforce the storage.Interface guarantee that the resource version of the returned data
	// "will be at least 'resourceVersion'".
	if minimumRV > actualRV {
		return storage.NewTooLargeResourceVersionError(minimumRV, actualRV, 0)
	}
	return nil
}

func (f *FleetClientSet) dynamicClientFor(clusterName string) (*dynamic.DynamicClient, error) {
	restConfig, err := f.restConfigFor(clusterName)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(restConfig)
}

func (f *FleetClientSet) location(clusterName string) (*url.URL, http.RoundTripper, error) {
	if clusterName == KarmadaCluster {
		return f.karmadaLocation, f.karmadaTransport, nil
	}

	restConfig, err := f.restConfigFor(clusterName)
	if err != nil {
		return nil, nil, err
	}
	location, err := url.Parse(restConfig.Host)
	if err != nil {
		return nil, nil, err
	}
	return normalizeLocation(location), transport2.NewBearerAuthRoundTripper(restConfig.BearerToken, restConfig.Transport), nil
}

func (f *FleetClientSet) restConfigFor(clusterName string) (*rest.Config, error) {
	if clusterName == KarmadaCluster {
		return f.LoopbackConfig, nil
	}

	cluster, err := f.clusterGetter(clusterName)
	if err != nil {
		return nil, InterpretGetClusterError(err, clusterName)
	}
	apiEndpoint := cluster.Spec.APIEndpoint
	if apiEndpoint == "" {
		return nil, fmt.Errorf("the api endpoint of cluster %s is empty", clusterName)
	}

	if cluster.Spec.SecretRef == nil {
		return nil, fmt.Errorf("cluster %s does not have a secret", clusterName)
	}

	secret, err := f.secretGetter(cluster.Spec.SecretRef.Namespace, cluster.Spec.SecretRef.Name)
	if err != nil {
		return nil, err
	}
	caBundle, found := secret.Data[clusterv1alpha1.SecretCADataKey]
	if !found {
		return nil, fmt.Errorf("the CA bundle of cluster %s is empty", clusterName)
	}
	token, found := secret.Data["token"]
	if !found {
		return nil, fmt.Errorf("token not found")
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caBundle)
	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: cluster.Spec.InsecureSkipTLSVerification,
	}

	var proxyDialerFn utilnet.DialFunc
	trans := utilnet.SetTransportDefaults(&http.Transport{
		DialContext:     proxyDialerFn,
		TLSClientConfig: tlsConfig,
	})

	if proxyURL := cluster.Spec.ProxyURL; proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse url of proxy url %s: %v", proxyURL, err)
		}
		trans.Proxy = http.ProxyURL(u)
		trans.ProxyConnectHeader = ParseProxyHeaders(cluster.Spec.ProxyHeader)
	}

	return &rest.Config{
		BearerToken: string(token),
		Host:        apiEndpoint,
		Transport:   trans,
	}, nil
}

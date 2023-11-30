package registry

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	"net/http"
	"net/url"
	"strconv"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
)

type FleetClientset struct {
	clusterGetter  func(string) (*clusterv1alpha1.Cluster, error)
	clustersLister func() ([]*clusterv1alpha1.Cluster, error)
	secretGetter   func(string, string) (*corev1.Secret, error)
	LoopbackConfig *rest.Config

	karmadaLocation  *url.URL
	karmadaTransport http.RoundTripper

	codec       runtime.Codec
	NewFunc     func() runtime.Object
	NewListFunc func() runtime.Object
	Versioner   storage.APIObjectVersioner
}

func (f *FleetClientset) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
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

	dynamicClient, err := f.dynamicClientFor(clusterName)
	if err != nil {
		return nil, err
	}
	resourceClient := dynamicClient.Resource(schema.GroupVersionResource{Group: info.APIGroup, Version: info.APIVersion, Resource: info.Resource})

	var result *unstructured.Unstructured
	if info.Namespace != "" && info.Resource != "namespaces" {
		result, err = resourceClient.Namespace(info.Namespace).Get(ctx, info.Name, *options)
	} else {
		result, err = resourceClient.Get(ctx, info.Name, *options)
	}
	if err != nil {
		return nil, err
	}

	restClient, _ := rest.RESTClientFor(f.LoopbackConfig)
	r := restClient.Get().AbsPath().VersionedParams().Do(ctx)
	r.Raw()

	b, err := result.MarshalJSON()
	if err != nil {
		return nil, err
	}

	objPtr := f.NewFunc()
	if err = decode(f.codec, b, objPtr); err != nil {
		return nil, err
	}
	accessor, err := meta.Accessor(objPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %v", err)
	}
	if clusterName != KarmadaCluster {
		accessor.SetName(accessor.GetName() + ".clusterspace." + clusterName)
	}
	return objPtr, nil

	//location, transport, err := f.location(clusterName)
	//if err != nil {
	//	return nil, err
	//}
	//newURL := constructURLPath(location, info)
	//if req.URL.RawQuery != "" {
	//	newURL += "?" + req.URL.RawQuery // todo: parse multi cluster resourceVersion
	//}
	//
	//newReq, err := http.NewRequest(http.MethodGet, newURL, nil)
	//if err != nil {
	//	return nil, err
	//}
	//newReq.Header = req.Header.Clone()
	//newReq.Header.Set("Accept", "application/json")
	//
	//resp, err := transport.RoundTrip(newReq)
	//if err != nil {
	//	return nil, err
	//}
	//defer resp.Body.Close()
	//
	//b, err := io.ReadAll(resp.Body)
	//if err != nil {
	//	return nil, err
	//}
	//if resp.StatusCode != http.StatusOK {
	//	status := metav1.Status{}
	//	if err = decode(f.codec, b, &status); err != nil {
	//		return nil, fmt.Errorf("failed to decode status: %v", err)
	//	}
	//	return nil, InterpretGetError(&apierrors.StatusError{ErrStatus: status}, schema.GroupResource{Group: info.APIGroup, Resource: info.Resource}, resourceName, "")
	//}
	//objPtr := f.NewFunc()
	//if err = decode(f.codec, b, objPtr); err != nil {
	//	return nil, err
	//}
	//
	//accessor, err := meta.Accessor(objPtr)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to get metadata: %v", err)
	//}
	//if err = f.validateMinimumResourceVersion(options.ResourceVersion, accessor.GetResourceVersion()); err != nil {
	//	return nil, err
	//}
	//if clusterName != KarmadaCluster {
	//	accessor.SetName(accessor.GetName() + ".clusterspace." + clusterName)
	//}
	//return objPtr, nil
}

// referenceFieldSelectors contains field selectors that refer to sources
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

func listOptions2QueryString(options *metav1.ListOptions) string {
	values := url.Values{}
	if options.Limit != 0 {
		values.Set("limit", strconv.FormatInt(options.Limit, 10))
	}
	if len(options.Continue) != 0 {
		values.Set("continue", options.Continue)
	}
	if len(options.LabelSelector) != 0 {
		values.Set("labelSelector", options.LabelSelector)
	}
	if len(options.FieldSelector) != 0 {
		values.Set("fieldSelector", options.FieldSelector)
	}
	if len(options.ResourceVersion) != 0 {
		values.Set("resourceVersion", options.ResourceVersion)
	}
	if len(options.ResourceVersionMatch) != 0 {
		values.Set("resourceVersionMatch", string(options.ResourceVersionMatch))
	}
	if options.Watch {
		values.Set("watch", "true")
	}
	if options.AllowWatchBookmarks {
		values.Set("allowWatchBookmarks", "true")
	}
	if options.TimeoutSeconds != nil {
		values.Set("timeoutSeconds", strconv.FormatInt(*options.TimeoutSeconds, 10))
	}
	if options.SendInitialEvents != nil && *options.SendInitialEvents == true {
		values.Set("sendInitialEvents", "true")
	}
	return values.Encode()
}

func (f *FleetClientset) ListPredicate(ctx context.Context, p storage.SelectionPredicate, options *metainternalversion.ListOptions) (runtime.Object, error) {
	// specifying resource version(match) is not allowed when using continue
	if len(options.Continue) != 0 && (len(options.ResourceVersion)+len(options.ResourceVersionMatch) == 0) {
		return nil, apierrors.NewBadRequest("specifying resource version(match) is not allowed when using continue")
	}

	listOptions := &metav1.ListOptions{
		FieldSelector:        removeClusterNameInReferenceField(p.Field).String(),
		LabelSelector:        p.Label.String(),
		Limit:                options.Limit,
		Continue:             options.Continue,
		ResourceVersion:      options.ResourceVersion,
		ResourceVersionMatch: options.ResourceVersionMatch,
	}
	_ = listOptions

	return nil, nil
}

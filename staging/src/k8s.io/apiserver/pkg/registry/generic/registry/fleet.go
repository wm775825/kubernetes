package registry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	clientgotransport "k8s.io/client-go/transport"
)

type FleetClientSet struct {
	clusterGetter  func(string) (*clusterv1alpha1.Cluster, error)
	clustersLister func() ([]*clusterv1alpha1.Cluster, error)
	secretGetter   func(string, string) (*corev1.Secret, error)

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
	if req.URL.RawQuery != "" {
		newURL += "?" + req.URL.RawQuery // todo: parse multi cluster resourceVersion
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
		if apierrors.IsNotFound(&apierrors.StatusError{ErrStatus: status}) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Group: info.APIGroup, Resource: info.Resource}, resourceName)
		}
		return nil, fmt.Errorf(status.Message)
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
	return objPtr, nil
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

func (f *FleetClientSet) location(clusterName string) (*url.URL, http.RoundTripper, error) {
	if clusterName == KarmadaCluster {
		return f.karmadaLocation, f.karmadaTransport, nil
	}
	cluster, err := f.clusterGetter(clusterName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("failed to get cluster %s: %v", clusterName, err)
	}
	tlsConfig, err := GetTlsConfigForCluster(cluster, f.secretGetter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get tls config for cluster %s: %v", clusterName, err)
	}
	location, transport, err := Location(cluster, tlsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get transport of cluster %s: %v", clusterName, err)
	}
	location = normalizeLocation(location)
	secret, err := f.secretGetter(cluster.Spec.SecretRef.Namespace, cluster.Spec.SecretRef.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get secret of cluster %s: %v", clusterName, err)
	}
	token, exists := secret.Data["token"]
	if !exists {
		return nil, nil, fmt.Errorf("token not found")
	}
	return location, clientgotransport.NewBearerAuthRoundTripper(string(token), transport), nil
}

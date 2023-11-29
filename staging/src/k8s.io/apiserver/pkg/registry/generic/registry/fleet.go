package registry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
)

type FleetClientset struct {
	clusterGetter  func(string) (*clusterv1alpha1.Cluster, error)
	clustersLister func() ([]*clusterv1alpha1.Cluster, error)
	secretGetter   func(string, string) (*corev1.Secret, error)

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

	req, ok := request.RequestFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("no http request found from ctx")
	}

	location, transport, err := f.location(clusterName)
	if err != nil {
		return nil, fmt.Errorf("failed to get transport of cluster %s: %v", clusterName, err)
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

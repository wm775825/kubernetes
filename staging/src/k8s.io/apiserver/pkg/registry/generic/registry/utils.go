package registry

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
)

const (
	KarmadaCluster = "karmada"

	ClusterSpaceSeparator = "clusterspace"
)

func ParseNameFromResourceName(name string, isNotCompatibleAPI bool) (resourceName string, clusterName string) {
	parts := strings.Split(name, ".")
	if len(parts) < 3 || parts[len(parts)-2] != ClusterSpaceSeparator {
		if isNotCompatibleAPI {
			return name, ""
		} else {
			return name, KarmadaCluster
		}
	}
	return strings.Join(parts[:len(parts)-2], "."), parts[len(parts)-1]
}

func constructURLPath(location *url.URL, info *request.RequestInfo) string {
	if !info.IsResourceRequest {
		return location.String() + path.Join("/", info.Path)
	}
	parts := []string{"/", info.APIPrefix, info.APIGroup, info.APIVersion}
	if info.Namespace != "" && info.Resource != "namespaces" {
		parts = append(parts, "namespaces", info.Namespace)
	}
	parts = append(parts, info.Resource, info.Name, info.Subresource)
	parts = append(parts, info.PartsAfterSubresource...)
	return location.String() + path.Join(parts...)
}

func decode(codec runtime.Codec, value []byte, objPtr runtime.Object) error {
	if _, err := conversion.EnforcePtr(objPtr); err != nil {
		return fmt.Errorf("unable to convert output object to pointer: %v", err)
	}
	_, _, err := codec.Decode(value, nil, objPtr)
	if err != nil {
		return err
	}
	return nil
}

func ParseProxyHeaders(proxyHeaders map[string]string) http.Header {
	if len(proxyHeaders) == 0 {
		return nil
	}

	header := http.Header{}
	for headerKey, headerValues := range proxyHeaders {
		values := strings.Split(headerValues, ",")
		header[headerKey] = values
	}
	return header
}

func normalizeLocation(location *url.URL) *url.URL {
	normalized, _ := url.Parse(location.String())
	if len(normalized.Scheme) == 0 {
		normalized.Scheme = "http"
	}
	return normalized
}

type skipFunc func(string) bool

type unionSkip []skipFunc

func (u unionSkip) skip(cluster string) bool {
	for _, f := range u {
		if f(cluster) {
			return true
		}
	}
	return false
}

func specifyCluster(specified string) skipFunc {
	return func(cluster string) bool {
		if specified != "" {
			return cluster != specified
		}
		return false
	}
}

func unready(clusters []*clusterv1alpha1.Cluster) (skipFunc, sets.String) {
	unreadyClusters := sets.NewString()
	for _, cluster := range clusters {
		if !IsClusterReady(&cluster.Status) {
			unreadyClusters.Insert(cluster.Name)
		}
	}
	return func(cluster string) bool { return unreadyClusters.Has(cluster) }, unreadyClusters
}

func IsClusterReady(clusterStatus *clusterv1alpha1.ClusterStatus) bool {
	return meta.IsStatusConditionTrue(clusterStatus.Conditions, clusterv1alpha1.ClusterConditionReady)
}

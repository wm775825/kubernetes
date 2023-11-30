package registry

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
	transport2 "k8s.io/client-go/transport"
)

const (
	KarmadaCluster = "karmada"
)

type SecretGetterFunc func(string, string) (*corev1.Secret, error)

func ParseNameFromResourceName(name string, clusterExistsInPath bool) (resourceName string, clusterName string) {
	// member cluster or karmada
	parts := strings.Split(name, ".")
	if len(parts) < 3 || parts[len(parts)-2] != "clusterspace" {
		if clusterExistsInPath {
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

func (f *FleetClientset) location(clusterName string) (*url.URL, http.RoundTripper, error) {
	if clusterName == KarmadaCluster {
		return f.karmadaLocation, f.karmadaTransport, nil
	}
	cluster, err := f.clusterGetter(clusterName)
	if err != nil {
		return nil, nil, InterpretGetClusterError(err, clusterName)
	}
	tlsConfig, err := GetTlsConfigForCluster(cluster, f.secretGetter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get tls config for cluster %s: %v", clusterName, err)
	}
	location, transport, err := Location(cluster, tlsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get transport of cluster %s: %v", clusterName, err)
	}
	secret, err := f.secretGetter(cluster.Spec.SecretRef.Namespace, cluster.Spec.SecretRef.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get secret of cluster %s: %v", clusterName, err)
	}
	token, exists := secret.Data["token"]
	if !exists {
		return nil, nil, fmt.Errorf("token not found")
	}
	return normalizeLocation(location), transport2.NewBearerAuthRoundTripper(string(token), transport), nil
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

func (f *FleetClientset) validateMinimumResourceVersion(minimumResourceVersion, actualResourceVersion string) error {
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

// Location returns a URL to which one can send traffic for the specified cluster.
func Location(cluster *clusterv1alpha1.Cluster, tlsConfig *tls.Config) (*url.URL, http.RoundTripper, error) {
	location, err := constructLocation(cluster)
	if err != nil {
		return nil, nil, err
	}

	proxyTransport, err := createProxyTransport(cluster, tlsConfig)
	if err != nil {
		return nil, nil, err
	}

	return location, proxyTransport, nil
}

func constructLocation(cluster *clusterv1alpha1.Cluster) (*url.URL, error) {
	apiEndpoint := cluster.Spec.APIEndpoint
	if apiEndpoint == "" {
		return nil, fmt.Errorf("API endpoint of cluster %s should not be empty", cluster.GetName())
	}

	uri, err := url.Parse(apiEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse api endpoint %s: %v", apiEndpoint, err)
	}
	return uri, nil
}

func createProxyTransport(cluster *clusterv1alpha1.Cluster, tlsConfig *tls.Config) (*http.Transport, error) {
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
	return trans, nil
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

func GetTlsConfigForCluster(cluster *clusterv1alpha1.Cluster, secretGetter SecretGetterFunc) (*tls.Config, error) {
	// The secret is optional for a pull-mode cluster, if no secret just returns a config with root CA unset.
	if cluster.Spec.SecretRef == nil {
		return &tls.Config{
			MinVersion: tls.VersionTLS13,
			// Ignore false positive warning: "TLS InsecureSkipVerify may be true. (gosec)"
			// Whether to skip server certificate verification depends on the
			// configuration(.spec.insecureSkipTLSVerification, defaults to false) in a Cluster object.
			InsecureSkipVerify: cluster.Spec.InsecureSkipTLSVerification, //nolint:gosec
		}, nil
	}
	caSecret, err := secretGetter(cluster.Spec.SecretRef.Namespace, cluster.Spec.SecretRef.Name)
	if err != nil {
		return nil, err
	}
	caBundle, err := getClusterCABundle(cluster.Name, caSecret)
	if err != nil {
		return nil, fmt.Errorf("failed to get CA bundle for cluster %s: %v", cluster.Name, err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caBundle))
	return &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS13,
		// Ignore false positive warning: "TLS InsecureSkipVerify may be true. (gosec)"
		// Whether to skip server certificate verification depends on the
		// configuration(.spec.insecureSkipTLSVerification, defaults to false) in a Cluster object.
		InsecureSkipVerify: cluster.Spec.InsecureSkipTLSVerification, //nolint:gosec
	}, nil
}

func getClusterCABundle(clusterName string, secret *corev1.Secret) (string, error) {
	caBundle, found := secret.Data[clusterv1alpha1.SecretCADataKey]
	if !found {
		return "", fmt.Errorf("the CA bundle of cluster %s is empty", clusterName)
	}
	return string(caBundle), nil
}

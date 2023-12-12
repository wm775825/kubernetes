/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rest

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"k8s.io/apimachinery/third_party/forked/golang/netutil"
	"k8s.io/klog/v2"
	gonet "net"
	"net/http"
	"net/http/httputil"
	"net/url"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/proxy"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	transport2 "k8s.io/client-go/transport"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/registry/core/pod"
)

// ProxyREST implements the proxy subresource for a Pod
type ProxyREST struct {
	Store          *genericregistry.Store
	ProxyTransport http.RoundTripper
}

// Implement Connecter
var _ = rest.Connecter(&ProxyREST{})

var proxyMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}

// New returns an empty podProxyOptions object.
func (r *ProxyREST) New() runtime.Object {
	return &api.PodProxyOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *ProxyREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// ConnectMethods returns the list of HTTP methods that can be proxied
func (r *ProxyREST) ConnectMethods() []string {
	return proxyMethods
}

// NewConnectOptions returns versioned resource that represents proxy parameters
func (r *ProxyREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodProxyOptions{}, true, "path"
}

// Connect returns a handler for the pod proxy
func (r *ProxyREST) Connect(ctx context.Context, id string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	proxyOpts, ok := opts.(*api.PodProxyOptions)
	if !ok {
		return nil, fmt.Errorf("Invalid options object: %#v", opts)
	}
	location, transport, err := pod.ResourceLocation(ctx, r.Store, r.ProxyTransport, id)
	if err != nil {
		return nil, err
	}
	location.Path = net.JoinPreservingTrailingSlash(location.Path, proxyOpts.Path)
	// Return a proxy handler that uses the desired transport, wrapped with additional proxy handling (to get URL rewriting, X-Forwarded-* headers, etc)
	return newThrottledUpgradeAwareProxyHandler(location, transport, true, false, responder), nil
}

// Support both GET and POST methods. We must support GET for browsers that want to use WebSockets.
var upgradeableMethods = []string{"GET", "POST"}

// AttachREST implements the attach subresource for a Pod
type AttachREST struct {
	Store       *genericregistry.Store
	KubeletConn client.ConnectionInfoGetter
}

// Implement Connecter
var _ = rest.Connecter(&AttachREST{})

// New creates a new podAttachOptions object.
func (r *AttachREST) New() runtime.Object {
	return &api.PodAttachOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *AttachREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Connect returns a handler for the pod exec proxy
func (r *AttachREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	attachOpts, ok := opts.(*api.PodAttachOptions)
	if !ok {
		return nil, fmt.Errorf("Invalid options object: %#v", opts)
	}
	location, transport, err := pod.AttachLocation(ctx, r.Store, r.KubeletConn, name, attachOpts)
	if err != nil {
		return nil, err
	}
	return newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder), nil
}

// NewConnectOptions returns the versioned object that represents exec parameters
func (r *AttachREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodAttachOptions{}, false, ""
}

// ConnectMethods returns the methods supported by exec
func (r *AttachREST) ConnectMethods() []string {
	return upgradeableMethods
}

// ExecREST implements the exec subresource for a Pod
type ExecREST struct {
	Store       *genericregistry.Store
	KubeletConn client.ConnectionInfoGetter
}

// Implement Connecter
var _ = rest.Connecter(&ExecREST{})

// New creates a new podExecOptions object.
func (r *ExecREST) New() runtime.Object {
	return &api.PodExecOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *ExecREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Connect returns a handler for the pod exec proxy
func (r *ExecREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	execOpts, ok := opts.(*api.PodExecOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}
	location, transport, err := pod.ExecLocation(ctx, r.Store, r.KubeletConn, name, execOpts)
	if err != nil {
		return nil, err
	}
	token, err := transport2.GetTokenFrom(transport)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if len(req.Header.Get("Authorization")) == 0 {
			req.Header.Set("Authorization", fmt.Sprintf("bearer %s", token))
		}
		wrapProxyTransport(transport)
		handler := newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder)
		handler.ServeHTTP(rw, req)
	}), nil
}

// NewConnectOptions returns the versioned object that represents exec parameters
func (r *ExecREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodExecOptions{}, false, ""
}

// ConnectMethods returns the methods supported by exec
func (r *ExecREST) ConnectMethods() []string {
	return upgradeableMethods
}

// PortForwardREST implements the portforward subresource for a Pod
type PortForwardREST struct {
	Store       *genericregistry.Store
	KubeletConn client.ConnectionInfoGetter
}

// Implement Connecter
var _ = rest.Connecter(&PortForwardREST{})

// New returns an empty podPortForwardOptions object
func (r *PortForwardREST) New() runtime.Object {
	return &api.PodPortForwardOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *PortForwardREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// NewConnectOptions returns the versioned object that represents the
// portforward parameters
func (r *PortForwardREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodPortForwardOptions{}, false, ""
}

// ConnectMethods returns the methods supported by portforward
func (r *PortForwardREST) ConnectMethods() []string {
	return upgradeableMethods
}

// Connect returns a handler for the pod portforward proxy
func (r *PortForwardREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	portForwardOpts, ok := opts.(*api.PodPortForwardOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}
	location, transport, err := pod.PortForwardLocation(ctx, r.Store, r.KubeletConn, name, portForwardOpts)
	if err != nil {
		return nil, err
	}
	return newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder), nil
}

func newThrottledUpgradeAwareProxyHandler(location *url.URL, transport http.RoundTripper, wrapTransport, upgradeRequired bool, responder rest.Responder) *proxy.UpgradeAwareHandler {
	handler := proxy.NewUpgradeAwareHandler(location, transport, wrapTransport, upgradeRequired, proxy.NewErrorResponder(responder))
	handler.MaxBytesPerSec = capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
	return handler
}

func wrapProxyTransport(transport http.RoundTripper) {
	switch trans := transport.(type) {
	case *http.Transport:
		goto LLL
	case net.RoundTripperWrapper:
		wrapProxyTransport(trans.WrappedRoundTripper())
		return
	default:
		return
	}

LLL:
	trans := transport.(*http.Transport)
	if trans.Proxy == nil {
		return
	}
	trans.DialContext = func(ctx context.Context, network, addr string) (gonet.Conn, error) {
		if network != "tcp" {
			return nil, fmt.Errorf("only support tcp")
		}
		u, _ := trans.Proxy(&http.Request{})
		proxyReq := http.Request{
			Method: http.MethodConnect,
			URL:    &url.URL{},
			Host:   addr,
			Header: trans.ProxyConnectHeader,
		}
		proxyReq = *proxyReq.WithContext(ctx)
		if pa := proxyAuth(u); pa != "" {
			proxyReq.Header.Set("Proxy-Authorization", pa)
		}
		dialer := &gonet.Dialer{}
		var proxyConn gonet.Conn
		var err error
		if u.Scheme == "http" {
			proxyConn, err = dialer.DialContext(ctx, "tcp", netutil.CanonicalAddr(u))
		} else {
			tlsDialer := tls.Dialer{NetDialer: dialer, Config: trans.TLSClientConfig}
			proxyConn, err = tlsDialer.DialContext(ctx, "tcp", netutil.CanonicalAddr(u))
		}
		if err != nil {
			klog.Errorf("wm775825: dial err is %#+v", err)
			return nil, err
		}
		proxyClientConn := httputil.NewProxyClientConn(proxyConn, nil)
		resp, err := proxyClientConn.Do(&proxyReq)
		if err != nil && err != httputil.ErrPersistEOF {
			klog.Errorf("wm775825: do err is %#+v", err)
			return nil, err
		}
		if resp != nil && resp.StatusCode >= 300 || resp.StatusCode < 200 {
			return nil, fmt.Errorf("CONNECT request to %s returned response: %s", u.Redacted(), resp.Status)
		}
		rwc, _ := proxyClientConn.Hijack()
		return rwc, nil
	}
}

func proxyAuth(proxyURL *url.URL) string {
	if proxyURL == nil || proxyURL.User == nil {
		return ""
	}
	username := proxyURL.User.Username()
	password, _ := proxyURL.User.Password()
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

package registry

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
)

const (
	KarmadaCluster = "karmada"

	ClusterSpaceSeparator = "clusterspace"
)

func ParseNameFromResourceName(name string, defaultEmpty bool) (resourceName string, clusterName string) {
	parts := strings.Split(name, ".")
	if len(parts) < 3 || parts[len(parts)-2] != ClusterSpaceSeparator {
		if defaultEmpty {
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

type singleDecoratedWatcher struct {
	watcher   watch.Interface
	decorator func(*watch.Event)
}

type watchMux struct {
	lock    *sync.Mutex
	sources []singleDecoratedWatcher
	result  chan watch.Event
	done    chan struct{}
}

func newWatchMux() *watchMux {
	return &watchMux{
		lock:   &sync.Mutex{},
		result: make(chan watch.Event),
		done:   make(chan struct{}),
	}
}

var _ watch.Interface = &watchMux{}

// AddSource shall be called before Start
func (w *watchMux) AddSource(watcher watch.Interface, decorator func(*watch.Event)) {
	w.sources = append(w.sources, singleDecoratedWatcher{
		watcher:   watcher,
		decorator: decorator,
	})
}

// Start run all single watchers
func (w *watchMux) Start() {
	wg := &sync.WaitGroup{}
	for i := range w.sources {
		source := w.sources[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			w.startSingle(source.watcher, source.decorator)
		}()
	}

	go func() {
		// close result chan after all goroutines exit, avoiding data race
		defer close(w.result)
		wg.Wait()
	}()
}

func (w *watchMux) startSingle(source watch.Interface, decorator func(*watch.Event)) {
	// if receiving the done signal from watchMux, stop this single watcher
	defer source.Stop()
	// if stream from the single watcher stopped, stop the watchMux to broadcast to all single watchers
	defer w.Stop()

	for {
		var copyEvent watch.Event
		select {
		case event, ok := <-source.ResultChan():
			if !ok {
				return
			}
			copyEvent = *event.DeepCopy()
			if decorator != nil {
				decorator(&copyEvent)
			}
		case <-w.done:
			return
		}

		// check whether watchMux is done before resend the event
		// if done, stop resend events and return
		select {
		case <-w.done:
			return
		case w.result <- copyEvent:
		}
	}
}

func (w *watchMux) ResultChan() <-chan watch.Event {
	return w.result
}

// Stop close the done channel to broadcast to all single watchers
func (w *watchMux) Stop() {
	// if done has been closed, return
	select {
	case <-w.done:
		return
	default:
	}

	// lock to avoid close channel more than once
	w.lock.Lock()
	defer w.lock.Unlock()
	select {
	case <-w.done:
		return
	default:
		close(w.done)
	}
}

func setNameAndResourceVersion(accessor metav1.Object, clusterName string) {
	setName(accessor, clusterName)
	setResourceVersion(accessor, clusterName)
}

func setName(accessor metav1.Object, clusterName string) {
	if clusterName != KarmadaCluster {
		accessor.SetName(accessor.GetName() + ".clusterspace." + clusterName)
	}
}

func setResourceVersion(accessor metav1.Object, clusterName string) {
	mrv := newMultiClusterResourceVersionWithCapacity(1)
	mrv.set(clusterName, accessor.GetResourceVersion())
	accessor.SetResourceVersion(mrv.String())
}

// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/events"
)

const (
	// ProgramName is the name of the program.
	ProgramName           = "minkapi"
	DefaultWatchQueueSize = 100
	DefaultWatchTimeout   = 5 * time.Minute
	// DefaultKubeConfigPath is the default kubeconfig path if none is specified.
	DefaultKubeConfigPath = "/tmp/minkapi.yaml"
	// DefaultBasePrefix is the default path prefix for the base minkapi server
	DefaultBasePrefix = "base"
)

// WatchConfig holds config parameters relevant for watchers.
type WatchConfig struct {
	// QueueSize is the maximum number of events to queue per watcher
	QueueSize int
	// Timeout represents the timeout for watches following which MinKAPI service will close the connection and ends the watch.
	Timeout time.Duration
}

// MinKAPIConfig holds the configuration for MinKAPI.
type MinKAPIConfig struct {
	// BasePrefix is the path prefix at which the base View of the minkapi service is served. ie KAPI-Service at http://<MinKAPIHost>:<MinKAPIPort>/BasePrefix
	// Defaults to [DefaultBasePrefix]
	BasePrefix string
	commontypes.ServerConfig
	WatchConfig WatchConfig
}

type WatchEventCallback func(watch.Event) (err error)

type ResourceStore interface {
	Add(mo metav1.Object) error
	Update(mo metav1.Object) error
	Delete(key string) error
	GetByKey(key string) (o runtime.Object, err error)

	DeleteObjects(c MatchCriteria) error
	List(c MatchCriteria) (listObj runtime.Object, err error)

	ListMetaObjects(c MatchCriteria) ([]metav1.Object, error)

	Watch(ctx context.Context, startVersion int64, namespace string, labelSelector labels.Selector, eventCallback WatchEventCallback) error
	Shutdown()
}

type ResourceStoreArgs struct {
	Name          string
	ObjectGVK     schema.GroupVersionKind
	ObjectListGVK schema.GroupVersionKind
	// Scheme is the runtime Scheme used by the KAPI objects storable in this store.
	Scheme      *runtime.Scheme
	WatchConfig WatchConfig
	Log         logr.Logger
}

type EventSink interface {
	events.EventSink
	List() []*eventsv1.Event
	Reset()
}

// View is the high-level facade to a repository of objects of different types (GVK).
// TODO: Think of a better name. Rename this to ObjectRepository or something else, also add godoc ?
type View interface {
	GetName() string
	GetType() ViewType
	GetClientFacades(clientType commontypes.ClientMode) (commontypes.ClientFacades, error)
	GetResourceStore(gvk schema.GroupVersionKind) (ResourceStore, error)
	GetEventSink() EventSink
	StoreObject(gvk schema.GroupVersionKind, obj metav1.Object) error
	GetObject(gvk schema.GroupVersionKind, objName cache.ObjectName) (runtime.Object, error)
	UpdateObject(gvk schema.GroupVersionKind, obj metav1.Object) error
	UpdatePodNodeBinding(podName cache.ObjectName, binding corev1.Binding) (*corev1.Pod, error)
	PatchObject(gvk schema.GroupVersionKind, objName cache.ObjectName, patchType types.PatchType, patchData []byte) (patchedObj runtime.Object, err error)
	PatchObjectStatus(gvk schema.GroupVersionKind, objName cache.ObjectName, patchData []byte) (patchedObj runtime.Object, err error)
	ListObjects(gvk schema.GroupVersionKind, criteria MatchCriteria) (runtime.Object, error)
	WatchObjects(ctx context.Context, gvk schema.GroupVersionKind, startVersion int64, namespace string, labelSelector labels.Selector, eventCallback WatchEventCallback) error
	DeleteObject(gvk schema.GroupVersionKind, objName cache.ObjectName) error
	DeleteObjects(gvk schema.GroupVersionKind, criteria MatchCriteria) error
	ListNodes(matchingNodeNames ...string) ([]*corev1.Node, error)
	ListPods(namespace string, matchingPodNames ...string) ([]*corev1.Pod, error)
	ListEvents(namespace string) ([]*eventsv1.Event, error)
	GetKubeConfigPath() string
	Close()
}

type ViewType string

const (
	BaseViewType    ViewType = "base"
	SandboxViewType ViewType = "sandbox"
)

// CreateSandboxViewFunc represents a creator function for constructing sandbox views from the delegate view and given args
type CreateSandboxViewFunc = func(log logr.Logger, delegateView View, args *ViewArgs) (View, error)

type ViewArgs struct {
	// Name represents name of View
	Name string
	// KubeConfigPath is the path of the kubeconfig file corresponding to this view
	KubeConfigPath string
	// Scheme is the runtime Scheme used by KAPI objects exposed by this view
	Scheme      *runtime.Scheme
	WatchConfig WatchConfig
}

// Server represents a MinKAPI server that provides access to a KAPI (kubernetes API) service accessible at http://<MinKAPIHost>:<MinKAPIPort>/basePrefix
// It also supports methods to create "sandbox" (private) views accessible at http://<MinKAPIHost>:<MinKAPIPort>/sandboxName
type Server interface {
	commontypes.Service
	// GetBaseView returns the foundational View of the KAPI Server which is exposed at http://<MinKAPIHost>:<MinKAPIPort>/basePrefix
	GetBaseView() View
	// GetSandboxView creates or returns a sandboxed KAPI View with the given name that is also served as a KAPI Service
	// at http://<MinKAPIHost>:<MinKAPIPort>/sandboxName. A kubeconfig named `minkapi-<name>.yaml` is also generated
	// in the same directory as the base `minkapi.yaml`.  The sandbox name should be a valid path-prefix, ie no-spaces.
	//
	// TODO: discuss whether the above is OK.
	GetSandboxView(ctx context.Context, name string) (View, error)
}

// App represents an application that wraps a minkapi Server, an application context and application cancel func.
//
// `main` entry-point functions taht embed minkapi are expected to construct a new App instance via cli.LaunchApp and shutdown applications via cli.ShutdownApp
type App struct {
	Server Server
	Ctx    context.Context
	Cancel context.CancelFunc
}

type MatchCriteria struct {
	Namespace string
	Names     sets.Set[string]
	// Labels        map[string]string
	LabelSelector labels.Selector
}

func (c MatchCriteria) Matches(obj metav1.Object) bool {
	if c.Namespace != "" && obj.GetNamespace() != c.Namespace {
		return false
	}
	if c.Names.Len() > 0 && !c.Names.Has(obj.GetName()) {
		return false
	}
	if c.LabelSelector != nil && !c.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
		return false
	}
	return true
}

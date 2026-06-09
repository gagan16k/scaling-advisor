// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	"github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/nodeutil"
	"github.com/gardener/scaling-advisor/common/podutil"
	"github.com/gardener/scaling-advisor/common/volutil"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ErrSnapshotNotSynced is returned by Snapshot when the builder has not yet  completed its initial cache sync.
var ErrSnapshotNotSynced = errors.New("cluster snapshot not synced")

// ClusterSnapshotBuilder owns shared informers for the seven object kinds the planner cares about
// (pods / nodes / PVs / PVCs / storage classes / priority classes / runtime classes) and keeps an
// in-memory projection updated on watch events. Snapshot() returns the current projection for
// planner consumption; the builder does not drive reconcile triggers.
type ClusterSnapshotBuilder struct {
	log    logr.Logger
	cache  cache.Cache
	state  *clusterState
	synced atomic.Bool
}

var _ manager.Runnable = (*ClusterSnapshotBuilder)(nil)

// NewClusterSnapshotBuilder returns a builder backed by the given manager cache.
func NewClusterSnapshotBuilder(c cache.Cache, log logr.Logger) *ClusterSnapshotBuilder {
	return &ClusterSnapshotBuilder{
		log:   log,
		cache: c,
		state: newClusterState(),
	}
}

// NeedLeaderElection returns false: every replica must keep its own live snapshot.
func (b *ClusterSnapshotBuilder) NeedLeaderElection() bool {
	return false
}

func (b *ClusterSnapshotBuilder) Snapshot() (planner.ClusterSnapshot, error) {
	if !b.synced.Load() {
		return planner.ClusterSnapshot{}, ErrSnapshotNotSynced
	}
	return b.state.snapshot(), nil
}

// Start registers per-type handlers on each watched informer, waits for cache sync, and blocks
// until ctx is cancelled.
func (b *ClusterSnapshotBuilder) Start(ctx context.Context) error {
	b.log.Info("cluster snapshot builder starting")
	defer b.log.Info("cluster snapshot builder stopped")

	type watch struct {
		obj     client.Object
		handler toolscache.ResourceEventHandler
	}
	watches := []watch{
		{&corev1.Pod{}, b.podHandler()},
		{&corev1.Node{}, b.nodeHandler()},
		{&corev1.PersistentVolume{}, b.pvHandler()},
		{&corev1.PersistentVolumeClaim{}, b.pvcHandler()},
		{&storagev1.StorageClass{}, b.storageClassHandler()},
		{&schedulingv1.PriorityClass{}, b.priorityClassHandler()},
		{&nodev1.RuntimeClass{}, b.runtimeClassHandler()},
	}
	for _, w := range watches {
		if err := b.registerHandler(ctx, w.obj, w.handler); err != nil {
			return err
		}
	}
	if !b.cache.WaitForCacheSync(ctx) {
		return fmt.Errorf("cluster snapshot builder: cache sync did not complete")
	}
	b.synced.Store(true)
	b.log.V(2).Info("cluster snapshot builder ready", "watchedKinds", len(watches))

	<-ctx.Done()
	return nil
}

// registerHandler attaches h to the informer for obj.
func (b *ClusterSnapshotBuilder) registerHandler(ctx context.Context, obj client.Object, h toolscache.ResourceEventHandler) error {
	informer, err := b.cache.GetInformer(ctx, obj)
	if err != nil {
		return fmt.Errorf("cluster snapshot builder: get informer for %T: %w", obj, err)
	}
	if _, err := informer.AddEventHandler(h); err != nil {
		return fmt.Errorf("cluster snapshot builder: add event handler for %T: %w", obj, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Per-type handlers: each type-asserts the incoming object, projects
// it via common/* helpers, and mutates clusterState.
// ---------------------------------------------------------------------------

func (b *ClusterSnapshotBuilder) podHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				b.log.V(2).Info("podHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("pod added", "namespace", pod.Namespace, "name", pod.Name)
			b.state.upsertPod(podutil.AsPodInfo(*pod))
		},
		UpdateFunc: func(_, newObj any) {
			pod, ok := newObj.(*corev1.Pod)
			if !ok {
				b.log.V(2).Info("podHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("pod updated", "namespace", pod.Namespace, "name", pod.Name)
			b.state.upsertPod(podutil.AsPodInfo(*pod))
		},
		DeleteFunc: func(obj any) {
			pod, ok := tombstoneOrObject[*corev1.Pod](obj)
			if !ok {
				b.log.V(2).Info("podHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("pod deleted", "namespace", pod.Namespace, "name", pod.Name)
			b.state.deletePod(commontypes.NamespacedName{Namespace: pod.Namespace, Name: pod.Name})
		},
	}
}

func (b *ClusterSnapshotBuilder) nodeHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				b.log.V(2).Info("nodeHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("node added", "name", node.Name)
			b.state.upsertNode(nodeutil.AsNodeInfo(*node))
		},
		UpdateFunc: func(_, newObj any) {
			node, ok := newObj.(*corev1.Node)
			if !ok {
				b.log.V(2).Info("nodeHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("node updated", "name", node.Name)
			b.state.upsertNode(nodeutil.AsNodeInfo(*node))
		},
		DeleteFunc: func(obj any) {
			node, ok := tombstoneOrObject[*corev1.Node](obj)
			if !ok {
				b.log.V(2).Info("nodeHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("node deleted", "name", node.Name)
			b.state.deleteNode(node.Name)
		},
	}
}

func (b *ClusterSnapshotBuilder) pvHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pv, ok := obj.(*corev1.PersistentVolume)
			if !ok {
				b.log.V(2).Info("pvHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("pv added", "name", pv.Name, "bound", pv.Spec.ClaimRef != nil)
			// Planner only consumes bound PVs.
			if pv.Spec.ClaimRef == nil {
				return
			}
			b.state.upsertPV(volutil.AsPVInfo(*pv))
		},
		UpdateFunc: func(_, newObj any) {
			pv, ok := newObj.(*corev1.PersistentVolume)
			if !ok {
				b.log.V(2).Info("pvHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("pv updated", "name", pv.Name, "bound", pv.Spec.ClaimRef != nil)
			if pv.Spec.ClaimRef == nil {
				// Transitioned out of bound (or never was). Evict if previously stored.
				b.state.deletePV(pv.Name)
				return
			}
			b.state.upsertPV(volutil.AsPVInfo(*pv))
		},
		DeleteFunc: func(obj any) {
			pv, ok := tombstoneOrObject[*corev1.PersistentVolume](obj)
			if !ok {
				b.log.V(2).Info("pvHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("pv deleted", "name", pv.Name)
			b.state.deletePV(pv.Name)
		},
	}
}

func (b *ClusterSnapshotBuilder) pvcHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pvc, ok := obj.(*corev1.PersistentVolumeClaim)
			if !ok {
				b.log.V(2).Info("pvcHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("pvc added", "namespace", pvc.Namespace, "name", pvc.Name)
			b.state.upsertPVC(volutil.AsPVCInfo(*pvc))
		},
		UpdateFunc: func(_, newObj any) {
			pvc, ok := newObj.(*corev1.PersistentVolumeClaim)
			if !ok {
				b.log.V(2).Info("pvcHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("pvc updated", "namespace", pvc.Namespace, "name", pvc.Name)
			b.state.upsertPVC(volutil.AsPVCInfo(*pvc))
		},
		DeleteFunc: func(obj any) {
			pvc, ok := tombstoneOrObject[*corev1.PersistentVolumeClaim](obj)
			if !ok {
				b.log.V(2).Info("pvcHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("pvc deleted", "namespace", pvc.Namespace, "name", pvc.Name)
			b.state.deletePVC(commontypes.NamespacedName{Namespace: pvc.Namespace, Name: pvc.Name})
		},
	}
}

func (b *ClusterSnapshotBuilder) storageClassHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			sc, ok := obj.(*storagev1.StorageClass)
			if !ok {
				b.log.V(2).Info("storageClassHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("storageclass added", "name", sc.Name)
			b.state.upsertStorageClass(*sc.DeepCopy())
		},
		UpdateFunc: func(_, newObj any) {
			sc, ok := newObj.(*storagev1.StorageClass)
			if !ok {
				b.log.V(2).Info("storageClassHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("storageclass updated", "name", sc.Name)
			b.state.upsertStorageClass(*sc.DeepCopy())
		},
		DeleteFunc: func(obj any) {
			sc, ok := tombstoneOrObject[*storagev1.StorageClass](obj)
			if !ok {
				b.log.V(2).Info("storageClassHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("storageclass deleted", "name", sc.Name)
			b.state.deleteStorageClass(sc.Name)
		},
	}
}

func (b *ClusterSnapshotBuilder) priorityClassHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pc, ok := obj.(*schedulingv1.PriorityClass)
			if !ok {
				b.log.V(2).Info("priorityClassHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("priorityclass added", "name", pc.Name)
			b.state.upsertPriorityClass(*pc.DeepCopy())
		},
		UpdateFunc: func(_, newObj any) {
			pc, ok := newObj.(*schedulingv1.PriorityClass)
			if !ok {
				b.log.V(2).Info("priorityClassHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("priorityclass updated", "name", pc.Name)
			b.state.upsertPriorityClass(*pc.DeepCopy())
		},
		DeleteFunc: func(obj any) {
			pc, ok := tombstoneOrObject[*schedulingv1.PriorityClass](obj)
			if !ok {
				b.log.V(2).Info("priorityClassHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("priorityclass deleted", "name", pc.Name)
			b.state.deletePriorityClass(pc.Name)
		},
	}
}

func (b *ClusterSnapshotBuilder) runtimeClassHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			rc, ok := obj.(*nodev1.RuntimeClass)
			if !ok {
				b.log.V(2).Info("runtimeClassHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("runtimeclass added", "name", rc.Name)
			b.state.upsertRuntimeClass(*rc.DeepCopy())
		},
		UpdateFunc: func(_, newObj any) {
			rc, ok := newObj.(*nodev1.RuntimeClass)
			if !ok {
				b.log.V(2).Info("runtimeClassHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("runtimeclass updated", "name", rc.Name)
			b.state.upsertRuntimeClass(*rc.DeepCopy())
		},
		DeleteFunc: func(obj any) {
			rc, ok := tombstoneOrObject[*nodev1.RuntimeClass](obj)
			if !ok {
				b.log.V(2).Info("runtimeClassHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("runtimeclass deleted", "name", rc.Name)
			b.state.deleteRuntimeClass(rc.Name)
		},
	}
}

// tombstoneOrObject extracts the typed object from a Delete event, transparently handling the
// DeletedFinalStateUnknown tombstone delivered after a missed watch resync.
func tombstoneOrObject[T client.Object](obj any) (T, bool) {
	if t, ok := obj.(T); ok {
		return t, true
	}
	if d, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		if t, ok := d.Obj.(T); ok {
			return t, true
		}
	}
	var zero T
	return zero, false
}

// ---------------------------------------------------------------------------
// clusterState: mutex-protected, in-memory projection of the cluster.
// Mutators must REPLACE map values, never mutate them in place.
// ---------------------------------------------------------------------------

type clusterState struct {
	mu sync.RWMutex

	pods            map[commontypes.NamespacedName]planner.PodInfo
	nodes           map[string]planner.NodeInfo
	pvs             map[string]planner.PVInfo // bound PVs only
	pvcs            map[commontypes.NamespacedName]planner.PVCInfo
	storageClasses  map[string]storagev1.StorageClass
	priorityClasses map[string]schedulingv1.PriorityClass
	runtimeClasses  map[string]nodev1.RuntimeClass
}

func newClusterState() *clusterState {
	return &clusterState{
		pods:            map[commontypes.NamespacedName]planner.PodInfo{},
		nodes:           map[string]planner.NodeInfo{},
		pvs:             map[string]planner.PVInfo{},
		pvcs:            map[commontypes.NamespacedName]planner.PVCInfo{},
		storageClasses:  map[string]storagev1.StorageClass{},
		priorityClasses: map[string]schedulingv1.PriorityClass{},
		runtimeClasses:  map[string]nodev1.RuntimeClass{},
	}
}

func (s *clusterState) upsertPod(info planner.PodInfo) {
	key := commontypes.NamespacedName{Namespace: info.Namespace, Name: info.Name}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pods[key] = info
}

func (s *clusterState) deletePod(key commontypes.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pods, key)
}

func (s *clusterState) upsertNode(info planner.NodeInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[info.Name] = info
}

func (s *clusterState) deleteNode(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, name)
}

func (s *clusterState) upsertPV(info planner.PVInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pvs[info.Name] = info
}

func (s *clusterState) deletePV(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pvs, name)
}

func (s *clusterState) upsertPVC(info planner.PVCInfo) {
	key := commontypes.NamespacedName{Namespace: info.Namespace, Name: info.Name}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pvcs[key] = info
}

func (s *clusterState) deletePVC(key commontypes.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pvcs, key)
}

func (s *clusterState) upsertStorageClass(sc storagev1.StorageClass) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storageClasses[sc.Name] = sc
}

func (s *clusterState) deleteStorageClass(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.storageClasses, name)
}

func (s *clusterState) upsertPriorityClass(pc schedulingv1.PriorityClass) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.priorityClasses[pc.Name] = pc
}

func (s *clusterState) deletePriorityClass(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.priorityClasses, name)
}

func (s *clusterState) upsertRuntimeClass(rc nodev1.RuntimeClass) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimeClasses[rc.Name] = rc
}

func (s *clusterState) deleteRuntimeClass(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runtimeClasses, name)
}

// snapshot materialises the current state. Returned slices are fresh; value-typed elements share
// pointer fields with the client-go cache (see Build).
func (s *clusterState) snapshot() planner.ClusterSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return planner.ClusterSnapshot{
		Pods:            slices.Collect(maps.Values(s.pods)),
		Nodes:           slices.Collect(maps.Values(s.nodes)),
		PVs:             slices.Collect(maps.Values(s.pvs)),
		PVCs:            slices.Collect(maps.Values(s.pvcs)),
		StorageClasses:  slices.Collect(maps.Values(s.storageClasses)),
		PriorityClasses: slices.Collect(maps.Values(s.priorityClasses)),
		RuntimeClasses:  slices.Collect(maps.Values(s.runtimeClasses)),
	}
}

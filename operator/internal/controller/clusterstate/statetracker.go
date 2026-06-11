// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package clusterstate

import (
	"context"
	"errors"
	"fmt"
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

// ErrNotSynced is returned by Snapshot when the builder has not yet completed its initial cache sync.
var ErrNotSynced = errors.New("cluster snapshot not synced")

// ClusterStateTracker owns shared informers for the seven object kinds the planner cares about
// (pods / nodes / PVs / PVCs / storage classes / priority classes / runtime classes) and keeps an
// in-memory projection updated on watch events. Snapshot() returns the current projection for
// planner consumption; the builder does not drive reconcile triggers.
type ClusterStateTracker struct {
	log    logr.Logger
	cache  cache.Cache
	state  *clusterState
	synced atomic.Bool
}

var _ manager.Runnable = (*ClusterStateTracker)(nil)

// NewClusterStateTracker returns a ClusterStateTracker backed by the given manager cache.
func NewClusterStateTracker(c cache.Cache, log logr.Logger) *ClusterStateTracker {
	return &ClusterStateTracker{
		log:   log,
		cache: c,
		state: newClusterState(),
	}
}

// NeedLeaderElection returns false: every replica must keep its own live snapshot.
func (b *ClusterStateTracker) NeedLeaderElection() bool {
	return false
}

func (b *ClusterStateTracker) Snapshot() (planner.ClusterSnapshot, error) {
	if !b.synced.Load() {
		return planner.ClusterSnapshot{}, ErrNotSynced
	}
	return b.state.snapshot(), nil
}

// Start registers per-type handlers on each watched informer, waits for cache sync, and blocks
// until ctx is cancelled.
func (b *ClusterStateTracker) Start(ctx context.Context) error {
	b.log.Info("cluster state tracker starting")
	defer b.log.Info("cluster state tracker stopped")

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
		return fmt.Errorf("cluster state tracker: cache sync did not complete")
	}
	b.synced.Store(true)
	b.log.V(2).Info("cluster state tracker ready", "watchedKinds", len(watches))

	<-ctx.Done()
	return nil
}

// registerHandler attaches h to the informer for obj.
func (b *ClusterStateTracker) registerHandler(ctx context.Context, obj client.Object, h toolscache.ResourceEventHandler) error {
	informer, err := b.cache.GetInformer(ctx, obj)
	if err != nil {
		return fmt.Errorf("cluster state tracker: get informer for %T: %w", obj, err)
	}
	if _, err := informer.AddEventHandler(h); err != nil {
		return fmt.Errorf("cluster state tracker: add event handler for %T: %w", obj, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Per-type handlers: each type-asserts the incoming object, projects
// it via common/* helpers, and mutates clusterState.
// ---------------------------------------------------------------------------

func (b *ClusterStateTracker) podHandler() toolscache.ResourceEventHandler {
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

func (b *ClusterStateTracker) nodeHandler() toolscache.ResourceEventHandler {
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

func (b *ClusterStateTracker) pvHandler() toolscache.ResourceEventHandler {
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

func (b *ClusterStateTracker) pvcHandler() toolscache.ResourceEventHandler {
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

func (b *ClusterStateTracker) storageClassHandler() toolscache.ResourceEventHandler {
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

func (b *ClusterStateTracker) priorityClassHandler() toolscache.ResourceEventHandler {
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

func (b *ClusterStateTracker) runtimeClassHandler() toolscache.ResourceEventHandler {
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

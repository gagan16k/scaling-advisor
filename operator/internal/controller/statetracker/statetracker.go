// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statetracker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/nodeutil"
	"github.com/gardener/scaling-advisor/common/podutil"
	"github.com/gardener/scaling-advisor/common/volutil"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ErrNotSynced is returned by Snapshot when the tracker has not yet completed its initial cache sync.
var ErrNotSynced = errors.New("cluster snapshot not synced")

// dataPlaneKinds enumerates the data-plane kinds Snapshot lists. Their informers
// are force-started in Start() so the cache actually populates them — only Node
// has an event handler; the rest are read-only on the Snapshot path.
var dataPlaneKinds = []client.Object{
	&corev1.Pod{},
	&corev1.Node{},
	&corev1.PersistentVolume{},
	&corev1.PersistentVolumeClaim{},
	&storagev1.StorageClass{},
	&schedulingv1.PriorityClass{},
	&nodev1.RuntimeClass{},
}

// ClusterStateTracker serves a planner.ClusterSnapshot from the data-plane
// cache (lister-backed informer store) and tracks advice-derived state from
// the control-plane cache.
type ClusterStateTracker struct {
	log               logr.Logger
	dataPlaneCache    cache.Cache
	controlPlaneCache cache.Cache
	state             *clusterState
	synced            atomic.Bool
}

var _ manager.Runnable = (*ClusterStateTracker)(nil)

// NewClusterStateTracker returns a tracker that watches dataPlaneCache for
// scheduling-relevant kinds and controlPlaneCache for ScalingAdvice.
func NewClusterStateTracker(controlPlaneCache, dataPlaneCache cache.Cache, log logr.Logger) *ClusterStateTracker {
	return &ClusterStateTracker{
		log:               log,
		dataPlaneCache:    dataPlaneCache,
		controlPlaneCache: controlPlaneCache,
		state:             newClusterState(),
	}
}

// NeedLeaderElection returns false: every replica must keep its own live snapshot.
func (b *ClusterStateTracker) NeedLeaderElection() bool {
	return false
}

// Snapshot materialises a ClusterSnapshot by listing the data-plane cache.
// Upcoming nodes are read from in-memory advice-derived state.
func (b *ClusterStateTracker) Snapshot(ctx context.Context, now time.Time) (planner.ClusterSnapshot, error) {
	if !b.synced.Load() {
		return planner.ClusterSnapshot{}, ErrNotSynced
	}
	var (
		pods  corev1.PodList
		nodes corev1.NodeList
		pvs   corev1.PersistentVolumeList
		pvcs  corev1.PersistentVolumeClaimList
		scs   storagev1.StorageClassList
		pcs   schedulingv1.PriorityClassList
		rcs   nodev1.RuntimeClassList
	)
	for _, list := range []client.ObjectList{&pods, &nodes, &pvs, &pvcs, &scs, &pcs, &rcs} {
		if err := b.dataPlaneCache.List(ctx, list); err != nil {
			return planner.ClusterSnapshot{}, fmt.Errorf("cluster state tracker: list %T: %w", list, err)
		}
	}
	snap := planner.ClusterSnapshot{
		Pods:            make([]planner.PodInfo, 0, len(pods.Items)),
		Nodes:           make([]planner.NodeInfo, 0, len(nodes.Items)),
		PVs:             make([]planner.PVInfo, 0, len(pvs.Items)),
		PVCs:            make([]planner.PVCInfo, 0, len(pvcs.Items)),
		StorageClasses:  scs.Items,
		PriorityClasses: pcs.Items,
		RuntimeClasses:  rcs.Items,
	}
	for i := range pods.Items {
		snap.Pods = append(snap.Pods, podutil.AsPodInfo(pods.Items[i]))
	}
	for i := range nodes.Items {
		snap.Nodes = append(snap.Nodes, nodeutil.AsNodeInfo(nodes.Items[i]))
	}
	for i := range pvs.Items {
		// Planner consumes only bound PVs.
		if pvs.Items[i].Spec.ClaimRef == nil {
			continue
		}
		snap.PVs = append(snap.PVs, volutil.AsPVInfo(pvs.Items[i]))
	}
	for i := range pvcs.Items {
		snap.PVCs = append(snap.PVCs, volutil.AsPVCInfo(pvcs.Items[i]))
	}
	snap.UpcomingNodes = b.state.upcomingSnapshot(now)
	return snap, nil
}

// FilterConstraint returns a deep-copy of in with backed-off NodePlacements removed.
func (b *ClusterStateTracker) FilterConstraint(in *corev1alpha1.ScalingConstraint, now time.Time) *corev1alpha1.ScalingConstraint {
	return b.state.filterConstraint(in, now)
}

// IsLatestAdviceAcknowledged reports whether the latest ScalingAdvice observed for the constraint with this key has been acknowledged.
// Returns true if no advice has been observed yet
func (b *ClusterStateTracker) IsLatestAdviceAcknowledged(ref commontypes.NamespacedName) bool {
	return b.state.isLatestAdviceAcknowledged(ref)
}

// Start force-starts the data-plane informers we list from in Snapshot,
// registers the Node and ScalingAdvice handlers, waits for both caches to
// sync, and blocks until ctx is cancelled.
func (b *ClusterStateTracker) Start(ctx context.Context) error {
	b.log.Info("cluster state tracker starting")

	// The data-plane cache only begins watching a kind once GetInformer/List/Get
	// is called for it. Trigger that here for every kind Snapshot lists; without
	// this, the cache stays empty and Snapshot would block on first List.
	for _, obj := range dataPlaneKinds {
		if _, err := b.dataPlaneCache.GetInformer(ctx, obj); err != nil {
			return fmt.Errorf("cluster state tracker: get informer for %T: %w", obj, err)
		}
	}

	type watch struct {
		c       cache.Cache
		obj     client.Object
		handler toolscache.ResourceEventHandler
	}
	watches := []watch{
		{b.dataPlaneCache, &corev1.Node{}, b.nodeHandler()},
		{b.controlPlaneCache, &corev1alpha1.ScalingAdvice{}, b.adviceHandler()},
	}
	for _, w := range watches {
		if err := b.registerHandler(ctx, w.c, w.obj, w.handler); err != nil {
			return err
		}
	}
	if !b.dataPlaneCache.WaitForCacheSync(ctx) {
		return fmt.Errorf("cluster state tracker: data-plane cache sync did not complete")
	}
	if !b.controlPlaneCache.WaitForCacheSync(ctx) {
		return fmt.Errorf("cluster state tracker: control-plane cache sync did not complete")
	}
	b.synced.Store(true)
	b.log.V(2).Info("cluster state tracker ready", "dataPlaneKinds", len(dataPlaneKinds), "handlers", len(watches))

	<-ctx.Done()
	return nil
}

func (b *ClusterStateTracker) registerHandler(ctx context.Context, c cache.Cache, obj client.Object, h toolscache.ResourceEventHandler) error {
	informer, err := c.GetInformer(ctx, obj)
	if err != nil {
		return fmt.Errorf("cluster state tracker: get informer for %T: %w", obj, err)
	}
	if _, err := informer.AddEventHandler(h); err != nil {
		return fmt.Errorf("cluster state tracker: add event handler for %T: %w", obj, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Handlers: only Node (data-plane) and ScalingAdvice (control-plane).
// All other data-plane kinds are read lazily from the cache in Snapshot.
// ---------------------------------------------------------------------------

// nodeHandler consumes an "upcoming" slot when a real node arrives matching a
// synthesised placement. AddFunc is idempotent: removeUpcomingMatching is a
// no-op once the slot is already consumed, so re-list re-delivery is harmless.
func (b *ClusterStateTracker) nodeHandler() toolscache.ResourceEventHandler {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				b.log.V(2).Info("nodeHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("node added", "name", node.Name)
			ni := nodeutil.AsNodeInfo(*node)
			placement, err := ni.GetNodePlacement()
			if err != nil {
				b.log.V(4).Info("node missing required labels; not consuming any upcoming entry", "node", node.Name, "err", err)
				return
			}
			b.state.removeUpcomingMatching(placement)
		},
	}
}

// adviceHandler observes ScalingAdvice on the control-plane cache.
func (b *ClusterStateTracker) adviceHandler() toolscache.ResourceEventHandler {
	apply := func(advice *corev1alpha1.ScalingAdvice) {
		var constraint *corev1alpha1.ScalingConstraint
		if isAdviceAcknowledged(advice) {
			constraint = b.lookupConstraint(advice)
		}
		b.state.applyAdvice(advice, constraint)
	}
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			advice, ok := obj.(*corev1alpha1.ScalingAdvice)
			if !ok {
				b.log.V(2).Info("adviceHandler.Add: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("scalingadvice added", "namespace", advice.Namespace, "name", advice.Name)
			apply(advice)
		},
		UpdateFunc: func(_, newObj any) {
			advice, ok := newObj.(*corev1alpha1.ScalingAdvice)
			if !ok {
				b.log.V(2).Info("adviceHandler.Update: unexpected type", "type", fmt.Sprintf("%T", newObj))
				return
			}
			b.log.V(4).Info("scalingadvice updated", "namespace", advice.Namespace, "name", advice.Name)
			apply(advice)
		},
		// TODO: deletion action needs to be decided
		DeleteFunc: func(obj any) {
			advice, ok := tombstoneOrObject[*corev1alpha1.ScalingAdvice](obj)
			if !ok {
				b.log.V(2).Info("adviceHandler.Delete: unexpected type", "type", fmt.Sprintf("%T", obj))
				return
			}
			b.log.V(4).Info("scalingadvice deleted", "namespace", advice.Namespace, "name", advice.Name)
			b.state.forgetAdvice(advice.Spec.ConstraintRef, advice.UID)
		},
	}
}

// lookupConstraint resolves the ScalingConstraint referenced by advice from the control-plane cache.
// TODO: It is possible to miss updates to advice (which triggered the handler) if there is any failure in reading constraints here
// Returns nil on cache miss; callers downstream treat a nil constraint as "skip upcoming-node synthesis".
func (b *ClusterStateTracker) lookupConstraint(advice *corev1alpha1.ScalingAdvice) *corev1alpha1.ScalingConstraint {
	ref := advice.Spec.ConstraintRef
	constraint := &corev1alpha1.ScalingConstraint{}
	if err := b.controlPlaneCache.Get(context.Background(), types.NamespacedName{
		Namespace: ref.Namespace, Name: ref.Name,
	}, constraint); err != nil {
		b.log.V(2).Info("constraint lookup failed; skipping advice upcoming-node synthesis",
			"advice", advice.Name, "err", err)
		return nil
	}
	return constraint
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

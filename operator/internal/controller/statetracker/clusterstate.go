// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package statetracker

import (
	"sync"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/objutil"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
)

// clusterState is a mutex-protected, in-memory projection of the cluster.
// Mutators must REPLACE map values, never mutate them in place.
type clusterState struct {
	mu sync.RWMutex

	pods            map[commontypes.NamespacedName]planner.PodInfo
	nodes           map[string]planner.NodeInfo
	pvs             map[string]planner.PVInfo // bound PVs only
	pvcs            map[commontypes.NamespacedName]planner.PVCInfo
	storageClasses  map[string]storagev1.StorageClass
	priorityClasses map[string]schedulingv1.PriorityClass
	runtimeClasses  map[string]nodev1.RuntimeClass

	// upcoming holds synthetic NodeInfos derived from acknowledged ScalingAdviceitems.
	// Entries past creationDeadline are pruned lazily by snapshot().
	upcoming map[upcomingKey]upcomingNode
	// backoffs records the latest BackoffUntil per NodePlacement (MAX wins).
	backoffs map[corev1alpha1.NodePlacement]time.Time
	// latestAdvice records the most recently observed advice per ConstraintRef
	// and whether it is acknowledged.
	latestAdvice map[commontypes.NamespacedName]adviceState
}

// adviceState is the per-constraint summary the reconciler reads via
// IsLatestAdviceAcknowledged.
type adviceState struct {
	uid          types.UID
	creationTime time.Time
	acknowledged bool
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
		upcoming:        map[upcomingKey]upcomingNode{},
		backoffs:        map[corev1alpha1.NodePlacement]time.Time{},
		latestAdvice:    map[commontypes.NamespacedName]adviceState{},
	}
}

func (s *clusterState) upsertPod(info planner.PodInfo) {
	key := objutil.NamespacedName(&info)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pods[key] = info
}

func (s *clusterState) deletePod(key commontypes.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pods, key)
}

// upsertNode writes the node and returns true iff the node name was not previously known.
func (s *clusterState) upsertNode(info planner.NodeInfo) (firstSeen bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.nodes[info.Name]
	s.nodes[info.Name] = info
	return !existed
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
	key := objutil.NamespacedName(&info)
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

// setUpcomingNodesForAdvice replaces all entries belonging to adviceUID with items. Does not acquire lock; caller must hold it if needed.
func (s *clusterState) setUpcomingNodesForAdvice(adviceUID types.UID, items []upcomingNode) {
	for u := range s.upcoming {
		if u.adviceUID == adviceUID {
			delete(s.upcoming, u)
		}
	}
	for _, u := range items {
		s.upcoming[u.key] = u
	}
}

// mergeBackoffsForAdvice merges entries (MAX wins). Does not acquire lock; caller must hold it if needed.
func (s *clusterState) mergeBackoffsForAdvice(entries map[corev1alpha1.NodePlacement]time.Time) {
	for p, t := range entries {
		if existing, ok := s.backoffs[p]; !ok || t.After(existing) {
			s.backoffs[p] = t
		}
	}
}

// removeUpcomingMatching deletes the first upcoming entry whose placement equals p.
func (s *clusterState) removeUpcomingMatching(p corev1alpha1.NodePlacement) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.upcoming {
		if v.placement == p {
			delete(s.upcoming, k)
			return
		}
	}
}

// recordAdvice updates the per-constraint latest-advice summary if advice is
// at least as recent as what we have. It returns true iff the advice is acknowledged and this is the first time
// Does not acquire lock; caller must hold it if needed.
func (s *clusterState) recordAdvice(ref commontypes.NamespacedName, st adviceState) (ackedNow bool) {
	existing, ok := s.latestAdvice[ref]
	if ok && existing.creationTime.After(st.creationTime) {
		return false
	}
	// Set when advice is acknowlegded AND
	// when this is the first time we observe the advice OR
	// when the stored entry was for a different UID OR
	// when the previous state was unacknowledged.
	ackedNow = st.acknowledged && (!ok || existing.uid != st.uid || !existing.acknowledged)
	s.latestAdvice[ref] = st
	return ackedNow
}

// applyAdvice records the advice in clusterState and seeds upcoming entries once per advice ack transition.
// TODO: Currently, this blocks acceptance of advice acknowlegdement till all scale-out items are acknowledged.
// We may want to relax this in the future by allowing partial ack and applying backoff on a per-item basis.
// If so, all downstream logic needs change
func (s *clusterState) applyAdvice(advice *corev1alpha1.ScalingAdvice, constraint *corev1alpha1.ScalingConstraint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := advice.Spec.ConstraintRef
	acked := isAdviceAcknowledged(advice)
	ackedNow := s.recordAdvice(ref, adviceState{
		uid:          advice.UID,
		creationTime: advice.CreationTimestamp.Time,
		acknowledged: acked,
	})
	// Return if advice is not acknowledged
	if !acked {
		return
	}
	now := time.Now() //TODO: Can this be skipped, and directly called in the downstream functions?
	if ackedNow {
		s.setUpcomingNodesForAdvice(advice.UID, synthesizeUpcomingNodes(advice, constraint, now))
	}
	if backoffs := extractBackoffEntries(advice, now); len(backoffs) > 0 {
		s.mergeBackoffsForAdvice(backoffs)
	}
}

// forgetAdvice drops the per-constraint summary if it points at uid.
// TODO: Discuss advice deletion
func (s *clusterState) forgetAdvice(ref commontypes.NamespacedName, uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.latestAdvice[ref]; ok && existing.uid == uid {
		delete(s.latestAdvice, ref)
	}
}

// isLatestAdviceAcknowledged reports whether the most recently observed advice for ref is acknowledged.
// Returns true when no advice has been observed (no unacked advice is blocking reconcile).
func (s *clusterState) isLatestAdviceAcknowledged(ref commontypes.NamespacedName) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.latestAdvice[ref]
	if !ok {
		return true
	}
	return st.acknowledged
}

// filterConstraint returns a deep-copy of in with backed-off NodePlacements removed.
func (s *clusterState) filterConstraint(in *corev1alpha1.ScalingConstraint, now time.Time) *corev1alpha1.ScalingConstraint {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p, t := range s.backoffs {
		if !t.After(now) {
			delete(s.backoffs, p)
		}
	}
	return filterConstraintByBackoff(in, s.backoffs, now)
}

// snapshot materialises the current state. It also lazily prunes upcoming
// entries past their deadline and expired backoffs.
func (s *clusterState) snapshot(now time.Time) planner.ClusterSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.upcoming {
		if !v.creationDeadline.After(now) {
			delete(s.upcoming, k)
		}
	}
	snap := planner.ClusterSnapshot{
		Pods:            make([]planner.PodInfo, 0, len(s.pods)),
		Nodes:           make([]planner.NodeInfo, 0, len(s.nodes)),
		UpcomingNodes:   make([]planner.NodeInfo, 0, len(s.upcoming)),
		PVs:             make([]planner.PVInfo, 0, len(s.pvs)),
		PVCs:            make([]planner.PVCInfo, 0, len(s.pvcs)),
		StorageClasses:  make([]storagev1.StorageClass, 0, len(s.storageClasses)),
		PriorityClasses: make([]schedulingv1.PriorityClass, 0, len(s.priorityClasses)),
		RuntimeClasses:  make([]nodev1.RuntimeClass, 0, len(s.runtimeClasses)),
	}
	for _, v := range s.pods {
		snap.Pods = append(snap.Pods, v)
	}
	for _, v := range s.nodes {
		snap.Nodes = append(snap.Nodes, v)
	}
	for _, v := range s.upcoming {
		snap.UpcomingNodes = append(snap.UpcomingNodes, v.nodeInfo)
	}
	for _, v := range s.pvs {
		snap.PVs = append(snap.PVs, v)
	}
	for _, v := range s.pvcs {
		snap.PVCs = append(snap.PVCs, v)
	}
	for _, v := range s.storageClasses {
		snap.StorageClasses = append(snap.StorageClasses, v)
	}
	for _, v := range s.priorityClasses {
		snap.PriorityClasses = append(snap.PriorityClasses, v)
	}
	for _, v := range s.runtimeClasses {
		snap.RuntimeClasses = append(snap.RuntimeClasses, v)
	}
	return snap
}

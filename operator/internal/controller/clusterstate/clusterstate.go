// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package clusterstate

import (
	"sync"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	"github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/objutil"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"
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

// snapshot materialises the current state.
func (s *clusterState) snapshot() planner.ClusterSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := planner.ClusterSnapshot{
		Pods:            make([]planner.PodInfo, 0, len(s.pods)),
		Nodes:           make([]planner.NodeInfo, 0, len(s.nodes)),
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

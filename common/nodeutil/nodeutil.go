// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package nodeutil

import (
	"fmt"
	"maps"
	"time"

	"github.com/gardener/scaling-advisor/common/objutil"

	commonconstants "github.com/gardener/scaling-advisor/api/common/constants"
	sacorev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/minkapi/typeinfo"
	plannerapi "github.com/gardener/scaling-advisor/api/planner"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// GetInstanceType returns the instance-type of the given node from the label present on it.
func GetInstanceType(node *corev1.Node) string {
	return node.Labels[corev1.LabelInstanceTypeStable]
}

// AsNodeInfo converts a corev1.Node into a plannerapi.NodeInfo object.
func AsNodeInfo(node corev1.Node) plannerapi.NodeInfo {
	return plannerapi.NodeInfo{
		ObjectMeta:    node.ObjectMeta,
		InstanceType:  node.Labels[corev1.LabelInstanceTypeStable],
		Unschedulable: node.Spec.Unschedulable,
		Taints:        node.Spec.Taints,
		Capacity:      node.Status.Capacity,
		Allocatable:   node.Status.Allocatable,
		Conditions:    node.Status.Conditions,
	}
}

// AsNode converts a plannerapi.NodeInfo to a corev1.Node object.
func AsNode(info plannerapi.NodeInfo) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: info.ObjectMeta,
		Spec: corev1.NodeSpec{
			Taints:        info.Taints,
			Unschedulable: info.Unschedulable,
		},
		Status: corev1.NodeStatus{
			Capacity:    info.Capacity,
			Allocatable: info.Allocatable,
			Conditions:  info.Conditions,
		},
	}
}

// BuildAllocatable builds the allocatable resources of a node given its capacity, system reserved and kube reserved resources.
func BuildAllocatable(capacity, systemReserved, kubeReserved corev1.ResourceList) corev1.ResourceList {
	allocatable := capacity.DeepCopy()
	objutil.SubtractResources(allocatable, systemReserved)
	objutil.SubtractResources(allocatable, kubeReserved)
	if _, ok := allocatable[corev1.ResourcePods]; !ok {
		allocatable[corev1.ResourcePods] = resource.MustParse("110")
	}
	return allocatable
}

// BuildReadyConditions builds a full set of NodeConditions for a healthy node.
// All pressure conditions are set to False and NodeReady to True
func BuildReadyConditions(transitionTime time.Time) []corev1.NodeCondition {
	ts := metav1.Time{Time: transitionTime}
	make := func(t corev1.NodeConditionType, s corev1.ConditionStatus) corev1.NodeCondition {
		return corev1.NodeCondition{
			Type:               t,
			Status:             s,
			LastHeartbeatTime:  ts,
			LastTransitionTime: ts,
		}
	}
	return []corev1.NodeCondition{
		make(corev1.NodeReady, corev1.ConditionTrue),
		make(corev1.NodeMemoryPressure, corev1.ConditionFalse),
		make(corev1.NodeDiskPressure, corev1.ConditionFalse),
		make(corev1.NodePIDPressure, corev1.ConditionFalse),
	}
}

// AddNodeLabels adds the node labels for the given NodePlacement, architecture, and hostname to nodeLabels.
func AddNodeLabels(nodeLabels map[string]string, arch string, hostName string, placement sacorev1alpha1.NodePlacement) {
	nodeLabels[corev1.LabelInstanceTypeStable] = placement.InstanceType
	nodeLabels[corev1.LabelArchStable] = arch
	nodeLabels[corev1.LabelTopologyZone] = placement.AvailabilityZone
	nodeLabels[corev1.LabelTopologyRegion] = placement.Region
	nodeLabels[corev1.LabelOSStable] = string(corev1.Linux)
	nodeLabels[corev1.LabelHostname] = hostName
	nodeLabels[commonconstants.LabelNodePoolName] = placement.PoolName
	nodeLabels[commonconstants.LabelNodeTemplateName] = placement.TemplateName
}

// NewCSINode returns a fresh CSINode object referring to the node with given name and uid and populated with the given CSISpec
func NewCSINode(nodeName string, nodeUID types.UID, csiNodeSpec storagev1.CSINodeSpec) *storagev1.CSINode {
	return &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: storagev1.SchemeGroupVersion.Version,
					Kind:       typeinfo.CSINodeDescriptor.GetKind(),
					Name:       nodeName,
					UID:        nodeUID,
				},
			},
		},
		Spec: csiNodeSpec,
	}
}

// FindPoolTemplate returns the NodePool and NodeTemplate in constraint whose (name, templateName, instanceType) match placement.
// Returns (nil, nil, false) when constraint is nil or no matching (pool, template) pair is found.
// TODO: To be changed after API changes PR#168
func FindPoolTemplate(constraint *sacorev1alpha1.ScalingConstraint, p sacorev1alpha1.NodePlacement) (*sacorev1alpha1.NodePool, *sacorev1alpha1.NodeTemplate, bool) {
	if constraint == nil {
		return nil, nil, false
	}
	for i := range constraint.Spec.NodePools {
		pool := &constraint.Spec.NodePools[i]
		if pool.Name != p.PoolName {
			continue
		}
		for j := range pool.NodeTemplates {
			t := &pool.NodeTemplates[j]
			if t.Name == p.TemplateName && t.InstanceType == p.InstanceType {
				return pool, t, true
			}
		}
	}
	return nil, nil, false
}

// BuildUpcomingNodeInfo synthesises a NodeInfo. The hostname follows the format
// upcoming-<adviceUID>-<pool>-<template>-<itemIdx>-<replicaIdx>.
func BuildUpcomingNodeInfo(
	adviceUID types.UID,
	itemIdx, replicaIdx int32,
	pool *sacorev1alpha1.NodePool,
	template *sacorev1alpha1.NodeTemplate,
	placement sacorev1alpha1.NodePlacement,
	readyConditions []corev1.NodeCondition,
) plannerapi.NodeInfo {
	hostname := fmt.Sprintf("upcoming-%s-%s-%s-%d-%d",
		adviceUID, placement.PoolName, placement.TemplateName, itemIdx, replicaIdx)

	labels := map[string]string{}
	maps.Copy(labels, pool.Labels)
	AddNodeLabels(labels, template.Architecture, hostname, placement)

	annotations := map[string]string{}
	maps.Copy(annotations, pool.Annotations)

	return plannerapi.NodeInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name:        hostname,
			Labels:      labels,
			Annotations: annotations,
		},
		InstanceType: template.InstanceType,
		Capacity:     template.Capacity.DeepCopy(),
		Allocatable:  BuildAllocatable(template.Capacity, template.SystemReserved, template.KubeReserved),
		Taints:       append([]corev1.Taint(nil), pool.Taints...),
		Conditions:   readyConditions,
	}
}

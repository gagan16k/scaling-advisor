// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package config

import corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"

// Match returns the first FailureRule whose criteria match placement, and true.
// Returns the zero value and false when no rule matches (unmatched items succeed).
// An empty string in MatchCriteria means wildcard.
func Match(rules []FailureRule, p corev1alpha1.NodePlacement) (FailureRule, bool) {
	for _, r := range rules {
		m := r.Match
		if m.PoolName != "" && m.PoolName != p.PoolName {
			continue
		}
		if m.TemplateName != "" && m.TemplateName != p.TemplateName {
			continue
		}
		if m.InstanceType != "" && m.InstanceType != p.InstanceType {
			continue
		}
		if m.Zone != "" && m.Zone != p.AvailabilityZone {
			continue
		}
		return r, true
	}
	return FailureRule{}, false
}

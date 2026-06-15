// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package advisor

import (
	"errors"

	plannerapi "github.com/gardener/scaling-advisor/api/planner"
)

// plannerErrClass picks the reconcile disposition for a planner error.
type plannerErrClass int

const (
	// plannerErrBackoff: return error (exponential backoff).
	plannerErrBackoff plannerErrClass = iota
	// plannerErrRetry: planner ran with nothing to advise; log Info, normal cadence.
	plannerErrRetry
	// plannerErrConfig: bad input/config; log Error, normal cadence (no backoff).
	plannerErrConfig
)

func classifyPlannerErr(err error) plannerErrClass {
	switch {
	case errors.Is(err, plannerapi.ErrNoScaleOutPlan),
		errors.Is(err, plannerapi.ErrNoUnscheduledPods),
		errors.Is(err, plannerapi.ErrNoWinningNodeScore):
		return plannerErrRetry
	case errors.Is(err, plannerapi.ErrInvalidRequest),
		errors.Is(err, plannerapi.ErrInvalidScalingConstraint),
		errors.Is(err, plannerapi.ErrUnsupportedSimulatorStrategy):
		return plannerErrConfig
	default:
		return plannerErrBackoff
	}
}

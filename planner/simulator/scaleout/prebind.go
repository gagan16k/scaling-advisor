// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package scaleout

import (
	"context"
	"fmt"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	"github.com/gardener/scaling-advisor/api/minkapi"
	plannerapi "github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/ioutil"
	"github.com/gardener/scaling-advisor/common/viewutil"
	"github.com/gardener/scaling-advisor/common/volutil"
	"github.com/go-logr/logr"
)

// prebindRequestView runs the embedded kube-scheduler against the request view
// to bind unscheduled pods that fit on existing real and upcoming nodes before
// any candidate scale-out node is introduced. Bindings persist on the view;
// sandbox views created later via GetSandboxViewOverDelegate inherit them.
// No-op when there are no unscheduled pods or no upcoming nodes.
func prebindRequestView(
	ctx context.Context,
	view minkapi.View,
	snapshot *plannerapi.ClusterSnapshot,
	schedulerLauncher plannerapi.SchedulerLauncher,
	simConfig plannerapi.SimulatorConfig,
) error {
	log := logr.FromContextOrDiscard(ctx).WithValues("phase", "prebind", "view", view.GetName())

	unscheduled := len(snapshot.GetUnscheduledPods())
	upcoming := len(snapshot.UpcomingNodes)
	if upcoming == 0 || unscheduled == 0 {
		log.Info("skipping prebind", "upcomingNodes", upcoming, "unscheduledPods", unscheduled)
		return nil
	}

	clientFacades, err := view.GetClientFacades(ctx, commontypes.ClientAccessModeInMemory)
	if err != nil {
		return fmt.Errorf("cannot get client facades for prebind: %w", err)
	}
	handle, err := schedulerLauncher.Launch(ctx, &plannerapi.SchedulerLaunchParams{
		ClientFacades: clientFacades,
		EventSink:     view.GetEventSink(),
	})
	if err != nil {
		return fmt.Errorf("cannot launch scheduler for prebind: %w", err)
	}
	defer ioutil.CloseQuietly(handle)

	log.V(2).Info("running prebind pass", "upcomingNodes", upcoming, "unscheduledPods", unscheduled)

	pollTimer := time.NewTimer(simConfig.TrackPollInterval)
	defer pollTimer.Stop()

	var unchanged int
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Drive any WFFC volume work the scheduler scheduled in the previous
		// iteration; the VolumeBinding plugin must observe a provisioned PV
		// before it will bind the dependent pod.
		if _, err = volutil.ProvisionAndBindVolumesFoSelectedClaimsInWFFC(ctx, view); err != nil {
			return fmt.Errorf("WFFC volume provisioning failed during prebind: %w", err)
		}
		if _, err = volutil.FinalizeStaticBindingsForSelectedClaimsInWFFC(ctx, view); err != nil {
			return fmt.Errorf("WFFC volume finalisation failed during prebind: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pollTimer.C:
		}
		pollTimer.Reset(simConfig.TrackPollInterval)

		// Atomically take and clear the scheduler's events for this tick.
		evList := view.GetEventSink().Drain()
		if len(evList) == 0 {
			unchanged++
			if unchanged > simConfig.MaxUnchangedTrackAttempts {
				log.V(2).Info("prebind stabilised", "unchangedAttempts", unchanged)
				return nil
			}
			continue
		}
		unchanged = 0

		remaining, err := viewutil.ListUnscheduledPods(ctx, view)
		if err != nil {
			return fmt.Errorf("cannot list pods during prebind: %w", err)
		}
		if len(remaining) == 0 {
			log.Info("prebind bound all unscheduled pods")
			return nil
		}
	}
}

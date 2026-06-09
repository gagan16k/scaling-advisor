// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"github.com/gardener/scaling-advisor/operator/internal/controller/advisor"
	"github.com/gardener/scaling-advisor/operator/internal/controller/clusterstate"

	commonconstants "github.com/gardener/scaling-advisor/api/common/constants"
	configv1alpha1 "github.com/gardener/scaling-advisor/api/config/v1alpha1"
	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	ctrlmetricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// CreateManagerAndRegisterControllers creates a controller manager and registers all controllers.
func CreateManagerAndRegisterControllers(log logr.Logger, saCfg *configv1alpha1.OperatorConfig) (ctrl.Manager, error) {
	mgrOpts, err := createManagerOptions(log, saCfg)
	if err != nil {
		return nil, err
	}

	controlPlaneRestCfg, err := getControlPlaneRestConfig(saCfg)
	if err != nil {
		return nil, err
	}
	dataPlaneRestCfg, err := getDataPlaneRestConfig(saCfg)
	if err != nil {
		return nil, err
	}

	mgr, err := ctrl.NewManager(controlPlaneRestCfg, mgrOpts)
	if err != nil {
		return nil, err
	}

	// Create a separate cache for the data plane cluster and add it to the manager, so it gets started and stopped together with the manager.
	dataPlaneCache, err := cache.New(dataPlaneRestCfg, cache.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		return nil, err
	}
	if err = mgr.Add(dataPlaneCache); err != nil {
		return nil, err
	}

	if err = registerControllers(mgr, saCfg, dataPlaneCache); err != nil {
		return nil, err
	}
	return mgr, nil
}

func createManagerOptions(log logr.Logger, saCfg *configv1alpha1.OperatorConfig) (ctrl.Options, error) {
	scheme, err := createScalingAdvisorScheme()
	if err != nil {
		return ctrl.Options{}, err
	}
	opts := ctrl.Options{
		Scheme:                  scheme,
		GracefulShutdownTimeout: ptr.To(commonconstants.DefaultGracefulShutdownTimeout),
		Logger:                  log,
		Metrics: ctrlmetricsserver.Options{
			BindAddress: saCfg.Server.MetricsBindAddress,
		},
		LeaderElection:                saCfg.LeaderElection.Enabled,
		LeaderElectionID:              saCfg.LeaderElection.ResourceName,
		LeaderElectionResourceLock:    saCfg.LeaderElection.ResourceLock,
		LeaderElectionReleaseOnCancel: true,
		LeaseDuration:                 &saCfg.LeaderElection.LeaseDuration.Duration,
		RenewDeadline:                 &saCfg.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:                   &saCfg.LeaderElection.RetryPeriod.Duration,
		Controller: ctrlconfig.Controller{
			RecoverPanic: ptr.To(true),
		},
	}
	if saCfg.Server.ProfilingEnabled {
		opts.PprofBindAddress = saCfg.Server.ProfilingBindAddress
	}
	return opts, nil
}

func getControlPlaneRestConfig(cfg *configv1alpha1.OperatorConfig) (*rest.Config, error) {
	var (
		restCfg *rest.Config
		err     error
	)
	if cfg != nil && cfg.ControlPlaneClientConnection.KubeConfigPath != "" {
		restCfg, err = clientcmd.BuildConfigFromFlags("", cfg.ControlPlaneClientConnection.KubeConfigPath)

	} else {
		restCfg, err = ctrl.GetConfig() // in-cluster fallback

	}
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		applyClientConnectionSettings(restCfg, cfg.ControlPlaneClientConnection)
	}
	return restCfg, nil
}

func getDataPlaneRestConfig(cfg *configv1alpha1.OperatorConfig) (*rest.Config, error) {
	// KubeConfigPath is guaranteed non-empty by validation
	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.DataPlaneClientConnection.KubeConfigPath)
	if err != nil {
		return nil, err
	}
	applyClientConnectionSettings(restCfg, cfg.DataPlaneClientConnection)
	return restCfg, nil
}

func applyClientConnectionSettings(restCfg *rest.Config, conn configv1alpha1.ClientConnectionConfig) {
	restCfg.Burst = conn.Burst
	restCfg.QPS = conn.QPS
	restCfg.AcceptContentTypes = conn.AcceptContentTypes
	restCfg.ContentType = conn.ContentType
}

func createScalingAdvisorScheme() (*runtime.Scheme, error) {
	localSchemeBuilder := runtime.NewSchemeBuilder(
		k8sscheme.AddToScheme,
		configv1alpha1.AddToScheme,
		corev1alpha1.AddToScheme,
	)
	scheme := runtime.NewScheme()
	if err := localSchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return scheme, nil
}

func registerControllers(mgr ctrl.Manager, opCfg *configv1alpha1.OperatorConfig, dataplaneCache cache.Cache) error {
	log := mgr.GetLogger().WithName("advisor")
	csBuilder := clusterstate.NewClusterStateTracker(dataplaneCache, log.WithName("cs-tracker"))
	if err := mgr.Add(csBuilder); err != nil {
		return err
	}
	reconciler, err := advisor.NewReconciler(mgr, opCfg, csBuilder)
	if err != nil {
		return err
	}
	return reconciler.SetupWithManager(mgr)
}

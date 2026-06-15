// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Command fakeactuator is a configurable lifecycle-controller stand-in for
// local development of the Scaling Advisor Operator. It watches ScalingAdvice
// on a control-plane cluster, creates/deletes real Nodes on a data-plane
// cluster according to a YAML config, and writes ScalingFeedback back.
package main

import (
	"fmt"
	"os"

	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	commoncli "github.com/gardener/scaling-advisor/common/cliutil"
	"github.com/gardener/scaling-advisor/test/fakeactuator/internal/config"
	"github.com/gardener/scaling-advisor/test/fakeactuator/internal/reconciler"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const programName = "fakeactuator"

// options holds parsed CLI flags.
type options struct {
	configPath             string
	controlPlaneKubeconfig string
	dataPlaneKubeconfig    string
	version                bool
}

func main() {
	log := klog.NewKlogr()
	ctrl.SetLogger(log)

	opts, err := parseArgs(os.Args[1:])
	if err != nil {
		commoncli.HandleErrorAndExit(err)
	}
	if opts.version {
		commoncli.PrintVersion(programName)
		os.Exit(commoncli.ExitSuccess)
	}

	if err := run(log, opts); err != nil {
		commoncli.HandleErrorAndExit(err)
	}
}

func parseArgs(args []string) (*options, error) {
	opts := &options{}
	fs := pflag.NewFlagSet(programName, pflag.ContinueOnError)

	fs.StringVar(&opts.configPath, "config", opts.configPath,
		"path to fakeactuator YAML config (required)")
	fs.StringVar(&opts.controlPlaneKubeconfig, "control-plane-kubeconfig", opts.controlPlaneKubeconfig,
		"path to control-plane kubeconfig (overrides config file value; falls back to in-cluster / default discovery)")
	fs.StringVar(&opts.dataPlaneKubeconfig, "data-plane-kubeconfig", opts.dataPlaneKubeconfig,
		"path to data-plane kubeconfig (overrides config file value; falls back to control-plane config)")
	fs.BoolVarP(&opts.version, "version", "V", opts.version, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("%w: %w", commoncli.ErrParseArgs, err)
	}
	return opts, nil
}

func run(log klog.Logger, opts *options) error {
	// Load config (or use defaults when no --config provided for legacy compat).
	var cfg *config.Config
	var err error
	if opts.configPath != "" {
		cfg, err = config.Load(opts.configPath)
		if err != nil {
			return err
		}
	} else {
		cfg = config.Default()
	}

	// CLI overrides take precedence over config file values.
	if opts.controlPlaneKubeconfig != "" {
		cfg.ControlPlaneKubeconfig = opts.controlPlaneKubeconfig
	}
	if opts.dataPlaneKubeconfig != "" {
		cfg.DataPlaneKubeconfig = opts.dataPlaneKubeconfig
	}

	scheme, err := createScheme()
	if err != nil {
		return err
	}

	cpRestCfg, err := buildRestConfig(cfg.ControlPlaneKubeconfig)
	if err != nil {
		return fmt.Errorf("control-plane rest config: %w", err)
	}

	dpKubeconfig := cfg.DataPlaneKubeconfig
	if dpKubeconfig == "" {
		dpKubeconfig = cfg.ControlPlaneKubeconfig
	}
	dpRestCfg, err := buildRestConfig(dpKubeconfig)
	if err != nil {
		return fmt.Errorf("data-plane rest config: %w", err)
	}

	dpClient, err := client.New(dpRestCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("data-plane client: %w", err)
	}

	mgr, err := ctrl.NewManager(cpRestCfg, ctrl.Options{
		Scheme:  scheme,
		Logger:  log,
		Metrics: ctrlmetricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return err
	}

	r := reconciler.NewReconciler(mgr, dpClient, cfg)
	if err := r.SetupWithManager(mgr); err != nil {
		return err
	}

	return mgr.Start(ctrl.SetupSignalHandler())
}

// createScheme builds a scheme with k8s core types and the scaling-advisor API.
func createScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := k8sscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return scheme, nil
}

// buildRestConfig returns a rest.Config for the given kubeconfig path.
// An empty path falls back to controller-runtime's discovery chain.
func buildRestConfig(kubeConfigPath string) (*rest.Config, error) {
	if kubeConfigPath == "" {
		return ctrl.GetConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeConfigPath)
}

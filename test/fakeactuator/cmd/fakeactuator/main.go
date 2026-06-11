// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Command fakeactuator is a minimal lifecycle-controller stand-in used to
// exercise the Scaling Advisor Operator's advice→feedback contract during
// local development. It watches ScalingAdvice and writes ack-shaped feedback
// into ScalingAdvice.Status.Feedback.

package main

import (
	"fmt"
	"os"
	"time"

	corev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	commoncli "github.com/gardener/scaling-advisor/common/cliutil"
	"github.com/gardener/scaling-advisor/test/fakeactuator/internal/reconciler"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlmetricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	programName          = "fakeactuator"
	defaultCreationDelay = 30 * time.Second
)

// options holds parsed CLI flags. Adding a new flag = one field here + one
// fs.*Var line in parseArgs + (optionally) one yq line in the Makefile.
type options struct {
	kubeconfig    string
	creationDelay time.Duration
	version       bool
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
	opts := &options{creationDelay: defaultCreationDelay}
	fs := pflag.NewFlagSet(programName, pflag.ContinueOnError)
	fs.StringVar(&opts.kubeconfig, "kubeconfig", opts.kubeconfig,
		"path to the kubeconfig of the cluster where ScalingAdvice is published; empty falls back to in-cluster / default discovery")
	fs.DurationVar(&opts.creationDelay, "creation-delay", opts.creationDelay,
		"delay added to now() when computing CreationDeadline for each ScaleOutItemFeedback")
	fs.BoolVarP(&opts.version, "version", "V", opts.version, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("%w: %w", commoncli.ErrParseArgs, err)
	}
	if opts.creationDelay < 0 {
		return nil, fmt.Errorf("%w: --creation-delay must be non-negative", commoncli.ErrInvalidOpt)
	}
	return opts, nil
}

func run(log klog.Logger, opts *options) error {
	restCfg, err := buildRestConfig(opts.kubeconfig)
	if err != nil {
		return err
	}

	scheme := runtime.NewScheme()
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:  scheme,
		Logger:  log,
		Metrics: ctrlmetricsserver.Options{BindAddress: "0"}, // disabled
	})
	if err != nil {
		return err
	}

	r := reconciler.NewReconciler(mgr, reconciler.Options{CreationDelay: opts.creationDelay})
	if err := r.SetupWithManager(mgr); err != nil {
		return err
	}

	return mgr.Start(ctrl.SetupSignalHandler())
}

// buildRestConfig returns the rest.Config for the given kubeconfig path. An
// empty path falls back to controller-runtime's discovery (in-cluster, then
// $KUBECONFIG, then ~/.kube/config).
func buildRestConfig(kubeConfigPath string) (*rest.Config, error) {
	if kubeConfigPath == "" {
		return ctrl.GetConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeConfigPath)
}

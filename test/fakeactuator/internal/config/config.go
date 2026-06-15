// SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package config defines the YAML-driven configuration for fakeactuator.
package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// FailureMode enumerates supported per-placement failure modes.
type FailureMode string

const (
	ModeResourceExhausted  FailureMode = "resourceExhausted"
	ModeCreationTimeout    FailureMode = "creationTimeout"
	ModePartial            FailureMode = "partial"
	ModeSilentDeadlineMiss FailureMode = "silentDeadlineMiss"
)

// Timing holds all delay knobs.
type Timing struct {
	CreationDelay    time.Duration `yaml:"creationDelay"`
	NodeCreateDelay  time.Duration `yaml:"nodeCreateDelay"`
	NodeReadyDelay   time.Duration `yaml:"nodeReadyDelay"`
	BackoffDuration  time.Duration `yaml:"backoffDuration"`
	DeleteGraceDelay time.Duration `yaml:"deleteGraceDelay"`
}

// MatchCriteria selects a ScaleOutItem. An empty string means wildcard.
type MatchCriteria struct {
	PoolName     string `yaml:"poolName"`
	TemplateName string `yaml:"templateName"`
	InstanceType string `yaml:"instanceType"`
	Zone         string `yaml:"zone"`
}

// FailureRule describes deterministic failure injection for matched items.
// First-match wins; unmatched items succeed.
type FailureRule struct {
	Match MatchCriteria `yaml:"match"`
	// Mode is one of: resourceExhausted | creationTimeout | partial | silentDeadlineMiss.
	Mode FailureMode `yaml:"mode"`
	// BackoffDuration overrides timing.backoffDuration for this rule.
	BackoffDuration time.Duration `yaml:"backoffDuration"`
	// FailCount is the number of replicas to fail (mode=partial only). Default: 1.
	FailCount int32 `yaml:"failCount"`
	// CreationDelay overrides timing.creationDelay for this rule (silentDeadlineMiss only).
	CreationDelay time.Duration `yaml:"creationDelay"`
}

// Config is the top-level fakeactuator configuration.
type Config struct {
	ControlPlaneKubeconfig string        `yaml:"controlPlaneKubeconfig"`
	DataPlaneKubeconfig    string        `yaml:"dataPlaneKubeconfig"`
	Timing                 Timing        `yaml:"timing"`
	FailureRules           []FailureRule `yaml:"failureRules"`
}

// Default returns a Config with sensible defaults and no failure rules.
func Default() *Config {
	return &Config{
		Timing: Timing{
			CreationDelay:   30 * time.Second,
			BackoffDuration: 2 * time.Minute,
		},
	}
}

// Load parses and validates the config at path using strict YAML (unknown keys rejected).
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config: validate %s: %w", path, err)
	}
	return cfg, nil
}

func validate(cfg *Config) error {
	t := cfg.Timing
	for _, c := range []struct {
		name string
		v    time.Duration
	}{
		{"creationDelay", t.CreationDelay},
		{"nodeCreateDelay", t.NodeCreateDelay},
		{"nodeReadyDelay", t.NodeReadyDelay},
		{"backoffDuration", t.BackoffDuration},
		{"deleteGraceDelay", t.DeleteGraceDelay},
	} {
		if c.v < 0 {
			return fmt.Errorf("timing.%s must be non-negative", c.name)
		}
	}
	for i, r := range cfg.FailureRules {
		switch r.Mode {
		case ModeResourceExhausted, ModeCreationTimeout, ModePartial, ModeSilentDeadlineMiss:
		default:
			return fmt.Errorf("failureRules[%d].mode %q is invalid", i, r.Mode)
		}
		if r.BackoffDuration < 0 {
			return fmt.Errorf("failureRules[%d].backoffDuration must be non-negative", i)
		}
		if r.FailCount < 0 {
			return fmt.Errorf("failureRules[%d].failCount must be non-negative", i)
		}
	}
	return nil
}

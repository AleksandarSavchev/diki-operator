// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	goflag "flag"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/gardener/gardener/extensions/pkg/util"
	gardenerhealthz "github.com/gardener/gardener/pkg/healthz"
	"github.com/gardener/gardener/pkg/logger"
	"github.com/go-logr/logr"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	componentbaseconfigv1alpha1 "k8s.io/component-base/config/v1alpha1"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controllerconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	compliancescan "github.com/gardener/diki-operator/internal/reconciler/compliancescan"
	configv1alpha1 "github.com/gardener/diki-operator/pkg/apis/config/v1alpha1"
	dikiinstall "github.com/gardener/diki-operator/pkg/apis/diki/install"
)

// AppName is the name of the application.
const AppName = "diki-operator"

// NewCommand is the root command for the Diki operator.
func NewCommand() *cobra.Command {
	opt := newOptions()

	cmd := &cobra.Command{
		Use:   AppName,
		Short: "Launch the " + AppName,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := opt.Complete(); err != nil {
				return err
			}
			if err := opt.Validate(); err != nil {
				return fmt.Errorf("cannot validate options: %w", err)
			}

			logLevel, logFormat := opt.LogConfig()
			log, err := logger.NewZapLogger(logLevel, logFormat)
			if err != nil {
				return fmt.Errorf("error instantiating zap logger: %w", err)
			}

			logf.SetLogger(log)
			klog.SetLogger(log)

			log.Info("Starting application", "app", AppName, "version", version.Get())
			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				log.Info("Flag", "name", flag.Name, "value", flag.Value, "default", flag.DefValue)
			})

			return run(cmd.Context(), log, opt.config)
		},
		PreRunE: func(_ *cobra.Command, _ []string) error {
			verflag.PrintAndExitIfRequested()
			return nil
		},
	}

	flags := cmd.Flags()
	opt.addFlags(flags)
	flags.AddGoFlagSet(goflag.CommandLine)

	return cmd
}

func run(ctx context.Context, log logr.Logger, cfg *configv1alpha1.DikiOperatorConfiguration) error {
	// Get the target cluster config (scanned cluster)
	conf, err := ctrl.GetConfig()
	if err != nil {
		return err
	}

	util.ApplyClientConnectionConfigurationToRESTConfig(&componentbaseconfigv1alpha1.ClientConnectionConfiguration{
		QPS:   100.0,
		Burst: 130,
	}, conf)

	log.Info("Setting up manager")
	mgr, err := ctrl.NewManager(conf, ctrl.Options{
		Logger: log.WithName("manager"),
		Metrics: metricsserver.Options{
			BindAddress: net.JoinHostPort(cfg.Server.Metrics.BindAddress, strconv.Itoa(int(cfg.Server.Metrics.Port))),
		},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				"kube-system": {},
			},
		},
		GracefulShutdownTimeout: ptr.To(5 * time.Second),

		LeaderElection:                *cfg.LeaderElection.LeaderElect,
		LeaderElectionResourceLock:    cfg.LeaderElection.ResourceLock,
		LeaderElectionID:              cfg.LeaderElection.ResourceName,
		LeaderElectionNamespace:       cfg.LeaderElection.ResourceNamespace,
		LeaderElectionReleaseOnCancel: true,
		LeaseDuration:                 &cfg.LeaderElection.LeaseDuration.Duration,
		RenewDeadline:                 &cfg.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:                   &cfg.LeaderElection.RetryPeriod.Duration,

		PprofBindAddress: "",
		HealthProbeBindAddress: net.JoinHostPort(
			cfg.Server.HealthProbes.BindAddress,
			strconv.Itoa(int(cfg.Server.HealthProbes.Port))),

		Controller: controllerconfig.Controller{
			RecoverPanic: ptr.To(true),
		},
	})
	if err != nil {
		return fmt.Errorf("unable to create manager: %w", err)
	}

	if err := dikiinstall.AddToScheme(mgr.GetScheme()); err != nil {
		return fmt.Errorf("could not update manager scheme: %w", err)
	}
	if err := clientgoscheme.AddToScheme(mgr.GetScheme()); err != nil {
		return fmt.Errorf("could not update manager scheme: %w", err)
	}

	log.Info("Setting up health check endpoints")
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("informer-sync", gardenerhealthz.NewCacheSyncHealthzWithDeadline(mgr.GetLogger(), clock.RealClock{}, mgr.GetCache(), gardenerhealthz.DefaultCacheSyncDeadline)); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("informer-sync", gardenerhealthz.NewCacheSyncHealthz(mgr.GetCache())); err != nil {
		return err
	}

	// Create runner client for the cluster where diki-runner pods will be deployed.
	// Use in-cluster config if available, otherwise use the manager kubeconfig.
	var runnerClient client.Client
	var runnerRESTConfig *rest.Config

	inClusterConf, err := rest.InClusterConfig()
	if err != nil {
		log.Info("Not running in-cluster, using manager kubeconfig for runner client")
		runnerClient = mgr.GetClient()
		runnerRESTConfig = mgr.GetConfig()
	} else {
		log.Info("Running in-cluster, using in-cluster config for runner client")
		util.ApplyClientConnectionConfigurationToRESTConfig(&componentbaseconfigv1alpha1.ClientConnectionConfiguration{
			QPS:   100.0,
			Burst: 130,
		}, inClusterConf)

		runnerClient, err = client.New(inClusterConf, client.Options{
			Scheme: mgr.GetScheme(),
		})
		if err != nil {
			return fmt.Errorf("unable to create runner client: %w", err)
		}
		runnerRESTConfig = inClusterConf
	}

	// Determine the target REST config for the cluster to be scanned.
	// If DikiRunner.Kubeconfig is configured, load the target config from that file.
	// Otherwise, use the manager's kubeconfig as the target.
	var targetRESTConfig *rest.Config
	if cfg.Controllers.ComplianceScan.DikiRunner.Kubeconfig != nil && *cfg.Controllers.ComplianceScan.DikiRunner.Kubeconfig != "" {
		log.Info("Loading target cluster kubeconfig for diki-runner", "path", *cfg.Controllers.ComplianceScan.DikiRunner.Kubeconfig)
		targetRESTConfig, err = clientcmd.BuildConfigFromFlags("", *cfg.Controllers.ComplianceScan.DikiRunner.Kubeconfig)
		if err != nil {
			return fmt.Errorf("failed to load diki-runner target kubeconfig: %w", err)
		}
		util.ApplyClientConnectionConfigurationToRESTConfig(&componentbaseconfigv1alpha1.ClientConnectionConfiguration{
			QPS:   100.0,
			Burst: 130,
		}, targetRESTConfig)
	} else {
		log.Info("DikiRunner kubeconfig not configured, using manager kubeconfig as target")
		targetRESTConfig = mgr.GetConfig()
	}

	// Setup ComplianceScan controller
	if err := (&compliancescan.Reconciler{
		TargetClient:     mgr.GetClient(),  // Manager cluster (where ComplianceScan CRDs live)
		TargetRESTConfig: targetRESTConfig, // Target cluster config (cluster to be scanned by diki-runner)
		Client:           runnerClient,     // Runner cluster (where diki-runner pods are deployed)
		RESTConfig:       runnerRESTConfig, // Runner cluster config
		Config:           cfg.Controllers.ComplianceScan,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("unable to create complianceScan reconcile controller: %w", err)
	}

	log.Info("Starting manager")
	return mgr.Start(ctx)
}

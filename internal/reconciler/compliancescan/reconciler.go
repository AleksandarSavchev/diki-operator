// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gardener/diki-operator/imagevector"
	configv1alpha1 "github.com/gardener/diki-operator/pkg/apis/config/v1alpha1"
	"github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1"
	dikiv1alpha1helper "github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1/helper"
)

// Reconciler reconciles compliance scans.
type Reconciler struct {
	TargetClient     client.Client
	TargetRESTConfig *rest.Config
	Client           client.Client
	RESTConfig       *rest.Config
	Config           configv1alpha1.ComplianceScanConfig
}

// Reconcile handles reconciliation requests for ComplianceScan resources.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("name", req.Name)

	complianceScan := &v1alpha1.ComplianceScan{}

	if err := r.TargetClient.Get(ctx, client.ObjectKey{Name: req.Name}, complianceScan); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Object is gone, stop reconciling")
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("error retrieving complianceScan: %w", err)
	}

	if len(complianceScan.Status.Phase) > 0 {
		log.Info("ComplianceScan already processed, stop reconciling", "phase", complianceScan.Status.Phase)
		return reconcile.Result{}, nil
	}

	// Update phase to Running
	patch := client.MergeFrom(complianceScan.DeepCopy())
	complianceScan.Status.Conditions = dikiv1alpha1helper.UpdateConditions(
		complianceScan.Status.Conditions,
		v1alpha1.ConditionTypeCompleted,
		v1alpha1.ConditionFalse,
		ConditionReasonRunning,
		"ComplianceScan is running",
		time.Now(),
	)
	complianceScan.Status.Phase = v1alpha1.ComplianceScanRunning
	if err := r.TargetClient.Status().Patch(ctx, complianceScan, patch); err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}

	log.Info("Updated ComplianceScan phase to Running")

	reportOutputs := []v1alpha1.ReportOutput{}
	// WE get the reportOutput
	for _, output := range complianceScan.Spec.Outputs {
		reportOutputObj := &v1alpha1.ReportOutput{
			ObjectMeta: v1.ObjectMeta{
				Name: output.Name,
			},
		}
		if err := r.TargetClient.Get(ctx, client.ObjectKeyFromObject(reportOutputObj), reportOutputObj); err != nil {
			return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
		}
		reportOutputs = append(reportOutputs, *reportOutputObj)
	}

	// Create kubeconfig secret if target cluster is different from operator cluster
	var kubeconfigSecretName string
	if r.needsKubeconfig() {
		log.Info("Target cluster differs from operator cluster, creating kubeconfig secret")
		kubeconfigSecret, err := r.deployKubeconfigSecret(ctx, complianceScan)
		if err != nil {
			return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
		}
		kubeconfigSecretName = kubeconfigSecret.Name
		log.Info(fmt.Sprintf("Created kubeconfig Secret %s", client.ObjectKeyFromObject(kubeconfigSecret)))
	} else {
		log.Info("Target cluster is the same as operator cluster, using in-cluster config")
	}

	dikiConfigMap, err := r.deployDikiConfigMap(ctx, complianceScan, len(kubeconfigSecretName) > 0)
	if err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}
	log.Info(fmt.Sprintf("Created ConfigMap %s", client.ObjectKeyFromObject(dikiConfigMap)))

	exporterConfigSecret, err := r.deployExporterConfigSecret(ctx, complianceScan, reportOutputs)
	if err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}
	log.Info(fmt.Sprintf("Created Secret %s", client.ObjectKeyFromObject(exporterConfigSecret)))

	dikiImage, err := imagevector.ImageVector().FindImage("diki")
	if err != nil {
		log.Error(err, "failed to find image version for diki")
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, fmt.Errorf("failed to find image version for %s: %w", "diki-runner", err))
	}
	dikiExporterImage, err := imagevector.ImageVector().FindImage("diki-exporter")
	if err != nil {
		log.Error(err, "failed to find image version for diki-exporter")
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, fmt.Errorf("failed to find image version for %s: %w", "diki-exporter", err))
	}

	// TODO(AleksandarSavchev): Create diki-runner job here.
	dikiRunner, err := r.deployDikiRunner(ctx, dikiImage.String(), dikiExporterImage.String(), dikiConfigMap.Name, exporterConfigSecret.Name, kubeconfigSecretName, complianceScan)
	if err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}
	log.Info(fmt.Sprintf("Created runner pod %s", client.ObjectKeyFromObject(dikiRunner)))

	if err := r.waitPodCompleted(ctx, dikiRunner.Name, dikiRunner.Namespace, log); err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, fmt.Errorf("diki runner pod did not become healthy: %w", err))
	}

	log.Info(fmt.Sprintf("Pod %s completed", client.ObjectKeyFromObject(dikiRunner)))

	// Update phase to Completed
	patch = client.MergeFrom(complianceScan.DeepCopy())
	complianceScan.Status.Phase = v1alpha1.ComplianceScanCompleted
	complianceScan.Status.Conditions = dikiv1alpha1helper.UpdateConditions(
		complianceScan.Status.Conditions,
		v1alpha1.ConditionTypeCompleted,
		v1alpha1.ConditionTrue,
		ConditionReasonCompleted,
		"ComplianceScan has completed successfully",
		time.Now(),
	)
	if err := r.TargetClient.Status().Patch(ctx, complianceScan, patch); err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}

	log.Info("Updated ComplianceScan phase to Completed")

	return ctrl.Result{}, nil
}

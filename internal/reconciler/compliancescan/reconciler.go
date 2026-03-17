// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	Client     client.Client
	RESTConfig *rest.Config
	Config     configv1alpha1.ComplianceScanConfig
}

// Reconcile handles reconciliation requests for ComplianceScan resources.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx).WithValues("name", req.Name)

	complianceScan := &v1alpha1.ComplianceScan{}

	if err := r.Client.Get(ctx, client.ObjectKey{Name: req.Name}, complianceScan); err != nil {
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
	if err := r.Client.Status().Patch(ctx, complianceScan, patch); err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}

	log.Info("Updated ComplianceScan phase to Running")

	configMap, err := r.deployDikiConfigMap(ctx, complianceScan)
	if err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}

	log.Info(fmt.Sprintf("Created ConfigMap %s", client.ObjectKeyFromObject(configMap)))

	dikiImage, err := imagevector.ImageVector().FindImage("diki")
	if err != nil {
		log.Error(err, "failed to find image version for diki")
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, fmt.Errorf("failed to find image version for %s: %w", "diki-runner", err))
	}
	dikiOpsImage, err := imagevector.ImageVector().FindImage("diki-ops")
	if err != nil {
		log.Error(err, "failed to find image version for diki-ops")
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, fmt.Errorf("failed to find image version for %s: %w", "diki-runner", err))
	}

	// TODO(AleksandarSavchev): Create diki-runner job here.
	dikiRunner, err := r.deployDikiRunner(ctx, dikiImage.String(), dikiOpsImage.String(), configMap.Name, complianceScan)
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
	if err := r.Client.Status().Patch(ctx, complianceScan, patch); err != nil {
		return reconcile.Result{}, r.handleFailedScan(ctx, complianceScan, log, err)
	}

	log.Info("Updated ComplianceScan phase to Completed")

	return ctrl.Result{}, nil
}

package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gardener/gardener/pkg/utils/retry"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1"
)

func (r *Reconciler) deployDikiRunner(ctx context.Context, dikiImage, dikiExporterImage, dikiConfigMapName, dikiExporterConfigSecretName, kubeconfigSecretName string, complianceScan *v1alpha1.ComplianceScan) (*corev1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "diki-runner-",
			Namespace:    r.Config.DikiRunner.Namespace,
			Labels:       r.getLabels(complianceScan),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "diki.gardener.cloud/v1alpha1",
					Kind:               "ComplianceScan",
					Name:               complianceScan.Name,
					UID:                complianceScan.UID,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				},
			},
		},
		Spec: corev1.PodSpec{
			ActiveDeadlineSeconds: ptr.To[int64](600),
			InitContainers: []corev1.Container{
				{
					Name:  "diki-scan",
					Image: dikiImage,
					Args: []string{
						"run",
						"--config=/config/config.yaml",
						"--all",
						"--output=/output/report.json",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "shared-volume",
							MountPath: "/output",
						},
						{
							Name:      "diki-config",
							MountPath: "/config",
							ReadOnly:  true,
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "diki-exporter",
					Image: dikiExporterImage,
					Args: []string{
						"--config", "/config/exporter-config.yaml",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "shared-volume",
							MountPath: "/output",
						},
						{
							Name:      "exporter-config",
							MountPath: "/config",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "shared-volume",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "diki-config",
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: dikiConfigMapName,
							},
						},
					},
				},
				{
					Name: "exporter-config",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: dikiExporterConfigSecretName,
						},
					},
				},
			},
			ServiceAccountName: "diki-runner",
			RestartPolicy:      "Never",
			Tolerations: []corev1.Toleration{
				{
					Effect:   "NoSchedule",
					Operator: "Exists",
				},
				{
					Effect:   "NoExecute",
					Operator: "Exists",
				},
			},
		},
	}

	if len(kubeconfigSecretName) > 0 {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
			Name: "target-kubeconfig",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: kubeconfigSecretName,
				},
			},
		})

		pod.Spec.InitContainers[0].VolumeMounts = append(pod.Spec.InitContainers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "target-kubeconfig",
			MountPath: "/kubeconfig",
			ReadOnly:  true,
		})

		// Mount the target kubeconfig on the diki-exporter container as well,
		// so it can reach the target cluster where the ComplianceScan CRDs exist.
		pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "target-kubeconfig",
			MountPath: "/kubeconfig",
			ReadOnly:  true,
		})
		pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args,
			"--kubeconfig", "/kubeconfig/"+KubeconfigKey,
		)
	}

	if err := r.Client.Create(ctx, pod); err != nil {
		return nil, fmt.Errorf("failed to create diki runner pod: %w", err)
	}

	return pod, nil
}

func (r *Reconciler) waitPodCompleted(ctx context.Context, name, namespace string, log logr.Logger) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, r.Config.DikiRunner.PodCompletionTimeout.Duration)
	defer cancel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	return retry.Until(timeoutCtx, time.Second*5, func(ctx context.Context) (done bool, err error) {
		if err := r.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
			log.Info("Oh naur...")
			return retry.SevereError(err)
		}

		if pod.Status.Phase != corev1.PodSucceeded {
			conditions, err := json.Marshal(pod.Status.Conditions)
			if err != nil {
				return retry.MinorError(fmt.Errorf("failed parsing pod %s status conditions: %w", client.ObjectKeyFromObject(pod).String(), err))
			}
			return retry.MinorError(fmt.Errorf("pod %s is not yet Running, pod conditions: %s", client.ObjectKeyFromObject(pod).String(), string(conditions)))
		}

		return retry.Ok()
	})
}

// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1"
)

const dikiRunnerContextName = "diki-runner"

// generateKubeconfig creates a kubeconfig from the TargetRESTConfig
func (r *Reconciler) generateKubeconfig() ([]byte, error) {
	// TODO: Fix this to work in every scenario.
	tokenBytes, err := os.ReadFile("/var/run/secrets/gardener.cloud/shoot/generic-kubeconfig-runner/token")
	if err != nil {
		return nil, fmt.Errorf("failed to read bearer token file %q: %w", r.TargetRESTConfig.BearerTokenFile, err)
	}
	token := string(tokenBytes)

	authInfo := &clientcmdapi.AuthInfo{
		ClientCertificateData: r.TargetRESTConfig.CertData,
		ClientKeyData:         r.TargetRESTConfig.KeyData,
		Token:                 token,
	}

	return kubeconfigWithAuthInfo(r.TargetRESTConfig, authInfo)
}

// kubeconfigWithAuthInfo creates a serialized kubeconfig from a REST config and auth info.
func kubeconfigWithAuthInfo(config *rest.Config, authInfo *clientcmdapi.AuthInfo) ([]byte, error) {
	// Prefer CA file reference; fall back to inline CA data.
	caFile, caData := config.CAFile, []byte{}
	if len(caFile) == 0 {
		caData = config.CAData
	}

	return clientcmd.Write(clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{dikiRunnerContextName: {
			Server:                   config.Host,
			InsecureSkipTLSVerify:    config.Insecure,
			CertificateAuthority:     caFile,
			CertificateAuthorityData: caData,
		}},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{dikiRunnerContextName: authInfo},
		Contexts: map[string]*clientcmdapi.Context{dikiRunnerContextName: {
			Cluster:  dikiRunnerContextName,
			AuthInfo: dikiRunnerContextName,
		}},
		CurrentContext: dikiRunnerContextName,
	})
}

// deployKubeconfigSecret creates a Secret containing the kubeconfig for the target cluster
func (r *Reconciler) deployKubeconfigSecret(ctx context.Context, complianceScan *v1alpha1.ComplianceScan) (*corev1.Secret, error) {
	kubeconfigBytes, err := r.generateKubeconfig()
	if err != nil {
		return nil, fmt.Errorf("failed to generate kubeconfig: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: KubeconfigSecretGenerateNamePrefix,
			Namespace:    r.Config.DikiRunner.Namespace,
			Labels:       r.getLabels(complianceScan),
		},
		Data: map[string][]byte{
			KubeconfigKey: kubeconfigBytes,
		},
	}

	if err := r.Client.Create(ctx, secret); err != nil {
		return nil, fmt.Errorf("failed to create kubeconfig secret: %w", err)
	}

	return secret, nil
}

// needsKubeconfig returns true if the target cluster is different from the operator cluster
func (r *Reconciler) needsKubeconfig() bool {
	// Compare the hosts to determine if they're different clusters
	return r.TargetRESTConfig.Host != r.RESTConfig.Host
}

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
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/gardener/diki-operator/pkg/apis/diki/v1alpha1"
)

// generateKubeconfig creates a kubeconfig from the TargetRESTConfig
func (r *Reconciler) generateKubeconfig() ([]byte, error) {
	// When the REST config was loaded from a kubeconfig with a tokenFile reference
	// (e.g. Gardener's generic token kubeconfig), BearerToken is empty and the token
	// lives in BearerTokenFile. Read it so we can embed it inline in the generated kubeconfig.
	token := r.TargetRESTConfig.BearerToken
	if token == "" && r.TargetRESTConfig.BearerTokenFile != "" {
		tokenBytes, err := os.ReadFile(r.TargetRESTConfig.BearerTokenFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read bearer token file %q: %w", r.TargetRESTConfig.BearerTokenFile, err)
		}
		token = string(tokenBytes)
	}

	config := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"target-cluster": {
				Server:                   r.TargetRESTConfig.Host,
				CertificateAuthorityData: r.TargetRESTConfig.CAData,
				InsecureSkipTLSVerify:    r.TargetRESTConfig.Insecure,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"target-user": {
				ClientCertificateData: r.TargetRESTConfig.CertData,
				ClientKeyData:         r.TargetRESTConfig.KeyData,
				Token:                 token,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"target-context": {
				Cluster:  "target-cluster",
				AuthInfo: "target-user",
			},
		},
		CurrentContext: "target-context",
	}

	return clientcmd.Write(config)
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

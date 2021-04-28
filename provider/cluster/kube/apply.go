package kube

// nolint:deadcode,golint

import (
	"context"

	akashv1 "github.com/ovrclk/akash/pkg/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func applyNS(ctx context.Context, kc kubernetes.Interface, b *nsBuilder) error {
	obj, err := kc.CoreV1().Namespaces().Get(ctx, b.name(), metav1.GetOptions{})
	label := "success"
	if err != nil && !errors.IsNotFound(err) {
		label = "fail"
	}
	kubeCallsCounter.WithLabelValues("namespaces-get", label).Inc()

	switch {
	case err == nil:
		obj, err = b.update(obj)
		if err == nil {
			_, err = kc.CoreV1().Namespaces().Update(ctx, obj, metav1.UpdateOptions{})
			label := "success"
			if err != nil {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("namespaces-update", label).Inc()
		}
	case errors.IsNotFound(err):
		obj, err = b.create()
		if err == nil {
			_, err = kc.CoreV1().Namespaces().Create(ctx, obj, metav1.CreateOptions{})
			label := "success"
			if err != nil {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("namespaces-create", label).Inc()
		}
	}
	return err
}

// Apply list of Network Policies
func applyNetPolicies(ctx context.Context, kc kubernetes.Interface, b *netPolBuilder) error {
	var err error

	policies, err := b.create()
	if err != nil {
		return err
	}

	for _, pol := range policies {
		obj, err := kc.NetworkingV1().NetworkPolicies(b.ns()).Get(ctx, pol.Name, metav1.GetOptions{})
		label := "success"
		if err != nil && !errors.IsNotFound(err) {
			label = "fail"
		}
		kubeCallsCounter.WithLabelValues("networking-policies-get", label).Inc()
		switch {
		case err == nil:
			_, err = b.update(obj)
			if err == nil {
				_, err = kc.NetworkingV1().NetworkPolicies(b.ns()).Update(ctx, pol, metav1.UpdateOptions{})
				label := "success"
				if err != nil {
					label = "fail"
				}
				kubeCallsCounter.WithLabelValues("networking-policies-update", label).Inc()
			}
		case errors.IsNotFound(err):
			_, err = kc.NetworkingV1().NetworkPolicies(b.ns()).Create(ctx, pol, metav1.CreateOptions{})
			label := "success"
			if err != nil {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("networking-policies-create", label).Inc()
		}
		if err != nil {
			break
		}
	}

	return err
}

// TODO: re-enable.  see #946
// func applyRestrictivePodSecPoliciesToNS(ctx context.Context, kc kubernetes.Interface, p *pspRestrictedBuilder) error {
// 	obj, err := kc.PolicyV1beta1().PodSecurityPolicies().Get(ctx, p.name(), metav1.GetOptions{})
// 	switch {
// 	case err == nil:
// 		obj, err = p.update(obj)
// 		if err == nil {
// 			_, err = kc.PolicyV1beta1().PodSecurityPolicies().Update(ctx, obj, metav1.UpdateOptions{})
// 		}
// 	case errors.IsNotFound(err):
// 		obj, err = p.create()
// 		if err == nil {
// 			_, err = kc.PolicyV1beta1().PodSecurityPolicies().Create(ctx, obj, metav1.CreateOptions{})
// 		}
// 	}
// 	return err
// }

func applyDeployment(ctx context.Context, kc kubernetes.Interface, b *deploymentBuilder) error {
	obj, err := kc.AppsV1().Deployments(b.ns()).Get(ctx, b.name(), metav1.GetOptions{})

	label := "success"
	if err != nil && !errors.IsNotFound(err) {
		label = "fail"
	}
	kubeCallsCounter.WithLabelValues("deployments-get", label).Inc()

	switch {
	case err == nil:
		obj, err = b.update(obj)

		if err == nil {
			_, err = kc.AppsV1().Deployments(b.ns()).Update(ctx, obj, metav1.UpdateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("deployments-update", label).Inc()
		}
	case errors.IsNotFound(err):
		obj, err = b.create()
		if err == nil {
			_, err = kc.AppsV1().Deployments(b.ns()).Create(ctx, obj, metav1.CreateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("deployments-create", label).Inc()
		}
	}
	return err
}

func applyService(ctx context.Context, kc kubernetes.Interface, b *serviceBuilder) error {
	obj, err := kc.CoreV1().Services(b.ns()).Get(ctx, b.name(), metav1.GetOptions{})
	label := "success"
	if err != nil && !errors.IsNotFound(err) {
		label = "fail"
	}
	kubeCallsCounter.WithLabelValues("services-get", label).Inc()
	switch {
	case err == nil:
		obj, err = b.update(obj)
		if err == nil {
			_, err = kc.CoreV1().Services(b.ns()).Update(ctx, obj, metav1.UpdateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("services-update", label).Inc()
		}
	case errors.IsNotFound(err):
		obj, err = b.create()
		if err == nil {
			_, err = kc.CoreV1().Services(b.ns()).Create(ctx, obj, metav1.CreateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("services-create", label).Inc()
		}
	}
	return err
}

func applyIngress(ctx context.Context, kc kubernetes.Interface, b *ingressBuilder) error {
	obj, err := kc.NetworkingV1().Ingresses(b.ns()).Get(ctx, b.name(), metav1.GetOptions{})
	label := "success"
	if err != nil && !errors.IsNotFound(err) {
		label = "fail"
	}
	kubeCallsCounter.WithLabelValues("ingresses-get", label).Inc()
	switch {
	case err == nil:
		obj, err = b.update(obj)
		if err == nil {
			_, err = kc.NetworkingV1().Ingresses(b.ns()).Update(ctx, obj, metav1.UpdateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("networking-ingresses-update", label).Inc()
		}
	case errors.IsNotFound(err):
		obj, err = b.create()
		if err == nil {
			_, err = kc.NetworkingV1().Ingresses(b.ns()).Create(ctx, obj, metav1.CreateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("networking-ingreses-create", label).Inc()
		}
	}
	return err
}

func prepareEnvironment(ctx context.Context, kc kubernetes.Interface, ns string) error {
	_, err := kc.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	label := "success"
	if err != nil && !errors.IsNotFound(err) {
		label = "fail"
	}
	kubeCallsCounter.WithLabelValues("namespaces-get", label).Inc()

	if errors.IsNotFound(err) {
		obj := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: ns,
				Labels: map[string]string{
					akashManagedLabelName: "true",
				},
			},
		}
		_, err = kc.CoreV1().Namespaces().Create(ctx, obj, metav1.CreateOptions{})
		label := "success"
		if err != nil  {
			label = "fail"
		}
		kubeCallsCounter.WithLabelValues("namespaces-create", label).Inc()
	}
	return err
}

func applyManifest(ctx context.Context, kc akashv1.Interface, b *manifestBuilder) error {
	obj, err := kc.AkashV1().Manifests(b.ns()).Get(ctx, b.name(), metav1.GetOptions{})

	label := "success"
	if err != nil && !errors.IsNotFound(err) {
		label = "fail"
	}
	kubeCallsCounter.WithLabelValues("akash-manifests-get", label).Inc()

	switch {
	case err == nil:
		obj, err = b.update(obj)
		if err == nil {
			_, err = kc.AkashV1().Manifests(b.ns()).Update(ctx, obj, metav1.UpdateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("akash-manifests-update", label).Inc()
		}
	case errors.IsNotFound(err):
		obj, err = b.create()
		if err == nil {
			_, err = kc.AkashV1().Manifests(b.ns()).Create(ctx, obj, metav1.CreateOptions{})
			label := "success"
			if err != nil  {
				label = "fail"
			}
			kubeCallsCounter.WithLabelValues("akash-manifests-create", label).Inc()
		}
	}
	return err
}

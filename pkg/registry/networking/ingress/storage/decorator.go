package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/kubernetes/pkg/apis/networking"
)

func fleetDecorator(obj runtime.Object) {
	switch obj.(type) {
	case *networking.Ingress:
		ing := obj.(*networking.Ingress)
		setClusterNameSuffix(ing)
	case *networking.IngressList:
		items := obj.(*networking.IngressList).Items
		for i := range items {
			setClusterNameSuffix(&items[i])
		}
	default:
	}
}

func setClusterNameSuffix(ing *networking.Ingress) {
	if ing == nil || ing.Name == "" {
		return
	}

	_, clusterName := genericregistry.ParseNameFromResourceName(ing.Name, false)
	if clusterName == genericregistry.KarmadaCluster {
		return
	}

	setSuffixForIngressClassName(ing.Spec.IngressClassName, clusterName)
	setSuffixForBackend(ing.Spec.DefaultBackend, clusterName)
	setSuffixForRules(ing.Spec.Rules, clusterName)
	setSuffixForTLSConfigurations(ing.Spec.TLS, clusterName)
}

func setSuffixForIngressClassName(ingClass *string, clusterName string) {
	if ingClass == nil {
		return
	}
	*ingClass += ".clusterspace." + clusterName
}

func setSuffixForRules(rules []networking.IngressRule, clusterName string) {
	for i := range rules {
		rule := &rules[i]
		if rule.HTTP == nil {
			continue
		}
		paths := rule.HTTP.Paths
		for j := range paths {
			setSuffixForBackend(&paths[j].Backend, clusterName)
		}
	}
}

func setSuffixForBackend(backend *networking.IngressBackend, clusterName string) {
	if backend == nil {
		return
	}
	if svc := backend.Service; svc != nil {
		svc.Name += ".clusterspace." + clusterName
	}
	if resource := backend.Resource; resource != nil {
		resource.Name += ".clusterspace." + clusterName
	}
}

func setSuffixForTLSConfigurations(tlsConfigs []networking.IngressTLS, clusterName string) {
	for i := range tlsConfigs {
		tlsConfigs[i].SecretName += ".clusterspace." + clusterName
	}
}

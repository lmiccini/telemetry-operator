package autoscaling

import (
	telemetryv1 "github.com/openstack-k8s-operators/telemetry-operator/api/v1beta1"
	monv1 "github.com/rhobs/obo-prometheus-operator/pkg/apis/monitoring/v1"
	obov1 "github.com/rhobs/observability-operator/pkg/apis/monitoring/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Prometheus(
	instance *telemetryv1.Autoscaling,
	labels map[string]string,
) (*obov1.MonitoringStack, error) {
	var replicas int32 = 1
	prometheus := &obov1.MonitoringStack{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName,
			Namespace: instance.Namespace,
			Labels:    labels,
		},
		Spec: obov1.MonitoringStackSpec{
			AlertmanagerConfig: obov1.AlertmanagerConfig{
				Disabled: true,
			},
			PrometheusConfig: &obov1.PrometheusConfig{
				Replicas: &replicas,
			},
			Retention: monv1.Duration("5h"),
			LogLevel:  "debug",
			ResourceSelector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		},
	}
	return prometheus, nil
}

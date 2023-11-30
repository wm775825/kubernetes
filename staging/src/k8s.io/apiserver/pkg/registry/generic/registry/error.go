package registry

import (
	"fmt"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func InterpretGetClusterError(err error, clusterName string) error {
	return InterpretGetError(err, schema.GroupResource{Group: clusterv1alpha1.GroupName, Resource: "cluster"}, clusterName, "failed to get cluster %s: %v", clusterName)
}

// InterpretGetError converts a generic error on a retrieval
// operation into the appropriate API error.
func InterpretGetError(err error, qualifiedResource schema.GroupResource, name string, format string, args ...interface{}) error {
	switch {
	case apierrors.IsNotFound(err):
		return apierrors.NewNotFound(qualifiedResource, name)
	case apierrors.IsServiceUnavailable(err):
		return apierrors.NewServerTimeout(qualifiedResource, "get", 2) // TODO: make configurable or handled at a higher level
	case apierrors.IsInternalError(err):
		return apierrors.NewInternalError(err)
	default:
		if format != "" {
			return fmt.Errorf(format, append(args, err)...)
		}
		return err
	}
}

// InterpretListError converts a generic error on a retrieval
// operation into the appropriate API error.
func InterpretListError(err error, qualifiedResource schema.GroupResource, format string, args ...interface{}) error {
	switch {
	case apierrors.IsNotFound(err):
		return apierrors.NewNotFound(qualifiedResource, "")
	case apierrors.IsServiceUnavailable(err):
		return apierrors.NewServerTimeout(qualifiedResource, "list", 2) // TODO: make configurable or handled at a higher level
	case apierrors.IsInternalError(err):
		return apierrors.NewInternalError(err)
	default:
		if format != "" {
			return fmt.Errorf(format, append(args, err)...)
		}
		return err
	}
}

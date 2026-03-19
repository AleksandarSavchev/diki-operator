package utils

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// ToRawExtension marshals v to a runtime.RawExtension.
func ToRawExtension(v any) runtime.RawExtension {
	if v == nil {
		return runtime.RawExtension{}
	}

	data, err := json.Marshal(v)
	if err != nil {
		errData, _ := json.Marshal(map[string]string{"error": fmt.Sprintf("failed to marshal details: %v", err)})
		return runtime.RawExtension{Raw: errData}
	}

	return runtime.RawExtension{Raw: data}
}

package mq

import (
	"fmt"
	"math/rand"
	"time"

	"go.starlark.net/starlark"
)

// Utility functions for the MQ module

// splitIntoBatches splits a slice into batches of specified size
func splitIntoBatches[T any](items []T, batchSize int) [][]T {
	if len(items) == 0 {
		return nil
	}

	if batchSize <= 0 {
		batchSize = 1
	}

	batches := make([][]T, 0, (len(items)+batchSize-1)/batchSize)

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}

	return batches
}

// splitBatchMessages splits BatchMessage slice into batches
func splitBatchMessages(messages []BatchMessage, batchSize int) [][]BatchMessage {
	return splitIntoBatches(messages, batchSize)
}

// validateQueueName validates a queue name according to common rules
func validateQueueName(name string) error {
	if name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	if len(name) > 80 {
		return fmt.Errorf("queue name cannot be longer than 80 characters")
	}

	// Basic validation - alphanumeric, hyphens, underscores
	for _, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-' || char == '_') {
			return fmt.Errorf("queue name contains invalid character: %c", char)
		}
	}

	return nil
}

// validateMessageBody validates message body
func validateMessageBody(body string) error {
	if len(body) == 0 {
		return fmt.Errorf("message body cannot be empty")
	}

	// Check size limit (256KB is common limit)
	if len(body) > 256*1024 {
		return fmt.Errorf("message body too large (max 256KB)")
	}

	return nil
}

// normalizeProperties converts various property value types to string/supported types
func normalizeProperties(properties map[string]interface{}) map[string]interface{} {
	if properties == nil {
		return nil
	}

	normalized := make(map[string]interface{})
	for k, v := range properties {
		switch val := v.(type) {
		case string:
			normalized[k] = val
		case int, int32, int64:
			normalized[k] = fmt.Sprintf("%d", val)
		case float32, float64:
			normalized[k] = fmt.Sprintf("%f", val)
		case bool:
			if val {
				normalized[k] = "true"
			} else {
				normalized[k] = "false"
			}
		default:
			normalized[k] = fmt.Sprintf("%v", val)
		}
	}

	return normalized
}

// generateMessageID generates a unique message ID
func generateMessageID() string {
	// Simple implementation - in practice, this might use UUIDs
	return fmt.Sprintf("msg-%d-%d", time.Now().Unix(), rand.Int63())
}

// getServiceBatchLimit returns the batch size limit for a service
func getServiceBatchLimit(serviceType string) int {
	switch serviceType {
	case ServiceTypeAWSSQS:
		return 10
	case ServiceTypeAzureServiceBus:
		return 100
	default:
		return 10 // Conservative default
	}
}

// adaptBatchSize adapts batch size to service limits
func adaptBatchSize(serviceType string, requestedSize int) int {
	limit := getServiceBatchLimit(serviceType)
	if requestedSize <= 0 || requestedSize > limit {
		return limit
	}
	return requestedSize
}

// coalesceInt returns the first non-zero int
func coalesceInt(values ...int) int {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// convertStarlarkDictToInterface converts a Starlark dictionary to a Go map[string]interface{}
func convertStarlarkDictToInterface(dict *starlark.Dict) (map[string]interface{}, error) {
	if dict == nil || dict.Len() == 0 {
		return nil, nil
	}

	result := make(map[string]interface{}, dict.Len())
	for _, item := range dict.Items() {
		keyStr, ok := item[0].(starlark.String)
		if !ok {
			return nil, fmt.Errorf("dictionary key must be a string, got %T", item[0])
		}

		key := keyStr.GoString()
		val := item[1]

		// Convert Starlark values to Go types
		switch v := val.(type) {
		case starlark.String:
			result[key] = v.GoString()
		case starlark.Int:
			intVal, ok := v.Int64()
			if !ok {
				return nil, fmt.Errorf("integer value too large for key %s", key)
			}
			result[key] = int(intVal)
		case starlark.Float:
			result[key] = float64(v)
		case starlark.Bool:
			result[key] = bool(v)
		case *starlark.Dict:
			// Nested dictionary
			nestedMap, err := convertStarlarkDictToInterface(v)
			if err != nil {
				return nil, fmt.Errorf("error converting nested dict for key %s: %w", key, err)
			}
			result[key] = nestedMap
		case *starlark.List:
			// Convert list to slice
			listVal := make([]interface{}, v.Len())
			for i := 0; i < v.Len(); i++ {
				elem := v.Index(i)
				switch e := elem.(type) {
				case starlark.String:
					listVal[i] = e.GoString()
				case starlark.Int:
					intVal, ok := e.Int64()
					if !ok {
						return nil, fmt.Errorf("integer value too large in list for key %s", key)
					}
					listVal[i] = int(intVal)
				case starlark.Float:
					listVal[i] = float64(e)
				case starlark.Bool:
					listVal[i] = bool(e)
				default:
					listVal[i] = e.String() // Fallback to string representation
				}
			}
			result[key] = listVal
		default:
			result[key] = val.String() // Fallback to string representation
		}
	}

	return result, nil
}

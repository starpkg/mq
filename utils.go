package mq

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"time"

	"github.com/1set/starlet/dataconv"
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

// validateQueueName validates a queue name according to AWS and Azure naming rules
func validateQueueName(name string) error {
	if name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}

	// Length checks: AWS allows up to 80 chars, Azure up to 260
	if len(name) > 80 {
		return fmt.Errorf("queue name cannot be longer than 80 characters")
	}

	// Unified naming rules compatible with both AWS and Azure:
	// - Start and end with alphanumeric
	// - Can contain alphanumeric, hyphens, underscores
	// - No consecutive hyphens or underscores
	nameRegex := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$`)

	if !nameRegex.MatchString(name) {
		return fmt.Errorf("queue name must start and end with alphanumeric characters and contain only letters, numbers, hyphens, and underscores")
	}

	// Check for consecutive special characters
	consecutiveRegex := regexp.MustCompile(`[_-]{2,}`)
	if consecutiveRegex.MatchString(name) {
		return fmt.Errorf("queue name cannot contain consecutive hyphens or underscores")
	}

	return nil
}

// validateMessageBody validates message body
func validateMessageBody(body string) error {
	if len(body) == 0 {
		return fmt.Errorf("message body cannot be empty")
	}
	// Only check if empty, no length limits as per requirements
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

// generateMessageID generates a unique message ID starting with "Star"
func generateMessageID() string {
	// Generate true random bytes
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to timestamp-based random if crypto/rand fails
		randomBytes = []byte(fmt.Sprintf("%08d", time.Now().Nanosecond()))
	}

	// Convert to hex string
	randomHex := fmt.Sprintf("%x", randomBytes)

	// Format: Star-{timestamp}-{random_hex}
	return fmt.Sprintf("Star-%d-%s", time.Now().Unix(), randomHex)
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

// messageResultSliceToStarlark converts a slice of MessageResult to Starlark list
func messageResultSliceToStarlark(messages []*MessageResult) (starlark.Value, error) {
	values := make([]starlark.Value, len(messages))
	for i, msg := range messages {
		val, err := msg.Struct()
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return starlark.NewList(values), nil
}

// boolSliceToStarlark converts a slice of bool to Starlark list
func boolSliceToStarlark(bools []bool) starlark.Value {
	values := make([]starlark.Value, len(bools))
	for i, b := range bools {
		values[i] = starlark.Bool(b)
	}
	return starlark.NewList(values)
}

// convertStarlarkDictToInterface converts a Starlark dictionary to a Go map[string]interface{}
func convertStarlarkDictToInterface(dict *starlark.Dict) (map[string]interface{}, error) {
	if dict == nil || dict.Len() == 0 {
		return nil, nil
	}

	// Use dataconv.Unmarshal for conversion
	result, err := dataconv.Unmarshal(dict)
	if err != nil {
		return nil, fmt.Errorf("failed to convert Starlark dict to map[string]interface{}: %w", err)
	}

	// Verify that the result is indeed a map[string]interface{}
	resultMap, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("dataconv.Unmarshal did not return map[string]interface{}, got %T", result)
	}

	return resultMap, nil
}

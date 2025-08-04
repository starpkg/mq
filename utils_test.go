package mq

import (
	"testing"
)

// TestCoalesceInt tests the coalesceInt utility function
func TestCoalesceInt(t *testing.T) {
	tests := []struct {
		name     string
		value    int
		fallback int
		expected int
	}{
		{
			name:     "non-zero value",
			value:    42,
			fallback: 10,
			expected: 42,
		},
		{
			name:     "zero value uses fallback",
			value:    0,
			fallback: 10,
			expected: 10,
		},
		{
			name:     "negative value",
			value:    -5,
			fallback: 10,
			expected: -5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := coalesceInt(tt.value, tt.fallback)
			if result != tt.expected {
				t.Errorf("coalesceInt(%d, %d) = %d, want %d", tt.value, tt.fallback, result, tt.expected)
			}
		})
	}
}

// TestGenerateMessageID tests the generateMessageID utility function
func TestGenerateMessageID(t *testing.T) {
	id1 := generateMessageID()
	id2 := generateMessageID()

	if id1 == "" {
		t.Error("generateMessageID() returned empty string")
	}

	if id1 == id2 {
		t.Error("generateMessageID() returned duplicate IDs")
	}

	// Test that IDs have reasonable length (UUIDs are typically 36 characters)
	if len(id1) < 10 {
		t.Errorf("generateMessageID() returned ID that's too short: %s", id1)
	}
}

// TestNormalizeProperties tests the normalizeProperties utility function
func TestNormalizeProperties(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: map[string]interface{}{},
		},
		{
			name:     "empty input",
			input:    map[string]interface{}{},
			expected: map[string]interface{}{},
		},
		{
			name: "mixed types",
			input: map[string]interface{}{
				"string_key": "value",
				"int_key":    42,
				"bool_key":   true,
			},
			expected: map[string]interface{}{
				"string_key": "value",
				"int_key":    "42",   // normalized to string
				"bool_key":   "true", // normalized to string
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeProperties(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("normalizeProperties() returned %d properties, want %d", len(result), len(tt.expected))
			}

			for key, expected := range tt.expected {
				actual, exists := result[key]
				if !exists {
					t.Errorf("normalizeProperties() missing key %s", key)
					continue
				}
				if actual != expected {
					t.Errorf("normalizeProperties()[%s] = %v, want %v", key, actual, expected)
				}
			}
		})
	}
}

// TestAdaptBatchSize tests the adaptBatchSize utility function
func TestAdaptBatchSize(t *testing.T) {
	tests := []struct {
		name          string
		serviceType   string
		requestedSize int
		expected      int
	}{
		{
			name:          "AWS SQS max limit",
			serviceType:   "aws_sqs",
			requestedSize: 50,
			expected:      10,
		},
		{
			name:          "AWS SQS within limit",
			serviceType:   "aws_sqs",
			requestedSize: 5,
			expected:      5,
		},
		{
			name:          "Azure Service Bus max limit",
			serviceType:   "azure_servicebus",
			requestedSize: 200,
			expected:      100,
		},
		{
			name:          "Azure Service Bus within limit",
			serviceType:   "azure_servicebus",
			requestedSize: 50,
			expected:      50,
		},
		{
			name:          "unknown service defaults to AWS limit",
			serviceType:   "unknown",
			requestedSize: 50,
			expected:      10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := adaptBatchSize(tt.serviceType, tt.requestedSize)
			if result != tt.expected {
				t.Errorf("adaptBatchSize(%s, %d) = %d, want %d", tt.serviceType, tt.requestedSize, result, tt.expected)
			}
		})
	}
}

// TestMin tests the min utility function
func TestMin(t *testing.T) {
	tests := []struct {
		name     string
		a, b     int
		expected int
	}{
		{
			name:     "a smaller than b",
			a:        5,
			b:        10,
			expected: 5,
		},
		{
			name:     "b smaller than a",
			a:        10,
			b:        5,
			expected: 5,
		},
		{
			name:     "a equals b",
			a:        7,
			b:        7,
			expected: 7,
		},
		{
			name:     "negative numbers",
			a:        -5,
			b:        -10,
			expected: -10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := min(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestMax tests the max utility function
func TestMax(t *testing.T) {
	tests := []struct {
		name     string
		a, b     int
		expected int
	}{
		{
			name:     "a greater than b",
			a:        10,
			b:        5,
			expected: 10,
		},
		{
			name:     "b greater than a",
			a:        5,
			b:        10,
			expected: 10,
		},
		{
			name:     "a equals b",
			a:        7,
			b:        7,
			expected: 7,
		},
		{
			name:     "negative numbers",
			a:        -5,
			b:        -10,
			expected: -5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := max(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("max(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

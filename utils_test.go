package mq

// Unit tests for the pure helper functions in utils.go.
//
// Sections (add a new helper test as a section here, not a new file):
//   - TestCoalesceInt / TestMin / TestMax            : numeric helpers
//   - TestGenerateMessageID                          : message-ID shape + uniqueness
//   - TestNormalizeProperties                        : property value normalization
//   - TestAdaptBatchSize / TestGetServiceBatchLimit  : per-service batch limits
//   - TestValidateQueueName                          : unified queue-name rules
//   - TestValidateMessageBody                        : message-body validation
//   - TestSplitIntoBatches                           : generic batch splitting
//   - TestConvertStarlarkDictToInterface             : Starlark dict -> Go map

import (
	"fmt"
	"strings"
	"testing"

	"go.starlark.net/starlark"
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

	// Format is "Star-<unix>-<16 hex chars>".
	if !strings.HasPrefix(id1, "Star-") {
		t.Errorf("generateMessageID() = %q, want prefix %q", id1, "Star-")
	}
	parts := strings.Split(id1, "-")
	if len(parts) != 3 {
		t.Fatalf("generateMessageID() = %q, want 3 dash-separated parts, got %d", id1, len(parts))
	}
	if len(parts[2]) != 16 {
		t.Errorf("generateMessageID() random suffix = %q (len %d), want 16 hex chars", parts[2], len(parts[2]))
	}
	for _, r := range parts[2] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Errorf("generateMessageID() suffix %q contains non-hex rune %q", parts[2], r)
			break
		}
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
		{
			name: "numeric width and float and false",
			input: map[string]interface{}{
				"i32":   int32(7),
				"i64":   int64(-9),
				"f32":   float32(1.5),
				"f64":   float64(2.25),
				"falsy": false,
			},
			expected: map[string]interface{}{
				"i32":   "7",
				"i64":   "-9",
				"f32":   "1.500000", // %f formatting
				"f64":   "2.250000",
				"falsy": "false",
			},
		},
		{
			name: "unsupported type falls through to %v",
			input: map[string]interface{}{
				"slice": []int{1, 2},
			},
			expected: map[string]interface{}{
				"slice": "[1 2]",
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

// TestGetServiceBatchLimit checks the per-service batch caps used by adaptBatchSize.
func TestGetServiceBatchLimit(t *testing.T) {
	tests := []struct {
		service string
		want    int
	}{
		{ServiceTypeAWSSQS, 10},
		{ServiceTypeAzureServiceBus, 100},
		{"", 10},        // conservative default
		{"unknown", 10}, // conservative default
	}
	for _, tt := range tests {
		if got := getServiceBatchLimit(tt.service); got != tt.want {
			t.Errorf("getServiceBatchLimit(%q) = %d, want %d", tt.service, got, tt.want)
		}
	}
}

// TestCoalesceIntVariadic exercises the variadic first-non-zero behavior.
func TestCoalesceIntVariadic(t *testing.T) {
	tests := []struct {
		name   string
		values []int
		want   int
	}{
		{"all zero", []int{0, 0, 0}, 0},
		{"empty", nil, 0},
		{"first non-zero wins", []int{0, 0, 5, 7}, 5},
		{"single", []int{3}, 3},
		{"negative is non-zero", []int{0, -2, 9}, -2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coalesceInt(tt.values...); got != tt.want {
				t.Errorf("coalesceInt(%v) = %d, want %d", tt.values, got, tt.want)
			}
		})
	}
}

// TestValidateQueueName covers the unified AWS/Azure naming rules.
func TestValidateQueueName(t *testing.T) {
	tests := []struct {
		name      string
		queue     string
		wantErr   bool
		errSubstr string
	}{
		{"simple", "orders", false, ""},
		{"single char", "a", false, ""},
		{"single digit", "9", false, ""},
		{"with hyphen", "my-queue", false, ""},
		{"with underscore", "my_queue", false, ""},
		{"alnum mix", "Queue123", false, ""},
		{"max length 80", strings.Repeat("a", 80), false, ""},
		{"empty", "", true, "cannot be empty"},
		{"too long 81", strings.Repeat("a", 81), true, "longer than 80"},
		{"leading hyphen", "-queue", true, "start and end with alphanumeric"},
		{"trailing hyphen", "queue-", true, "start and end with alphanumeric"},
		{"leading underscore", "_queue", true, "start and end with alphanumeric"},
		{"space", "my queue", true, "start and end with alphanumeric"},
		{"dot", "my.queue", true, "start and end with alphanumeric"},
		{"consecutive hyphens", "a--b", true, "consecutive hyphens or underscores"},
		{"consecutive underscores", "a__b", true, "consecutive hyphens or underscores"},
		{"mixed consecutive", "a-_b", true, "consecutive hyphens or underscores"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueueName(tt.queue)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateQueueName(%q) = nil, want error", tt.queue)
				}
				if !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("validateQueueName(%q) error = %q, want substring %q", tt.queue, err, tt.errSubstr)
				}
				return
			}
			if err != nil {
				t.Errorf("validateQueueName(%q) = %v, want nil", tt.queue, err)
			}
		})
	}
}

// TestValidateMessageBody checks that only an empty body is rejected.
func TestValidateMessageBody(t *testing.T) {
	if err := validateMessageBody(""); err == nil {
		t.Error("validateMessageBody(\"\") = nil, want error")
	} else if !strings.Contains(err.Error(), "cannot be empty") {
		t.Errorf("validateMessageBody(\"\") error = %q, want substring %q", err, "cannot be empty")
	}
	for _, body := range []string{"x", " ", strings.Repeat("y", 1<<16)} {
		if err := validateMessageBody(body); err != nil {
			t.Errorf("validateMessageBody(len=%d) = %v, want nil", len(body), err)
		}
	}
}

// TestSplitIntoBatches covers the generic batch splitter, including edge sizes.
func TestSplitIntoBatches(t *testing.T) {
	tests := []struct {
		name      string
		items     []int
		batchSize int
		want      [][]int
	}{
		{"empty input", nil, 3, nil},
		{"size larger than len", []int{1, 2}, 10, [][]int{{1, 2}}},
		{"exact multiple", []int{1, 2, 3, 4}, 2, [][]int{{1, 2}, {3, 4}}},
		{"with remainder", []int{1, 2, 3, 4, 5}, 2, [][]int{{1, 2}, {3, 4}, {5}}},
		{"size one", []int{1, 2, 3}, 1, [][]int{{1}, {2}, {3}}},
		{"zero size clamps to one", []int{1, 2}, 0, [][]int{{1}, {2}}},
		{"negative size clamps to one", []int{1, 2}, -5, [][]int{{1}, {2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitIntoBatches(tt.items, tt.batchSize)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("splitIntoBatches(%v, %d) = %v, want %v", tt.items, tt.batchSize, got, tt.want)
			}
		})
	}
}

// TestConvertStarlarkDictToInterface checks dict conversion, nil/empty short-circuits,
// and that a cyclic dict is rejected (not a host hang/overflow).
func TestConvertStarlarkDictToInterface(t *testing.T) {
	// nil dict returns (nil, nil).
	if m, err := convertStarlarkDictToInterface(nil); err != nil || m != nil {
		t.Errorf("convertStarlarkDictToInterface(nil) = (%v, %v), want (nil, nil)", m, err)
	}

	// empty dict returns (nil, nil).
	if m, err := convertStarlarkDictToInterface(starlark.NewDict(0)); err != nil || m != nil {
		t.Errorf("convertStarlarkDictToInterface(empty) = (%v, %v), want (nil, nil)", m, err)
	}

	// populated dict round-trips string/int/bool values.
	d := starlark.NewDict(3)
	_ = d.SetKey(starlark.String("name"), starlark.String("orders"))
	_ = d.SetKey(starlark.String("count"), starlark.MakeInt(42))
	_ = d.SetKey(starlark.String("flag"), starlark.True)
	m, err := convertStarlarkDictToInterface(d)
	if err != nil {
		t.Fatalf("convertStarlarkDictToInterface(populated) error = %v", err)
	}
	if m["name"] != "orders" {
		t.Errorf("m[name] = %v, want orders", m["name"])
	}
	if m["count"] != int64(42) && m["count"] != 42 {
		t.Errorf("m[count] = %v (%T), want 42", m["count"], m["count"])
	}
	if m["flag"] != true {
		t.Errorf("m[flag] = %v, want true", m["flag"])
	}

	// A self-referential dict must be rejected with an error, not loop/overflow.
	cyclic := starlark.NewDict(1)
	if err := cyclic.SetKey(starlark.String("self"), cyclic); err != nil {
		t.Fatalf("failed to build cyclic dict: %v", err)
	}
	if _, err := convertStarlarkDictToInterface(cyclic); err == nil {
		t.Error("convertStarlarkDictToInterface(cyclic) = nil error, want cyclic-reference error")
	}
}

package mq

// Public unit tests for the non-TTY/non-network logic of the mq module.
//
// These tests exercise everything that runs WITHOUT a live AWS/Azure endpoint:
// argument parsing/validation, pure converters/formatters, the error taxonomy,
// config defaulting, and the offline (stub / pre-network error) branches of the
// script-facing builtins. The live-service integration scripts live in the
// private ../test/mq suite (driven by example_test.go) and auto-skip in CI.
//
// Sections:
//   - error taxonomy : TestNormalizeError, TestMQErrorString, TestMQErrorIs,
//                       TestErrorContainsHelper, TestIsRetryableError
//   - config         : TestClientConfigValidate, TestClientConfigGetters,
//                       TestDetectServiceType, TestGetConfigValue
//   - result types   : TestQueueStruct, TestQueueHelpers, TestMessageResultHelpers
//   - backend pure   : TestExtractNamespace, TestParseDuration,
//                       TestConvertToSQSMessageAttributes, TestServiceBusConverters,
//                       TestConvertToSQSAttributes
//   - wrapper object : TestClientWrapperSurface
//   - script surface : TestConnectScript, TestConnectValidationErrors,
//                       TestBuiltinArgErrors, TestOfflineStubMethods,
//                       TestModuleBuiltins

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/1set/starlet"
	"go.starlark.net/starlark"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// runMQScript executes a Starlark script against a fresh, default mq module and
// returns the resulting globals (as Go-native values) plus any error. No network
// is involved as long as the script does not call methods that reach the live
// service.
func runMQScript(t *testing.T, script string) (map[string]interface{}, error) {
	t.Helper()
	m := NewModule()
	s := starlet.NewDefault()
	s.AddLazyloadModules(map[string]starlet.ModuleLoader{ModuleName: m.LoadModule()})
	return s.RunScript([]byte(script), nil)
}

// awsClientScript opens an AWS SQS client offline (static credentials, no live call)
// and prepends it to the given body as variable `c`.
func awsClientScript(body string) string {
	return `load("mq", "connect")
c = connect(service_type="aws_sqs", aws_region="us-east-1", aws_access_key="AKIATEST", aws_secret_key="secret")
` + body
}

// azureClientScript opens an Azure Service Bus client offline (the constructor only
// parses the connection string) and prepends it to the body as variable `c`.
func azureClientScript(body string) string {
	return `load("mq", "connect")
c = connect(service_type="azure_servicebus", connection_string="Endpoint=sb://demo.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v")
` + body
}

func requireScriptErrorContains(t *testing.T, script, want string) {
	t.Helper()
	_, err := runMQScript(t, script)
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error containing %q, got: %v", want, err)
	}
}

func requireScriptOK(t *testing.T, script string) map[string]interface{} {
	t.Helper()
	g, err := runMQScript(t, script)
	if err != nil {
		t.Fatalf("script failed unexpectedly: %v", err)
	}
	return g
}

// ---------------------------------------------------------------------------
// Error taxonomy
// ---------------------------------------------------------------------------

// TestNormalizeError verifies vendor error strings map to the stable taxonomy
// for each service, and that an existing MQError passes through unchanged.
func TestNormalizeError(t *testing.T) {
	if NormalizeError(ServiceTypeAWSSQS, "send", nil) != nil {
		t.Error("NormalizeError(..., nil) should return nil")
	}

	existing := NewMQError(ErrorTypeTimeout, ServiceTypeAWSSQS, "send", "boom", nil)
	if got := NormalizeError(ServiceTypeAWSSQS, "other", existing); got != error(existing) {
		t.Errorf("NormalizeError should pass an existing *MQError through unchanged, got %v", got)
	}

	tests := []struct {
		name     string
		service  string
		raw      string
		wantType ErrorType
	}{
		{"aws not found", ServiceTypeAWSSQS, "AWS.SimpleQueueService.QueueDoesNotExist", ErrorTypeNotFound},
		{"aws nonexistent", ServiceTypeAWSSQS, "NonExistentQueue: nope", ErrorTypeNotFound},
		{"aws exists", ServiceTypeAWSSQS, "QueueAlreadyExists", ErrorTypeAlreadyExists},
		{"aws receipt invalid", ServiceTypeAWSSQS, "ReceiptHandleIsInvalid", ErrorTypeNotFound},
		{"aws invalid param", ServiceTypeAWSSQS, "InvalidParameterValue", ErrorTypeValidation},
		{"aws access denied", ServiceTypeAWSSQS, "AccessDenied", ErrorTypePermission},
		{"aws throttled", ServiceTypeAWSSQS, "RequestThrottled", ErrorTypeThrottling},
		{"aws unavailable", ServiceTypeAWSSQS, "ServiceUnavailable", ErrorTypeService},
		{"aws timeout", ServiceTypeAWSSQS, "context deadline exceeded", ErrorTypeTimeout},
		{"aws unknown", ServiceTypeAWSSQS, "some random failure", ErrorTypeUnknown},
		{"azure not found", ServiceTypeAzureServiceBus, "MessagingEntityNotFound", ErrorTypeNotFound},
		{"azure exists", ServiceTypeAzureServiceBus, "MessagingEntityAlreadyExists", ErrorTypeAlreadyExists},
		{"azure msg not found", ServiceTypeAzureServiceBus, "LockTokenNotFound", ErrorTypeNotFound},
		{"azure too large", ServiceTypeAzureServiceBus, "MessageSizeExceeded", ErrorTypeValidation},
		{"azure unauthorized", ServiceTypeAzureServiceBus, "UnauthorizedAccessException", ErrorTypePermission},
		{"azure busy", ServiceTypeAzureServiceBus, "ServerBusyException", ErrorTypeThrottling},
		{"azure internal", ServiceTypeAzureServiceBus, "InternalServerError", ErrorTypeService},
		{"azure arg", ServiceTypeAzureServiceBus, "ArgumentException", ErrorTypeValidation},
		{"azure timeout", ServiceTypeAzureServiceBus, "operation timeout", ErrorTypeTimeout},
		{"azure unknown", ServiceTypeAzureServiceBus, "weird azure thing", ErrorTypeUnknown},
		{"unknown service", "rabbitmq", "anything", ErrorTypeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeError(tt.service, "op", errors.New(tt.raw))
			mqErr, ok := got.(*MQError)
			if !ok {
				t.Fatalf("NormalizeError returned %T, want *MQError", got)
			}
			if mqErr.Type != tt.wantType {
				t.Errorf("type = %q, want %q (raw=%q)", mqErr.Type, tt.wantType, tt.raw)
			}
			if mqErr.Operation != "op" {
				t.Errorf("operation = %q, want %q", mqErr.Operation, "op")
			}
			// The raw error must be wrapped (retained as the cause).
			if mqErr.Unwrap() == nil || mqErr.Unwrap().Error() != tt.raw {
				t.Errorf("Unwrap() = %v, want the raw error %q", mqErr.Unwrap(), tt.raw)
			}
		})
	}
}

// TestMQErrorString checks the formatted message with and without an underlying error.
func TestMQErrorString(t *testing.T) {
	withErr := NewMQError(ErrorTypeNotFound, "aws_sqs", "send", "Queue not found", errors.New("boom"))
	s := withErr.Error()
	for _, want := range []string{"Queue not found", "aws_sqs.send", "not_found", "boom"} {
		if !strings.Contains(s, want) {
			t.Errorf("Error() = %q, missing %q", s, want)
		}
	}

	noErr := NewMQError(ErrorTypeValidation, "azure_servicebus", "create_queue", "bad name", nil)
	s2 := noErr.Error()
	if strings.Contains(s2, "(") {
		t.Errorf("Error() without underlying err should not include parenthesised cause, got %q", s2)
	}
	if !strings.Contains(s2, "validation") || !strings.Contains(s2, "azure_servicebus.create_queue") {
		t.Errorf("Error() = %q, missing expected fields", s2)
	}

	if noErr.Unwrap() != nil {
		t.Error("Unwrap() should be nil when there is no underlying error")
	}
	if withErr.Unwrap() == nil {
		t.Error("Unwrap() should return the underlying error")
	}

	// WithContext is chainable and stores the value.
	withErr.WithContext("queue", "orders").WithContext("attempt", 3)
	if withErr.Context["queue"] != "orders" || withErr.Context["attempt"] != 3 {
		t.Errorf("WithContext did not store values: %v", withErr.Context)
	}
}

// TestMQErrorIs maps sentinel errors to MQError types via errors.Is.
func TestMQErrorIs(t *testing.T) {
	tests := []struct {
		errType  ErrorType
		sentinel error
	}{
		{ErrorTypeNotFound, ErrQueueNotFound},
		{ErrorTypeNotFound, ErrMessageNotFound},
		{ErrorTypeAlreadyExists, ErrQueueAlreadyExists},
		{ErrorTypeValidation, ErrMessageTooLarge},
		{ErrorTypeValidation, ErrInvalidParameter},
		{ErrorTypePermission, ErrAccessDenied},
		{ErrorTypeThrottling, ErrThrottled},
		{ErrorTypeService, ErrServiceUnavailable},
		{ErrorTypeUnsupported, ErrUnsupportedOperation},
		{ErrorTypeConnection, ErrConnectionFailed},
		{ErrorTypeTimeout, ErrTimeout},
	}
	for _, tt := range tests {
		e := NewMQError(tt.errType, "aws_sqs", "op", "msg", nil)
		if !errors.Is(e, tt.sentinel) {
			t.Errorf("errors.Is(%s, %v) = false, want true", tt.errType, tt.sentinel)
		}
	}

	// A non-matching sentinel returns false.
	e := NewMQError(ErrorTypeTimeout, "aws_sqs", "op", "msg", nil)
	if errors.Is(e, ErrQueueNotFound) {
		t.Error("a timeout error should not match ErrQueueNotFound")
	}
}

// TestErrorContainsHelper checks the case-insensitive substring matcher.
func TestErrorContainsHelper(t *testing.T) {
	tests := []struct {
		s        string
		keywords []string
		want     bool
	}{
		{"QueueDoesNotExist", []string{"queuedoesnotexist"}, true},
		{"queuedoesnotexist", []string{"QueueDoesNotExist"}, true},
		{"a Throttling event", []string{"throttling"}, true},
		{"", []string{"x"}, false},
		{"short", []string{"longerthanstring"}, false},
		{"none here", []string{"a", "b", "missing"}, false},
		{"prefixMatchAtEnd", []string{"AtEnd"}, true},
		{"exact", []string{"exact"}, true},
	}
	for _, tt := range tests {
		if got := contains(tt.s, tt.keywords...); got != tt.want {
			t.Errorf("contains(%q, %v) = %v, want %v", tt.s, tt.keywords, got, tt.want)
		}
	}
}

// TestIsRetryableError checks the transient-error classifier.
func TestIsRetryableError(t *testing.T) {
	retryable := []ErrorType{ErrorTypeThrottling, ErrorTypeService, ErrorTypeTimeout, ErrorTypeConnection}
	for _, et := range retryable {
		e := NewMQError(et, "aws_sqs", "op", "m", nil)
		if !IsRetryableError(e) {
			t.Errorf("IsRetryableError(%s) = false, want true", et)
		}
		if !IsTemporaryError(e) {
			t.Errorf("IsTemporaryError(%s) = false, want true", et)
		}
	}
	nonRetryable := []ErrorType{ErrorTypeNotFound, ErrorTypeValidation, ErrorTypePermission, ErrorTypeUnsupported, ErrorTypeUnknown}
	for _, et := range nonRetryable {
		e := NewMQError(et, "aws_sqs", "op", "m", nil)
		if IsRetryableError(e) {
			t.Errorf("IsRetryableError(%s) = true, want false", et)
		}
	}
	// A plain (non-MQError) error is not retryable.
	if IsRetryableError(errors.New("plain")) {
		t.Error("IsRetryableError(plain) = true, want false")
	}
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// validAWSConfig returns a ClientConfig that passes Validate for AWS SQS.
func validAWSConfig() *ClientConfig {
	return &ClientConfig{
		ServiceType:         ServiceTypeAWSSQS,
		AWSRegion:           "us-east-1",
		Timeout:             30,
		MaxRetries:          3,
		DefaultLockDuration: 30,
		DefaultBatchSize:    10,
	}
}

// TestClientConfigValidate exercises every validation branch.
func TestClientConfigValidate(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*ClientConfig)
		errSubstr string // "" means valid
	}{
		{"valid aws no creds", func(c *ClientConfig) {}, ""},
		{"valid aws with creds", func(c *ClientConfig) { c.AWSAccessKey = "A"; c.AWSSecretKey = "S" }, ""},
		{"valid aws with session token", func(c *ClientConfig) {
			c.AWSAccessKey = "A"
			c.AWSSecretKey = "S"
			c.AWSSessionToken = "T"
		}, ""},
		{"empty service type", func(c *ClientConfig) { c.ServiceType = "" }, "service_type is required"},
		{"unknown service type", func(c *ClientConfig) { c.ServiceType = "rabbitmq" }, "unsupported service type"},
		{"aws missing region", func(c *ClientConfig) { c.AWSRegion = "" }, "aws_region is required"},
		{"aws access without secret", func(c *ClientConfig) { c.AWSAccessKey = "A" }, "both aws_access_key and aws_secret_key"},
		{"aws secret without access", func(c *ClientConfig) { c.AWSSecretKey = "S" }, "both aws_access_key and aws_secret_key"},
		{"aws session token without creds", func(c *ClientConfig) { c.AWSSessionToken = "T" }, "aws_session_token can only be used"},
		{"negative timeout", func(c *ClientConfig) { c.Timeout = -1 }, "timeout must be non-negative"},
		{"negative max retries", func(c *ClientConfig) { c.MaxRetries = -1 }, "max_retries must be non-negative"},
		{"zero lock duration", func(c *ClientConfig) { c.DefaultLockDuration = 0 }, "default_lock_duration must be positive"},
		{"zero batch size", func(c *ClientConfig) { c.DefaultBatchSize = 0 }, "default_batch_size must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validAWSConfig()
			tt.mutate(c)
			err := c.Validate()
			if tt.errSubstr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tt.errSubstr)
			}
			if !strings.Contains(err.Error(), tt.errSubstr) {
				t.Errorf("Validate() error = %q, want substring %q", err, tt.errSubstr)
			}
		})
	}

	// Azure requires a connection string.
	az := &ClientConfig{ServiceType: ServiceTypeAzureServiceBus, DefaultLockDuration: 30, DefaultBatchSize: 10}
	if err := az.Validate(); err == nil || !strings.Contains(err.Error(), "connection_string is required") {
		t.Errorf("azure without conn string: got %v, want connection_string error", err)
	}
	az.ConnectionString = "Endpoint=sb://x/;k=v"
	if err := az.Validate(); err != nil {
		t.Errorf("azure with conn string: got %v, want nil", err)
	}
}

// TestClientConfigGetters checks the duration conversions and Copy independence.
func TestClientConfigGetters(t *testing.T) {
	c := &ClientConfig{Timeout: 45, DefaultLockDuration: 60}
	if got := c.GetTimeout(); got != 45*time.Second {
		t.Errorf("GetTimeout() = %v, want 45s", got)
	}
	if got := c.GetDefaultLockDuration(); got != 60*time.Second {
		t.Errorf("GetDefaultLockDuration() = %v, want 60s", got)
	}

	src := validAWSConfig()
	src.AWSAccessKey = "A"
	src.AWSSecretKey = "S"
	cp := src.Copy()
	if *cp != *src {
		t.Errorf("Copy() = %+v, want equal to %+v", cp, src)
	}
	cp.AWSAccessKey = "MUTATED"
	if src.AWSAccessKey == "MUTATED" {
		t.Error("Copy() did not produce an independent value")
	}
}

// TestDetectServiceType covers the auto-detection precedence.
func TestDetectServiceType(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ClientConfig
		want string
	}{
		{"connection string => azure", &ClientConfig{ConnectionString: "Endpoint=sb://x/"}, ServiceTypeAzureServiceBus},
		{"aws access key => aws", &ClientConfig{AWSAccessKey: "A"}, ServiceTypeAWSSQS},
		{"aws secret key => aws", &ClientConfig{AWSSecretKey: "S"}, ServiceTypeAWSSQS},
		{"aws region => aws", &ClientConfig{AWSRegion: "us-east-1"}, ServiceTypeAWSSQS},
		{"empty falls back to aws", &ClientConfig{}, ServiceTypeAWSSQS},
		{"conn string beats aws region", &ClientConfig{ConnectionString: "Endpoint=sb://x/", AWSRegion: "us-east-1"}, ServiceTypeAzureServiceBus},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := detectServiceType(tt.cfg); got != tt.want {
				t.Errorf("detectServiceType() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestGetConfigValue checks the override precedence for the generic helper.
func TestGetConfigValue(t *testing.T) {
	if got := getConfigValue("module", "override"); got != "override" {
		t.Errorf("getConfigValue(module, override) = %q, want override", got)
	}
	if got := getConfigValue("module", ""); got != "module" {
		t.Errorf("getConfigValue(module, zero) = %q, want module", got)
	}
	if got := getConfigValue(10, 0); got != 10 {
		t.Errorf("getConfigValue(10, 0) = %d, want 10", got)
	}
	if got := getConfigValue(10, 5); got != 5 {
		t.Errorf("getConfigValue(10, 5) = %d, want 5", got)
	}
}

// ---------------------------------------------------------------------------
// Result types (Queue / MessageResult)
// ---------------------------------------------------------------------------

// dictValue extracts a key from a Starlark dict value, failing on error.
func dictValue(t *testing.T, v starlark.Value, key string) starlark.Value {
	t.Helper()
	d, ok := v.(*starlark.Dict)
	if !ok {
		t.Fatalf("value is %T, want *starlark.Dict", v)
	}
	got, found, err := d.Get(starlark.String(key))
	if err != nil || !found {
		t.Fatalf("dict missing key %q (found=%v err=%v)", key, found, err)
	}
	return got
}

// TestQueueStruct checks the Go->Starlark marshalling of a Queue, including the
// nil and non-nil dead-letter / duplicate-detection sub-dicts and RFC3339 times.
func TestQueueStruct(t *testing.T) {
	created := time.Date(2026, 6, 15, 10, 0, 0, 0, time.UTC)
	q := &Queue{
		Name:         "orders",
		ServiceType:  ServiceTypeAWSSQS,
		URL:          "https://sqs/orders",
		MessageCount: 7,
		LockDuration: 30,
		MaxQueueSize: -1,
		CreatedTime:  created,
		ModifiedTime: created,
	}
	v, err := q.Struct()
	if err != nil {
		t.Fatalf("Struct() error = %v", err)
	}
	if got := dictValue(t, v, "name"); got != starlark.String("orders") {
		t.Errorf("name = %v, want orders", got)
	}
	if got := dictValue(t, v, "created_time"); got != starlark.String("2026-06-15T10:00:00Z") {
		t.Errorf("created_time = %v, want RFC3339", got)
	}
	if got := dictValue(t, v, "dead_letter_config"); got != starlark.None {
		t.Errorf("dead_letter_config = %v, want None", got)
	}
	if got := dictValue(t, v, "duplicate_detection"); got != starlark.None {
		t.Errorf("duplicate_detection = %v, want None", got)
	}

	// With nested configs present.
	q.DeadLetterConfig = &DeadLetterConfig{Enabled: true, QueueName: "orders-dlq", MaxDeliveryCount: 5}
	q.DuplicateDetection = &DuplicateDetection{Enabled: true, WindowSeconds: 600}
	v2, err := q.Struct()
	if err != nil {
		t.Fatalf("Struct() with configs error = %v", err)
	}
	dlq := dictValue(t, v2, "dead_letter_config")
	if got := dictValue(t, dlq, "queue_name"); got != starlark.String("orders-dlq") {
		t.Errorf("dlq queue_name = %v, want orders-dlq", got)
	}
	dup := dictValue(t, v2, "duplicate_detection")
	if got := dictValue(t, dup, "window_seconds"); got != starlark.MakeInt(600) {
		t.Errorf("dup window_seconds = %v, want 600", got)
	}
}

// TestQueueHelpers checks the boolean/duration helpers and the Copy round-trip.
func TestQueueHelpers(t *testing.T) {
	q := NewQueue("orders", ServiceTypeAzureServiceBus)
	if q.Name != "orders" || q.ServiceType != ServiceTypeAzureServiceBus {
		t.Errorf("NewQueue did not set identity fields: %+v", q)
	}
	if q.MaxQueueSize != -1 || q.LockDuration != 30 {
		t.Errorf("NewQueue defaults wrong: size=%d lock=%d", q.MaxQueueSize, q.LockDuration)
	}
	if q.IsDeadLetterEnabled() || q.IsDuplicateDetectionEnabled() {
		t.Error("fresh queue should not report DLQ/dedup enabled")
	}
	if got := q.GetLockDuration(); got != 30*time.Second {
		t.Errorf("GetLockDuration() = %v, want 30s", got)
	}
	if got := q.GetRetentionPeriod(); got != time.Duration(q.RetentionPeriod)*time.Second {
		t.Errorf("GetRetentionPeriod() = %v, mismatch", got)
	}

	q.DeadLetterConfig = &DeadLetterConfig{Enabled: true, QueueName: "dlq", MaxDeliveryCount: 3}
	q.DuplicateDetection = &DuplicateDetection{Enabled: true, WindowSeconds: 120}
	if !q.IsDeadLetterEnabled() || !q.IsDuplicateDetectionEnabled() {
		t.Error("expected DLQ/dedup enabled")
	}

	cp := q.Copy()
	if cp.DeadLetterConfig == q.DeadLetterConfig {
		t.Error("Copy() should deep-copy DeadLetterConfig")
	}
	if cp.DuplicateDetection == q.DuplicateDetection {
		t.Error("Copy() should deep-copy DuplicateDetection")
	}
	cp.DeadLetterConfig.QueueName = "changed"
	if q.DeadLetterConfig.QueueName == "changed" {
		t.Error("Copy() shares DeadLetterConfig with original")
	}

	// FromStarlark round-trips through Struct.
	v, err := q.Struct()
	if err != nil {
		t.Fatalf("Struct() error = %v", err)
	}
	var q2 Queue
	if err := q2.FromStarlark(v); err != nil {
		t.Fatalf("FromStarlark() error = %v", err)
	}
	if q2.Name != q.Name || q2.ServiceType != q.ServiceType {
		t.Errorf("FromStarlark round-trip mismatch: %+v vs %+v", q2, q)
	}
	if q2.DeadLetterConfig == nil || q2.DeadLetterConfig.QueueName != "dlq" {
		t.Errorf("FromStarlark did not restore DeadLetterConfig: %+v", q2.DeadLetterConfig)
	}

	// FromStarlark rejects a non-dict.
	if err := q2.FromStarlark(starlark.String("nope")); err == nil {
		t.Error("FromStarlark(string) = nil, want error")
	}
}

// TestMessageResultHelpers checks the lock/schedule time helpers and Struct fields.
func TestMessageResultHelpers(t *testing.T) {
	m := NewMessageResult("Star-1-abc", "hello")
	if m.MessageID != "Star-1-abc" || m.Body != "hello" || !m.Success {
		t.Errorf("NewMessageResult identity wrong: %+v", m)
	}
	// No lock / schedule set => helpers return zero/false.
	if m.IsExpired() || m.IsScheduled() {
		t.Error("fresh message should not be expired/scheduled")
	}
	if m.TimeUntilExpiry() != 0 || m.TimeUntilScheduled() != 0 {
		t.Error("durations should be 0 when times are nil")
	}

	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	m.LockExpiresAt = &past
	if !m.IsExpired() {
		t.Error("IsExpired() = false for a past lock, want true")
	}
	if m.TimeUntilExpiry() >= 0 {
		t.Error("TimeUntilExpiry() should be negative for a past lock")
	}
	m.LockExpiresAt = &future
	if m.IsExpired() {
		t.Error("IsExpired() = true for a future lock, want false")
	}

	m.ScheduledTime = &future
	if !m.IsScheduled() {
		t.Error("IsScheduled() = false for a future schedule, want true")
	}
	if m.TimeUntilScheduled() <= 0 {
		t.Error("TimeUntilScheduled() should be positive for a future schedule")
	}
	m.ScheduledTime = &past
	if m.IsScheduled() {
		t.Error("IsScheduled() = true for a past schedule, want false")
	}

	// Struct emits RFC3339 strings and None for unset optional times.
	plain := NewMessageResult("id", "body")
	v, err := plain.Struct()
	if err != nil {
		t.Fatalf("Struct() error = %v", err)
	}
	if got := dictValue(t, v, "scheduled_time"); got != starlark.None {
		t.Errorf("scheduled_time = %v, want None", got)
	}
	if got := dictValue(t, v, "lock_expires_at"); got != starlark.None {
		t.Errorf("lock_expires_at = %v, want None", got)
	}
	if got := dictValue(t, v, "success"); got != starlark.True {
		t.Errorf("success = %v, want True", got)
	}
}

// ---------------------------------------------------------------------------
// Backend pure converters
// ---------------------------------------------------------------------------

// TestExtractNamespace covers the Azure connection-string namespace parser.
func TestExtractNamespace(t *testing.T) {
	tests := []struct {
		conn string
		want string
	}{
		{"Endpoint=sb://demo.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v", "demo"},
		{"SharedAccessKeyName=k;Endpoint=sb://ns2.servicebus.windows.net/", "ns2"},
		{"Endpoint=sb://only.servicebus.windows.net/", "only"},
		{"no endpoint here", "unknown"},
		{"", "unknown"},
		{"Endpoint=sb://nodot/", "unknown"}, // no '.' after host => unknown
	}
	for _, tt := range tests {
		if got := extractNamespaceFromConnectionString(tt.conn); got != tt.want {
			t.Errorf("extractNamespaceFromConnectionString(%q) = %q, want %q", tt.conn, got, tt.want)
		}
	}
}

// TestParseDuration covers the ISO-8601 PT<n>S parser with fallback.
func TestParseDuration(t *testing.T) {
	tests := []struct {
		in       string
		fallback int
		want     int
	}{
		{"PT30S", 5, 30},
		{"PT0S", 5, 0},
		{"PT3600S", 5, 3600},
		{"", 42, 42},       // empty => fallback
		{"30", 42, 42},     // not PT..S => fallback
		{"PTxS", 42, 42},   // non-numeric => fallback
		{"P30S", 42, 42},   // missing T => fallback
		{"PT30", 42, 42},   // missing trailing S => fallback
		{"PT-15S", 7, -15}, // negative is parsed by Atoi
	}
	for _, tt := range tests {
		if got := parseDuration(tt.in, tt.fallback); got != tt.want {
			t.Errorf("parseDuration(%q, %d) = %d, want %d", tt.in, tt.fallback, got, tt.want)
		}
	}
}

// TestConvertToSQSMessageAttributes checks the SQS attribute shaping and nil pass-through.
func TestConvertToSQSMessageAttributes(t *testing.T) {
	if got := convertToSQSMessageAttributes(nil); got != nil {
		t.Errorf("convertToSQSMessageAttributes(nil) = %v, want nil", got)
	}
	attrs := convertToSQSMessageAttributes(map[string]interface{}{
		"a": "x",
		"b": 7,
	})
	if len(attrs) != 2 {
		t.Fatalf("expected 2 attributes, got %d", len(attrs))
	}
	a, ok := attrs["a"].(map[string]interface{})
	if !ok {
		t.Fatalf("attribute a is %T, want map", attrs["a"])
	}
	if a["DataType"] != "String" || a["StringValue"] != "x" {
		t.Errorf("attribute a = %v, want String/x", a)
	}
	b := attrs["b"].(map[string]interface{})
	if b["StringValue"] != "7" {
		t.Errorf("attribute b StringValue = %v, want 7", b["StringValue"])
	}
}

// TestServiceBusConverters checks the Azure option->property converters for
// duration formatting (PT<n>S) and conditional inclusion.
func TestServiceBusConverters(t *testing.T) {
	// Empty options => empty/minimal maps.
	props := convertToServiceBusProperties(QueueOptions{})
	if len(props) != 0 {
		t.Errorf("convertToServiceBusProperties(empty) = %v, want empty", props)
	}

	full := convertToServiceBusProperties(QueueOptions{
		LockDuration:        30,
		RetentionPeriod:     60,
		MaxDeliveryCount:    5,
		EnableSessions:      true,
		DuplicateDetection:  true,
		DuplicateWindowSecs: 120,
		MaxQueueSize:        10 * 1024 * 1024,
		DeadLetterConfig:    &DeadLetterConfig{Enabled: true},
	})
	if full["LockDuration"] != "PT30S" {
		t.Errorf("LockDuration = %v, want PT30S", full["LockDuration"])
	}
	if full["DefaultMessageTimeToLive"] != "PT60S" {
		t.Errorf("DefaultMessageTimeToLive = %v, want PT60S", full["DefaultMessageTimeToLive"])
	}
	if full["MaxDeliveryCount"] != 5 {
		t.Errorf("MaxDeliveryCount = %v, want 5", full["MaxDeliveryCount"])
	}
	if full["RequiresSession"] != true || full["RequiresDuplicateDetection"] != true {
		t.Errorf("session/dedup flags wrong: %v", full)
	}
	if full["DuplicateDetectionHistoryTimeWindow"] != "PT120S" {
		t.Errorf("dedup window = %v, want PT120S", full["DuplicateDetectionHistoryTimeWindow"])
	}
	if full["MaxSizeInMegabytes"] != int64(10) {
		t.Errorf("MaxSizeInMegabytes = %v, want 10", full["MaxSizeInMegabytes"])
	}
	if full["EnableDeadLetteringOnMessageExpiration"] != true {
		t.Errorf("DLQ flag missing: %v", full)
	}

	// Message converter: body always present, optional fields conditional.
	msg := convertToServiceBusMessage("hi", MessageOptions{})
	if msg["Body"] != "hi" {
		t.Errorf("Body = %v, want hi", msg["Body"])
	}
	if _, ok := msg["SessionId"]; ok {
		t.Error("SessionId should be omitted when empty")
	}
	msg2 := convertToServiceBusMessage("hi", MessageOptions{
		SessionID:     "s1",
		CorrelationID: "c1",
		ReplyTo:       "r1",
		MessageID:     "m1",
		TimeToLive:    90,
	})
	if msg2["SessionId"] != "s1" || msg2["CorrelationId"] != "c1" || msg2["ReplyTo"] != "r1" || msg2["MessageId"] != "m1" {
		t.Errorf("optional message fields wrong: %v", msg2)
	}
	if msg2["TimeToLive"] != "PT90S" {
		t.Errorf("TimeToLive = %v, want PT90S", msg2["TimeToLive"])
	}
}

// TestConvertToSQSAttributes checks the SQS queue-attribute builder (non-DLQ paths
// avoid the STS call that getAccountID would make).
func TestConvertToSQSAttributes(t *testing.T) {
	c := &AWSSQSClient{config: &ClientConfig{DefaultLockDuration: 45}, region: "us-east-1"}

	// Defaults: lock from config, retention default, no FIFO/dedup.
	attrs := c.convertToSQSAttributes("orders", QueueOptions{})
	if attrs["VisibilityTimeout"] != "45" {
		t.Errorf("VisibilityTimeout = %q, want 45 (from config default)", attrs["VisibilityTimeout"])
	}
	if attrs["MessageRetentionPeriod"] != "1209600" {
		t.Errorf("MessageRetentionPeriod = %q, want 1209600", attrs["MessageRetentionPeriod"])
	}
	if _, ok := attrs["FifoQueue"]; ok {
		t.Error("FifoQueue should be absent without sessions")
	}

	// Explicit lock + sessions(FIFO) + dedup.
	attrs2 := c.convertToSQSAttributes("orders", QueueOptions{
		LockDuration:       15,
		RetentionPeriod:    3600,
		EnableSessions:     true,
		DuplicateDetection: true,
	})
	if attrs2["VisibilityTimeout"] != "15" {
		t.Errorf("VisibilityTimeout = %q, want 15", attrs2["VisibilityTimeout"])
	}
	if attrs2["MessageRetentionPeriod"] != "3600" {
		t.Errorf("MessageRetentionPeriod = %q, want 3600", attrs2["MessageRetentionPeriod"])
	}
	if attrs2["FifoQueue"] != "true" {
		t.Errorf("FifoQueue = %q, want true", attrs2["FifoQueue"])
	}
	if attrs2["ContentBasedDeduplication"] != "true" {
		t.Errorf("ContentBasedDeduplication = %q, want true", attrs2["ContentBasedDeduplication"])
	}

	// Hard fallback to 30s when neither option nor config provides a lock duration.
	c2 := &AWSSQSClient{config: &ClientConfig{}, region: "us-east-1"}
	if got := c2.convertToSQSAttributes("q", QueueOptions{})["VisibilityTimeout"]; got != "30" {
		t.Errorf("VisibilityTimeout fallback = %q, want 30", got)
	}
}

// ---------------------------------------------------------------------------
// Wrapper object surface
// ---------------------------------------------------------------------------

// fakeClient is an in-memory Client used to exercise ClientWrapper without a network.
type fakeClient struct {
	info map[string]interface{}
}

func (f *fakeClient) CreateQueue(ctx context.Context, name string, options QueueOptions) (*Queue, error) {
	return nil, nil
}

// The remaining methods are never called by the wrapper-surface tests; they exist
// only to satisfy the Client interface.
func (f *fakeClient) DeleteQueue(ctx context.Context, name string) error { return nil }
func (f *fakeClient) ListQueues(ctx context.Context, prefix string) ([]*Queue, error) {
	return nil, nil
}
func (f *fakeClient) GetQueue(ctx context.Context, name string) (*Queue, error) { return nil, nil }
func (f *fakeClient) Exists(ctx context.Context, name string) (bool, error)     { return false, nil }
func (f *fakeClient) Purge(ctx context.Context, name string) error              { return nil }
func (f *fakeClient) GetInfo(ctx context.Context, name string) (*Queue, error)  { return nil, nil }
func (f *fakeClient) Send(ctx context.Context, q, b string, o MessageOptions) (*MessageResult, error) {
	return nil, nil
}
func (f *fakeClient) Receive(ctx context.Context, q string, o ReceiveOptions) ([]*MessageResult, error) {
	return nil, nil
}
func (f *fakeClient) Delete(ctx context.Context, q string, ids []string) ([]bool, error) {
	return nil, nil
}
func (f *fakeClient) Lock(ctx context.Context, q, id string, d int) error { return nil }
func (f *fakeClient) Unlock(ctx context.Context, q, id string) error      { return nil }
func (f *fakeClient) BatchSend(ctx context.Context, q string, m []BatchMessage) ([]*MessageResult, error) {
	return nil, nil
}
func (f *fakeClient) Schedule(ctx context.Context, q, b string, t time.Time, o MessageOptions) (*MessageResult, error) {
	return nil, nil
}
func (f *fakeClient) Cancel(ctx context.Context, q, id string) error { return nil }
func (f *fakeClient) Peek(ctx context.Context, q string, n int) ([]*MessageResult, error) {
	return nil, nil
}
func (f *fakeClient) DeadLetterReceive(ctx context.Context, q string, n int) ([]*MessageResult, error) {
	return nil, nil
}
func (f *fakeClient) DeadLetterRequeue(ctx context.Context, q, id string) error { return nil }
func (f *fakeClient) DeadLetterPurge(ctx context.Context, q string) error       { return nil }
func (f *fakeClient) Close() error                                              { return nil }
func (f *fakeClient) GetClientInfo() map[string]interface{}                     { return f.info }

// TestClientWrapperSurface checks the Starlark value protocol of ClientWrapper.
func TestClientWrapperSurface(t *testing.T) {
	cw := NewClientWrapper(&fakeClient{info: map[string]interface{}{"service_type": "aws_sqs"}})

	if cw.Type() != "mq.Client" {
		t.Errorf("Type() = %q, want mq.Client", cw.Type())
	}
	if cw.Truth() != starlark.True {
		t.Error("Truth() should be True")
	}
	if got := cw.String(); got != "<mq.Client service_type=aws_sqs>" {
		t.Errorf("String() = %q, want <mq.Client service_type=aws_sqs>", got)
	}
	cw.Freeze() // no-op, must not panic
	if _, err := cw.Hash(); err == nil {
		t.Error("Hash() should return an error (unhashable)")
	}

	// AttrNames covers every registered method, and each is resolvable via Attr.
	// The wrapper registers 20 methods (see client.go methodMap).
	names := cw.AttrNames()
	if len(names) != 20 {
		t.Errorf("AttrNames() count = %d, want 20", len(names))
	}
	for _, n := range names {
		v, err := cw.Attr(n)
		if err != nil {
			t.Errorf("Attr(%q) error = %v", n, err)
			continue
		}
		bi, ok := v.(*starlark.Builtin)
		if !ok {
			t.Errorf("Attr(%q) = %T, want *starlark.Builtin", n, v)
			continue
		}
		if want := "mq." + n; bi.Name() != want {
			t.Errorf("Attr(%q).Name() = %q, want %q", n, bi.Name(), want)
		}
	}

	// Unknown attribute is a no-such-attr error, not a panic.
	if _, err := cw.Attr("nope"); err == nil {
		t.Error("Attr(nope) = nil error, want NoSuchAttrError")
	}

	// A few load-bearing names must be present.
	for _, must := range []string{"send", "receive", "delete", "create_queue", "get_client_info"} {
		if _, err := cw.Attr(must); err != nil {
			t.Errorf("required method %q missing: %v", must, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Script-facing surface (offline)
// ---------------------------------------------------------------------------

// wantStr asserts a script global equals the given Go string.
func wantStr(t *testing.T, g map[string]interface{}, key, want string) {
	t.Helper()
	if got, ok := g[key].(string); !ok || got != want {
		t.Errorf("%s = %v (%T), want %q", key, g[key], g[key], want)
	}
}

// wantInt asserts a script global equals the given integer (RunScript yields int64).
func wantInt(t *testing.T, g map[string]interface{}, key string, want int64) {
	t.Helper()
	if got, ok := g[key].(int64); !ok || got != want {
		t.Errorf("%s = %v (%T), want %d", key, g[key], g[key], want)
	}
}

// wantBool asserts a script global equals the given boolean.
func wantBool(t *testing.T, g map[string]interface{}, key string, want bool) {
	t.Helper()
	if got, ok := g[key].(bool); !ok || got != want {
		t.Errorf("%s = %v (%T), want %v", key, g[key], g[key], want)
	}
}

// TestConnectScript verifies connect succeeds offline for both backends and that
// get_client_info reports the chosen service.
func TestConnectScript(t *testing.T) {
	g := requireScriptOK(t, awsClientScript(`info = c.get_client_info()
service = info["service_type"]
region = info["region"]`))
	wantStr(t, g, "service", "aws_sqs")
	wantStr(t, g, "region", "us-east-1")

	g2 := requireScriptOK(t, azureClientScript(`info = c.get_client_info()
service = info["service_type"]
ns = info["namespace"]`))
	wantStr(t, g2, "service", "azure_servicebus")
	wantStr(t, g2, "ns", "demo")

	// auto-detect: connection string => azure.
	g3 := requireScriptOK(t, `load("mq", "connect")
c = connect(connection_string="Endpoint=sb://auto.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v")
service = c.get_client_info()["service_type"]`)
	wantStr(t, g3, "service", "azure_servicebus")
}

// TestConnectValidationErrors checks connect's configuration error branches.
func TestConnectValidationErrors(t *testing.T) {
	tests := []struct {
		name   string
		script string
		want   string
	}{
		{
			"azure missing conn string",
			`load("mq","connect")
connect(service_type="azure_servicebus")`,
			"connection_string is required",
		},
		{
			"unsupported service",
			`load("mq","connect")
connect(service_type="rabbitmq")`,
			"unsupported service type",
		},
		{
			"negative timeout",
			awsClientScript(``) + `
connect(service_type="aws_sqs", aws_region="us-east-1", aws_access_key="A", aws_secret_key="B", timeout=-1)`,
			"timeout must be non-negative",
		},
		{
			"bad kwarg type",
			`load("mq","connect")
connect(timeout="not-an-int")`,
			"timeout",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireScriptErrorContains(t, tt.script, tt.want)
		})
	}
}

// TestBuiltinArgErrors checks the argument-validation branches of the client
// method builtins — all of which execute before any network call.
func TestBuiltinArgErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{"send properties not dict", `c.send(queue_name="q", body="x", properties="nope")`, "properties must be a dict"},
		{"send bad scheduled_time", `c.send(queue_name="q", body="x", scheduled_time="not-a-time")`, "invalid scheduled_time format"},
		{"schedule bad time", `c.schedule(queue_name="q", body="x", scheduled_time="bad")`, "invalid scheduled_time format"},
		{"schedule properties not dict", `c.schedule(queue_name="q", body="x", scheduled_time="2030-01-01T00:00:00Z", properties=5)`, "properties must be a dict"},
		{"batch_send not list", `c.batch_send("q", "nope")`, "messages must be a list"},
		{"batch_send element not dict", `c.batch_send("q", ["nope"])`, "each message must be a dict"},
		{"create_queue dlq not dict", `c.create_queue(name="q", dead_letter_config="nope")`, "dead_letter_config must be a dict"},
		{"delete bad ids type", `c.delete("q", 123)`, "message_ids must be string or list of strings"},
		{"delete list non-string", `c.delete("q", ["ok", 5])`, "message_ids must be string or list of strings"},
		{"missing required arg", `c.send(body="x")`, "send"},
		{"unknown method", `c.does_not_exist()`, "has no .does_not_exist attribute"},
		{"create_queue invalid name", `c.create_queue(name="bad--name")`, "consecutive hyphens"},
		{"create_queue empty name", `c.create_queue(name="")`, "cannot be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireScriptErrorContains(t, awsClientScript(tt.body), tt.want)
		})
	}
}

// TestOfflineStubMethods exercises the stub / pre-network branches that return
// without reaching the live service, asserting on their observable results.
func TestOfflineStubMethods(t *testing.T) {
	// AWS batch_send is a local mock: it echoes bodies back as MessageResults.
	g := requireScriptOK(t, awsClientScript(`
res = c.batch_send("q", [{"body":"a"},{"body":"b"}])
n = len(res)
first_body = res[0]["body"]
first_success = res[0]["success"]
first_id = res[0]["message_id"]`))
	wantInt(t, g, "n", 2)
	wantStr(t, g, "first_body", "a")
	wantBool(t, g, "first_success", true)
	if id, ok := g["first_id"].(string); !ok || !strings.HasPrefix(id, "Star-") {
		t.Errorf("batch_send first id = %v, want Star-* prefix", g["first_id"])
	}

	// AWS batch_send with an empty list returns an empty list (no network).
	gEmpty := requireScriptOK(t, awsClientScript(`res = len(c.batch_send("q", []))`))
	wantInt(t, gEmpty, "res", 0)

	// AWS lock/unlock are stubs that return True.
	gLock := requireScriptOK(t, awsClientScript(`locked = c.lock("q","id",30)
unlocked = c.unlock("q","id")`))
	wantBool(t, gLock, "locked", true)
	wantBool(t, gLock, "unlocked", true)

	// AWS cancel/peek are unsupported errors.
	requireScriptErrorContains(t, awsClientScript(`c.cancel("q","id")`), "not supported by AWS SQS")
	requireScriptErrorContains(t, awsClientScript(`c.peek("q",1)`), "not supported by AWS SQS")

	// Azure cancel is a stub returning True; peek a stub returning an empty list.
	gAz := requireScriptOK(t, azureClientScript(`cancelled = c.cancel("q","id")
peeked = len(c.peek("q",5))`))
	wantBool(t, gAz, "cancelled", true)
	wantInt(t, gAz, "peeked", 0)

	// Azure lock/unlock are unsupported (require the original message object).
	requireScriptErrorContains(t, azureClientScript(`c.lock("q","id",30)`), "requires original message object")
	requireScriptErrorContains(t, azureClientScript(`c.unlock("q","id")`), "requires original message object")
}

// TestModuleBuiltins checks the load-time module-level builtins.
func TestModuleBuiltins(t *testing.T) {
	g := requireScriptOK(t, `load("mq","get_supported_services")
svcs = get_supported_services()
n = len(svcs)
first = svcs[0]
second = svcs[1]`)
	wantInt(t, g, "n", 2)
	wantStr(t, g, "first", "aws_sqs")
	wantStr(t, g, "second", "azure_servicebus")

	// Module-level get_client_info on a real client matches the method result.
	g2 := requireScriptOK(t, `load("mq","connect","get_client_info")
c = connect(service_type="aws_sqs", aws_region="eu-west-1", aws_access_key="A", aws_secret_key="B")
via_builtin = get_client_info(c)["service_type"]
via_method = c.get_client_info()["service_type"]`)
	wantStr(t, g2, "via_builtin", "aws_sqs")
	wantStr(t, g2, "via_method", "aws_sqs")

	// get_client_info on a non-client value is a clean error, not a panic.
	requireScriptErrorContains(t, `load("mq","get_client_info")
get_client_info("not a client")`, "expected mq.Client")
	requireScriptErrorContains(t, `load("mq","get_client_info")
get_client_info(42)`, "expected mq.Client")
}

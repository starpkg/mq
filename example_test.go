package mq

import (
	"context"
	"testing"

	"github.com/1set/starlet"
	"github.com/starpkg/base"
)

// TestStarlarkScripts runs Starlark test scripts from the test directory.
// Scripts with "test-" prefix should succeed, "panic-" prefix should fail.
func TestStarlarkScripts(t *testing.T) {
	// Create a module factory function that returns a fresh module loader for each test
	moduleFactory := func() starlet.ModuleLoader {
		return NewModule().LoadModule()
	}
	extraModules := []string{"runtime", "go_idiomatic", "file", "json"}

	// Use the helper function from the base package
	base.RunStarlarkTests(t, ModuleName, moduleFactory, extraModules, "")
}

// TestModuleWithConfig tests module creation with configuration
func TestModuleWithConfig(t *testing.T) {
	m := NewModuleWithConfig(ServiceTypeAWSSQS, "", 60, 5)
	if m == nil {
		t.Fatal("NewModuleWithConfig() returned nil")
	}

	if val, err := m.ServiceType.GetValue(); err != nil || val != ServiceTypeAWSSQS {
		t.Errorf("Expected service type %s, got %s", ServiceTypeAWSSQS, val)
	}

	if val, err := m.Timeout.GetValue(); err != nil || val != 60 {
		t.Errorf("Expected timeout 60, got %d", val)
	}

	if val, err := m.MaxRetries.GetValue(); err != nil || val != 5 {
		t.Errorf("Expected max retries 5, got %d", val)
	}
}

// TestClientFactory tests the client factory
func TestClientFactory(t *testing.T) {
	factory := NewClientFactory()
	if factory == nil {
		t.Fatal("NewClientFactory() returned nil")
	}

	supportedServices := factory.GetSupportedServices()
	expectedServices := []string{ServiceTypeAWSSQS, ServiceTypeAzureServiceBus}

	if len(supportedServices) != len(expectedServices) {
		t.Errorf("Expected %d supported services, got %d", len(expectedServices), len(supportedServices))
	}

	for _, expected := range expectedServices {
		found := false
		for _, actual := range supportedServices {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected service %s not found in supported services", expected)
		}
	}
}

// TestErrorHandling tests error handling functionality
func TestErrorHandling(t *testing.T) {
	// Test MQError creation
	err := NewMQError("test_error", "This is a test error")
	if err == nil {
		t.Fatal("NewMQError() returned nil")
	}

	if err.Type != "test_error" {
		t.Errorf("Expected error type 'test_error', got '%s'", err.Type)
	}

	if err.Message != "This is a test error" {
		t.Errorf("Expected error message 'This is a test error', got '%s'", err.Message)
	}

	// Test error chaining
	err = err.WithServiceType("test_service").WithOperation("test_operation")
	if err.ServiceType != "test_service" {
		t.Errorf("Expected service type 'test_service', got '%s'", err.ServiceType)
	}

	if err.Operation != "test_operation" {
		t.Errorf("Expected operation 'test_operation', got '%s'", err.Operation)
	}

	// Test error string representation
	errStr := err.Error()
	expectedStr := "[test_service] This is a test error (operation: test_operation)"
	if errStr != expectedStr {
		t.Errorf("Expected error string '%s', got '%s'", expectedStr, errStr)
	}
}

// TestMessageStructs tests message data structures
func TestMessageStructs(t *testing.T) {
	// Test MessageResult
	msg := &MessageResult{
		MessageID: "test-message-id",
		Body:      "test body",
		Success:   true,
	}

	starlarkStruct := msg.Struct()
	if starlarkStruct == nil {
		t.Fatal("MessageResult.Struct() returned nil")
	}

	// Test Queue
	queue := &Queue{
		Name:        "test-queue",
		ServiceType: ServiceTypeAWSSQS,
	}

	queueStruct := queue.Struct()
	if queueStruct == nil {
		t.Fatal("Queue.Struct() returned nil")
	}

	// Test ClientInfo
	clientInfo := &ClientInfo{
		ServiceType: ServiceTypeAWSSQS,
		Connected:   true,
	}

	clientStruct := clientInfo.Struct()
	if clientStruct == nil {
		t.Fatal("ClientInfo.Struct() returned nil")
	}
}

// TestConfigurationOptions tests configuration options
func TestConfigurationOptions(t *testing.T) {
	m := NewModule()

	// Test default values
	if val, err := m.ServiceType.GetValue(); err != nil || val != ServiceTypeAuto {
		t.Errorf("Expected default service type %s, got %s", ServiceTypeAuto, val)
	}

	if val, err := m.Timeout.GetValue(); err != nil || val != 30 {
		t.Errorf("Expected default timeout 30, got %d", val)
	}

	if val, err := m.MaxRetries.GetValue(); err != nil || val != 3 {
		t.Errorf("Expected default max retries 3, got %d", val)
	}

	// Test setting values
	err := m.ServiceType.SetValue(ServiceTypeAWSSQS)
	if err != nil {
		t.Errorf("Failed to set service type: %v", err)
	}
	if val, err := m.ServiceType.GetValue(); err != nil || val != ServiceTypeAWSSQS {
		t.Errorf("Expected service type %s after setting, got %s", ServiceTypeAWSSQS, val)
	}

	err = m.Timeout.SetValue(60)
	if err != nil {
		t.Errorf("Failed to set timeout: %v", err)
	}
	if val, err := m.Timeout.GetValue(); err != nil || val != 60 {
		t.Errorf("Expected timeout 60 after setting, got %d", val)
	}
}

// TestStarlarkClient tests the Starlark client wrapper
func TestStarlarkClient(t *testing.T) {
	// Create a mock client implementation for testing
	mockClient := &MockClient{}
	starlarkClient := NewStarlarkClient(mockClient)

	if starlarkClient == nil {
		t.Fatal("NewStarlarkClient() returned nil")
	}

	if starlarkClient.Type() != "mq.Client" {
		t.Errorf("Expected type 'mq.Client', got '%s'", starlarkClient.Type())
	}

	if starlarkClient.Truth() != true {
		t.Error("Expected StarlarkClient.Truth() to return true")
	}

	// Test attribute access
	attrNames := starlarkClient.AttrNames()
	if len(attrNames) == 0 {
		t.Error("Expected StarlarkClient to have attributes")
	}

	// Test getting an attribute
	attr, err := starlarkClient.Attr("create_queue")
	if err != nil {
		t.Errorf("Failed to get 'create_queue' attribute: %v", err)
	}
	if attr == nil {
		t.Error("Expected 'create_queue' attribute to exist")
	}

	// Test getting a non-existent attribute
	attr, err = starlarkClient.Attr("non_existent_method")
	if err != nil {
		t.Errorf("Unexpected error for non-existent attribute: %v", err)
	}
	if attr != nil {
		t.Error("Expected non-existent attribute to return nil")
	}
}

// MockClient is a mock implementation of the Client interface for testing
type MockClient struct{}

func (m *MockClient) CreateQueue(ctx context.Context, name string, options map[string]interface{}) (*Queue, error) {
	return &Queue{Name: name, ServiceType: "mock"}, nil
}

func (m *MockClient) DeleteQueue(ctx context.Context, name string) error { return nil }
func (m *MockClient) ListQueues(ctx context.Context, prefix string) ([]*Queue, error) {
	return nil, nil
}
func (m *MockClient) GetQueue(ctx context.Context, name string) (*Queue, error)     { return nil, nil }
func (m *MockClient) QueueExists(ctx context.Context, name string) (bool, error)    { return false, nil }
func (m *MockClient) PurgeQueue(ctx context.Context, name string) error             { return nil }
func (m *MockClient) GetQueueInfo(ctx context.Context, name string) (*Queue, error) { return nil, nil }
func (m *MockClient) SendMessage(ctx context.Context, queueName, body string, options map[string]interface{}) (*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) ReceiveMessages(ctx context.Context, queueName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) DeleteMessage(ctx context.Context, queueName, messageID string) error {
	return nil
}
func (m *MockClient) DeleteMessages(ctx context.Context, queueName string, messageIDs []string) ([]bool, error) {
	return nil, nil
}
func (m *MockClient) ExtendMessageLock(ctx context.Context, queueName, messageID string, lockDuration int) error {
	return nil
}
func (m *MockClient) ReleaseMessageLock(ctx context.Context, queueName, messageID string) error {
	return nil
}
func (m *MockClient) SendMessagesBatch(ctx context.Context, queueName string, messages []map[string]interface{}) ([]*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) PeekMessages(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) SendScheduledMessage(ctx context.Context, queueName, body string, scheduledTime string, options map[string]interface{}) (*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) CancelScheduledMessage(ctx context.Context, queueName, messageID string) error {
	return nil
}
func (m *MockClient) GetDeadLetterMessages(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) ReprocessDeadLetterMessage(ctx context.Context, queueName, messageID string) error {
	return nil
}
func (m *MockClient) PurgeDeadLetterQueue(ctx context.Context, queueName string) error { return nil }
func (m *MockClient) CreateTopic(ctx context.Context, name string, options map[string]interface{}) (*Topic, error) {
	return nil, nil
}
func (m *MockClient) DeleteTopic(ctx context.Context, name string) error { return nil }
func (m *MockClient) ListTopics(ctx context.Context, prefix string) ([]*Topic, error) {
	return nil, nil
}
func (m *MockClient) TopicExists(ctx context.Context, name string) (bool, error) { return false, nil }
func (m *MockClient) CreateSubscription(ctx context.Context, topicName, subscriptionName string, options map[string]interface{}) (*Subscription, error) {
	return nil, nil
}
func (m *MockClient) DeleteSubscription(ctx context.Context, topicName, subscriptionName string) error {
	return nil
}
func (m *MockClient) ListSubscriptions(ctx context.Context, topicName string) ([]*Subscription, error) {
	return nil, nil
}
func (m *MockClient) PublishMessage(ctx context.Context, topicName, body string, options map[string]interface{}) (*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) SubscribeMessages(ctx context.Context, topicName, subscriptionName string, maxCount int, options map[string]interface{}) ([]*MessageResult, error) {
	return nil, nil
}
func (m *MockClient) AddSubscriptionFilter(ctx context.Context, topicName, subscriptionName, ruleName, filterExpression string) error {
	return nil
}
func (m *MockClient) RemoveSubscriptionFilter(ctx context.Context, topicName, subscriptionName, ruleName string) error {
	return nil
}
func (m *MockClient) ListSubscriptionFilters(ctx context.Context, topicName, subscriptionName string) ([]SubscriptionFilter, error) {
	return nil, nil
}
func (m *MockClient) Close() error                          { return nil }
func (m *MockClient) HealthCheck(ctx context.Context) error { return nil }
func (m *MockClient) GetClientInfo() *ClientInfo {
	return &ClientInfo{ServiceType: "mock", Connected: true}
}
func (m *MockClient) CheckFeatureSupport(feature string) bool { return false }

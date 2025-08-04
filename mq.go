// Package mq provides a Starlark module for unified message queue operations.
// It supports AWS SQS and Azure Service Bus with a consistent API.
package mq

import (
	"context"
	"fmt"
	"strings"

	"github.com/1set/starlet"
	"github.com/1set/starlet/dataconv"

	"github.com/starpkg/base"
	"go.starlark.net/starlark"
)

// ModuleName defines the expected name for this module when used in Starlark's load() function
const ModuleName = "mq"

var (
	none = starlark.None
)

// Module wraps the ConfigurableModule with specific functionality for MQ operations
type Module struct {
	cfgMod *base.ConfigurableModule
	ext    *base.ConfigurableModuleExt
}

// NewModule creates a new instance of Module with default configurations
func NewModule() *Module {
	return newModuleWithOptions(
		genConfigOption(configKeyServiceType, "Service type (aws_sqs, azure_servicebus, auto)", "auto"),
		genSecretConfigOption(configKeyConnectionString, "Azure Service Bus connection string", ""),
		genConfigOption(configKeyTimeout, "Connection timeout in seconds", 30),
		genConfigOption(configKeyMaxRetries, "Maximum retry attempts", 3),
		genConfigOption(configKeyAWSRegion, "AWS region for SQS", "us-east-1"),
		genSecretConfigOption(configKeyAWSAccessKey, "AWS access key ID", ""),
		genSecretConfigOption(configKeyAWSSecretKey, "AWS secret access key", ""),
		genSecretConfigOption(configKeyAWSSessionToken, "AWS session token", ""),
		genConfigOption(configKeyDefaultLockDuration, "Default message lock duration in seconds", 30),
		genConfigOption(configKeyDefaultBatchSize, "Default batch size for operations", 10),
	)
}

// Helper functions

// genConfigOption creates a configuration option with common settings
func genConfigOption[T any](name, description string, defaultValue T) *base.ConfigOption[T] {
	envVar := fmt.Sprintf("MQ_%s", strings.ToUpper(strings.ReplaceAll(name, "_", "_")))

	return base.NewConfigOption(defaultValue).
		WithName(name).
		WithDescription(description).
		WithEnvVar(envVar)
}

// genSecretConfigOption creates a secret configuration option
func genSecretConfigOption(name, description, defaultValue string) *base.ConfigOption[string] {
	envVar := fmt.Sprintf("MQ_%s", strings.ToUpper(strings.ReplaceAll(name, "_", "_")))

	return base.NewConfigOption(defaultValue).
		WithName(name).
		WithDescription(description).
		WithEnvVar(envVar).
		SetSecret(true)
}

// newModuleWithOptions creates a Module with the given configuration options
func newModuleWithOptions(
	serviceTypeOpt *base.ConfigOption[string],
	connectionStringOpt *base.ConfigOption[string],
	timeoutOpt *base.ConfigOption[int],
	maxRetriesOpt *base.ConfigOption[int],
	awsRegionOpt *base.ConfigOption[string],
	awsAccessKeyOpt *base.ConfigOption[string],
	awsSecretKeyOpt *base.ConfigOption[string],
	awsSessionTokenOpt *base.ConfigOption[string],
	defaultLockDurationOpt *base.ConfigOption[int],
	defaultBatchSizeOpt *base.ConfigOption[int],
) *Module {
	cm, _ := base.NewConfigurableModuleWithConfigOptions(
		serviceTypeOpt,
		connectionStringOpt,
		timeoutOpt,
		maxRetriesOpt,
		awsRegionOpt,
		awsAccessKeyOpt,
		awsSecretKeyOpt,
		awsSessionTokenOpt,
		defaultLockDurationOpt,
		defaultBatchSizeOpt,
	)

	return &Module{
		cfgMod: cm,
		ext:    cm.Extend(),
	}
}

// LoadModule returns the Starlark module loader with MQ-specific functions
func (m *Module) LoadModule() starlet.ModuleLoader {
	// Module functions
	additionalFuncs := starlark.StringDict{
		"connect":                starlark.NewBuiltin(ModuleName+".connect", m.starConnect),
		"get_supported_services": starlark.NewBuiltin(ModuleName+".get_supported_services", starGetSupportedServices),
		"get_client_info":        starlark.NewBuiltin(ModuleName+".get_client_info", starGetClientInfo),
	}
	return m.cfgMod.LoadModule(ModuleName, additionalFuncs)
}

// starConnect creates and returns a message queue client
func (m *Module) starConnect(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		serviceType      = ""
		connectionString = ""
		awsRegion        = ""
		awsAccessKey     = ""
		awsSecretKey     = ""
		timeout          = 0
		maxRetries       = 0
	)

	// Parse arguments - all optional
	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"service_type?", &serviceType,
		"connection_string?", &connectionString,
		"aws_region?", &awsRegion,
		"aws_access_key?", &awsAccessKey,
		"aws_secret_key?", &awsSecretKey,
		"timeout?", &timeout,
		"max_retries?", &maxRetries,
	); err != nil {
		return none, err
	}

	// Get configuration values from module, using provided values as overrides
	config := &ClientConfig{
		ServiceType:         getConfigValue(m.ext.GetString(configKeyServiceType), serviceType),
		ConnectionString:    getConfigValue(m.ext.GetString(configKeyConnectionString), connectionString),
		AWSRegion:           getConfigValue(m.ext.GetString(configKeyAWSRegion), awsRegion),
		AWSAccessKey:        getConfigValue(m.ext.GetString(configKeyAWSAccessKey), awsAccessKey),
		AWSSecretKey:        getConfigValue(m.ext.GetString(configKeyAWSSecretKey), awsSecretKey),
		AWSSessionToken:     m.ext.GetString(configKeyAWSSessionToken),
		Timeout:             getConfigValue(m.ext.GetInt(configKeyTimeout), timeout),
		MaxRetries:          getConfigValue(m.ext.GetInt(configKeyMaxRetries), maxRetries),
		DefaultLockDuration: m.ext.GetInt(configKeyDefaultLockDuration),
		DefaultBatchSize:    m.ext.GetInt(configKeyDefaultBatchSize),
	}

	// Auto-detect service type if needed
	if config.ServiceType == "auto" {
		config.ServiceType = detectServiceType(config)
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return none, fmt.Errorf("invalid configuration: %w", err)
	}

	// Get context from thread
	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	// Create client based on service type
	client, err := createClient(ctx, config)
	if err != nil {
		return none, fmt.Errorf("failed to create client: %w", err)
	}

	// Wrap the client for Starlark
	wrapper := NewClientWrapper(client)
	return wrapper, nil
}

// starGetSupportedServices returns a list of supported message queue services
func starGetSupportedServices(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 0); err != nil {
		return none, err
	}

	services := []string{ServiceTypeAWSSQS, ServiceTypeAzureServiceBus}
	return dataconv.Marshal(services)
}

// starGetClientInfo returns information about a client
func starGetClientInfo(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var clientVal starlark.Value

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &clientVal); err != nil {
		return none, err
	}

	// Check if it's a ClientWrapper
	wrapper, ok := clientVal.(*ClientWrapper)
	if !ok {
		return none, fmt.Errorf("expected mq.Client, got %s", clientVal.Type())
	}

	info := wrapper.client.GetClientInfo()
	return dataconv.Marshal(info)
}

// Helper function to get configuration value with override
func getConfigValue[T comparable](moduleValue T, override T) T {
	var zero T
	if override != zero {
		return override
	}
	return moduleValue
}

// detectServiceType attempts to detect the service type from configuration
func detectServiceType(config *ClientConfig) string {
	// If connection string is provided, assume Azure Service Bus
	if config.ConnectionString != "" {
		return ServiceTypeAzureServiceBus
	}

	// If AWS credentials or region is provided, assume AWS SQS
	if config.AWSAccessKey != "" || config.AWSSecretKey != "" || config.AWSRegion != "" {
		return ServiceTypeAWSSQS
	}

	// Default to AWS SQS
	return ServiceTypeAWSSQS
}

// validateConfig validates the client configuration
func validateConfig(config *ClientConfig) error {
	if config.ServiceType == "" {
		return fmt.Errorf("service_type is required")
	}

	switch config.ServiceType {
	case ServiceTypeAWSSQS:
		if config.AWSRegion == "" {
			return fmt.Errorf("aws_region is required for AWS SQS")
		}
	case ServiceTypeAzureServiceBus:
		if config.ConnectionString == "" {
			return fmt.Errorf("connection_string is required for Azure Service Bus")
		}
	default:
		return fmt.Errorf("unsupported service type: %s", config.ServiceType)
	}

	if config.Timeout < 0 {
		return fmt.Errorf("timeout must be non-negative")
	}

	if config.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be non-negative")
	}

	return nil
}

// createClient creates a client based on the configuration
func createClient(ctx context.Context, config *ClientConfig) (Client, error) {
	switch config.ServiceType {
	case ServiceTypeAWSSQS:
		return NewAWSSQSClient(ctx, config)
	case ServiceTypeAzureServiceBus:
		return NewAzureServiceBusClient(ctx, config)
	default:
		return nil, fmt.Errorf("unsupported service type: %s", config.ServiceType)
	}
}

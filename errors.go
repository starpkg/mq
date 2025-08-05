package mq

import (
	"errors"
	"fmt"
)

// Common error types for unified error handling across services
var (
	// ErrQueueNotFound indicates that the specified queue does not exist
	ErrQueueNotFound = errors.New("queue not found")

	// ErrQueueAlreadyExists indicates that the queue already exists
	ErrQueueAlreadyExists = errors.New("queue already exists")

	// ErrMessageNotFound indicates that the specified message does not exist
	ErrMessageNotFound = errors.New("message not found")

	// ErrMessageTooLarge indicates that the message exceeds size limits
	ErrMessageTooLarge = errors.New("message too large")

	// ErrAccessDenied indicates insufficient permissions
	ErrAccessDenied = errors.New("access denied")

	// ErrThrottled indicates that the request was throttled
	ErrThrottled = errors.New("request throttled")

	// ErrServiceUnavailable indicates that the service is temporarily unavailable
	ErrServiceUnavailable = errors.New("service unavailable")

	// ErrInvalidParameter indicates invalid parameter values
	ErrInvalidParameter = errors.New("invalid parameter")

	// ErrUnsupportedOperation indicates that the operation is not supported by the service
	ErrUnsupportedOperation = errors.New("unsupported operation")

	// ErrConnectionFailed indicates that connection to the service failed
	ErrConnectionFailed = errors.New("connection failed")

	// ErrTimeout indicates that the operation timed out
	ErrTimeout = errors.New("operation timed out")
)

// MQError represents a message queue operation error with additional context
type MQError struct {
	// Type categorizes the error
	Type ErrorType

	// Message provides a human-readable description
	Message string

	// Service indicates which service reported the error
	Service string

	// Operation indicates which operation failed
	Operation string

	// Underlying error from the service
	Err error

	// Additional context
	Context map[string]interface{}
}

// ErrorType categorizes different types of errors
type ErrorType string

const (
	ErrorTypeNotFound      ErrorType = "not_found"
	ErrorTypeAlreadyExists ErrorType = "already_exists"
	ErrorTypePermission    ErrorType = "permission"
	ErrorTypeThrottling    ErrorType = "throttling"
	ErrorTypeValidation    ErrorType = "validation"
	ErrorTypeConnection    ErrorType = "connection"
	ErrorTypeTimeout       ErrorType = "timeout"
	ErrorTypeService       ErrorType = "service"
	ErrorTypeUnsupported   ErrorType = "unsupported"
	ErrorTypeUnknown       ErrorType = "unknown"
)

// Error implements the error interface
func (e *MQError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s [%s.%s]: %s (%v)", e.Message, e.Service, e.Operation, e.Type, e.Err)
	}
	return fmt.Sprintf("%s [%s.%s]: %s", e.Message, e.Service, e.Operation, e.Type)
}

// Unwrap returns the underlying error
func (e *MQError) Unwrap() error {
	return e.Err
}

// Is checks if the error matches a target error
func (e *MQError) Is(target error) bool {
	switch target {
	case ErrQueueNotFound:
		return e.Type == ErrorTypeNotFound
	case ErrQueueAlreadyExists:
		return e.Type == ErrorTypeAlreadyExists
	case ErrMessageNotFound:
		return e.Type == ErrorTypeNotFound
	case ErrMessageTooLarge:
		return e.Type == ErrorTypeValidation
	case ErrAccessDenied:
		return e.Type == ErrorTypePermission
	case ErrThrottled:
		return e.Type == ErrorTypeThrottling
	case ErrServiceUnavailable:
		return e.Type == ErrorTypeService
	case ErrInvalidParameter:
		return e.Type == ErrorTypeValidation
	case ErrUnsupportedOperation:
		return e.Type == ErrorTypeUnsupported
	case ErrConnectionFailed:
		return e.Type == ErrorTypeConnection
	case ErrTimeout:
		return e.Type == ErrorTypeTimeout
	}
	return false
}

// NewMQError creates a new MQError
func NewMQError(errType ErrorType, service, operation, message string, err error) *MQError {
	return &MQError{
		Type:      errType,
		Service:   service,
		Operation: operation,
		Message:   message,
		Err:       err,
		Context:   make(map[string]interface{}),
	}
}

// WithContext adds context to the error
func (e *MQError) WithContext(key string, value interface{}) *MQError {
	e.Context[key] = value
	return e
}

// NormalizeError converts service-specific errors to unified MQError types
func NormalizeError(service, operation string, err error) error {
	if err == nil {
		return nil
	}

	// If it's already an MQError, return as-is
	if mqErr, ok := err.(*MQError); ok {
		return mqErr
	}

	// Service-specific error normalization
	switch service {
	case ServiceTypeAWSSQS:
		return normalizeAWSError(operation, err)
	case ServiceTypeAzureServiceBus:
		return normalizeAzureError(operation, err)
	default:
		return NewMQError(ErrorTypeUnknown, service, operation, err.Error(), err)
	}
}

// normalizeAWSError normalizes AWS SQS specific errors
func normalizeAWSError(operation string, err error) error {
	errStr := err.Error()

	// Map common AWS SQS errors to unified error types
	switch {
	case contains(errStr, "QueueDoesNotExist", "NonExistentQueue"):
		return NewMQError(ErrorTypeNotFound, ServiceTypeAWSSQS, operation, "Queue not found", err)
	case contains(errStr, "QueueAlreadyExists"):
		return NewMQError(ErrorTypeAlreadyExists, ServiceTypeAWSSQS, operation, "Queue already exists", err)
	case contains(errStr, "MessageNotInflight", "ReceiptHandleIsInvalid"):
		return NewMQError(ErrorTypeNotFound, ServiceTypeAWSSQS, operation, "Message not found", err)
	case contains(errStr, "InvalidParameterValue", "MalformedInput"):
		return NewMQError(ErrorTypeValidation, ServiceTypeAWSSQS, operation, "Invalid parameter", err)
	case contains(errStr, "AccessDenied", "Forbidden"):
		return NewMQError(ErrorTypePermission, ServiceTypeAWSSQS, operation, "Access denied", err)
	case contains(errStr, "RequestThrottled", "Throttling"):
		return NewMQError(ErrorTypeThrottling, ServiceTypeAWSSQS, operation, "Request throttled", err)
	case contains(errStr, "ServiceUnavailable", "InternalError"):
		return NewMQError(ErrorTypeService, ServiceTypeAWSSQS, operation, "Service unavailable", err)
	case contains(errStr, "timeout", "context deadline exceeded"):
		return NewMQError(ErrorTypeTimeout, ServiceTypeAWSSQS, operation, "Operation timed out", err)
	default:
		return NewMQError(ErrorTypeUnknown, ServiceTypeAWSSQS, operation, err.Error(), err)
	}
}

// normalizeAzureError normalizes Azure Service Bus specific errors
func normalizeAzureError(operation string, err error) error {
	errStr := err.Error()

	// Map common Azure Service Bus errors to unified error types
	switch {
	case contains(errStr, "MessagingEntityNotFound", "EntityNotFound"):
		return NewMQError(ErrorTypeNotFound, ServiceTypeAzureServiceBus, operation, "Queue not found", err)
	case contains(errStr, "MessagingEntityAlreadyExists", "EntityAlreadyExists"):
		return NewMQError(ErrorTypeAlreadyExists, ServiceTypeAzureServiceBus, operation, "Queue already exists", err)
	case contains(errStr, "MessageNotFound", "LockTokenNotFound"):
		return NewMQError(ErrorTypeNotFound, ServiceTypeAzureServiceBus, operation, "Message not found", err)
	case contains(errStr, "MessageSizeExceeded", "RequestEntityTooLarge"):
		return NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, operation, "Message too large", err)
	case contains(errStr, "UnauthorizedAccessException", "Forbidden"):
		return NewMQError(ErrorTypePermission, ServiceTypeAzureServiceBus, operation, "Access denied", err)
	case contains(errStr, "ServerBusyException", "ServiceBusy"):
		return NewMQError(ErrorTypeThrottling, ServiceTypeAzureServiceBus, operation, "Request throttled", err)
	case contains(errStr, "ServiceBusyException", "InternalServerError"):
		return NewMQError(ErrorTypeService, ServiceTypeAzureServiceBus, operation, "Service unavailable", err)
	case contains(errStr, "ArgumentException", "InvalidOperation"):
		return NewMQError(ErrorTypeValidation, ServiceTypeAzureServiceBus, operation, "Invalid parameter", err)
	case contains(errStr, "timeout", "context deadline exceeded"):
		return NewMQError(ErrorTypeTimeout, ServiceTypeAzureServiceBus, operation, "Operation timed out", err)
	default:
		return NewMQError(ErrorTypeUnknown, ServiceTypeAzureServiceBus, operation, err.Error(), err)
	}
}

// contains checks if any of the keywords are present in the string (case-insensitive)
func contains(s string, keywords ...string) bool {
	s = fmt.Sprintf("%s", s) // Ensure string format
	for _, keyword := range keywords {
		if len(s) >= len(keyword) {
			for i := 0; i <= len(s)-len(keyword); i++ {
				match := true
				for j := 0; j < len(keyword); j++ {
					if s[i+j] != keyword[j] && s[i+j] != keyword[j]+32 && s[i+j] != keyword[j]-32 {
						match = false
						break
					}
				}
				if match {
					return true
				}
			}
		}
	}
	return false
}

// IsRetryableError determines if an error is retryable
func IsRetryableError(err error) bool {
	if mqErr, ok := err.(*MQError); ok {
		switch mqErr.Type {
		case ErrorTypeThrottling, ErrorTypeService, ErrorTypeTimeout, ErrorTypeConnection:
			return true
		default:
			return false
		}
	}
	return false
}

// IsTemporaryError determines if an error is temporary
func IsTemporaryError(err error) bool {
	return IsRetryableError(err)
}

package mq

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// Common errors
	ErrQueueNotFound        = errors.New("queue not found")
	ErrTopicNotFound        = errors.New("topic not found")
	ErrSubscriptionNotFound = errors.New("subscription not found")
	ErrInvalidConfiguration = errors.New("invalid configuration")
	ErrServiceNotSupported  = errors.New("service not supported")
	ErrFeatureNotSupported  = errors.New("feature not supported for this service")
	ErrMessageTooLarge      = errors.New("message too large")
	ErrAccessDenied         = errors.New("access denied")
	ErrInvalidParameters    = errors.New("invalid parameters")
	ErrConnectionFailed     = errors.New("connection failed")
	ErrTimeout              = errors.New("operation timeout")
	ErrServiceUnavailable   = errors.New("service unavailable")
	ErrThrottled            = errors.New("request throttled")
)

// MQError represents a standardized error with additional context
type MQError struct {
	Type        string                 `json:"type"`
	Message     string                 `json:"message"`
	ServiceType string                 `json:"service_type,omitempty"`
	Operation   string                 `json:"operation,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
	Cause       error                  `json:"-"`
}

// Error implements the error interface
func (e *MQError) Error() string {
	msg := e.Message
	if e.ServiceType != "" {
		msg = fmt.Sprintf("[%s] %s", e.ServiceType, msg)
	}
	if e.Operation != "" {
		msg = fmt.Sprintf("%s (operation: %s)", msg, e.Operation)
	}
	return msg
}

// Unwrap implements error unwrapping
func (e *MQError) Unwrap() error {
	return e.Cause
}

// NewMQError creates a new MQError
func NewMQError(errorType, message string) *MQError {
	return &MQError{
		Type:    errorType,
		Message: message,
		Details: make(map[string]interface{}),
	}
}

// WithServiceType sets the service type
func (e *MQError) WithServiceType(serviceType string) *MQError {
	e.ServiceType = serviceType
	return e
}

// WithOperation sets the operation
func (e *MQError) WithOperation(operation string) *MQError {
	e.Operation = operation
	return e
}

// WithDetail adds a detail field
func (e *MQError) WithDetail(key string, value interface{}) *MQError {
	if e.Details == nil {
		e.Details = make(map[string]interface{})
	}
	e.Details[key] = value
	return e
}

// WithCause sets the underlying cause
func (e *MQError) WithCause(cause error) *MQError {
	e.Cause = cause
	return e
}

// NormalizeError converts service-specific errors to unified MQ errors
func NormalizeError(err error, serviceType, operation string) error {
	if err == nil {
		return nil
	}

	// If it's already an MQError, just add context
	if mqErr, ok := err.(*MQError); ok {
		if mqErr.ServiceType == "" {
			mqErr.ServiceType = serviceType
		}
		if mqErr.Operation == "" {
			mqErr.Operation = operation
		}
		return mqErr
	}

	errMsg := strings.ToLower(err.Error())

	switch {
	case strings.Contains(errMsg, "queue") && strings.Contains(errMsg, "not found"):
		return NewMQError("queue_not_found", "Queue not found").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "topic") && strings.Contains(errMsg, "not found"):
		return NewMQError("topic_not_found", "Topic not found").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "subscription") && strings.Contains(errMsg, "not found"):
		return NewMQError("subscription_not_found", "Subscription not found").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "access denied") || strings.Contains(errMsg, "unauthorized"):
		return NewMQError("access_denied", "Access denied").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "message too large") || strings.Contains(errMsg, "size exceeded"):
		return NewMQError("message_too_large", "Message too large").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "throttle") || strings.Contains(errMsg, "rate limit"):
		return NewMQError("throttled", "Request throttled").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "timeout"):
		return NewMQError("timeout", "Operation timeout").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "service unavailable") || strings.Contains(errMsg, "server busy"):
		return NewMQError("service_unavailable", "Service unavailable").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	case strings.Contains(errMsg, "invalid parameter") || strings.Contains(errMsg, "bad request"):
		return NewMQError("invalid_parameters", "Invalid parameters").
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)

	default:
		return NewMQError("unknown_error", err.Error()).
			WithServiceType(serviceType).
			WithOperation(operation).
			WithCause(err)
	}
}

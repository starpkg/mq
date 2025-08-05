package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/1set/starlet/dataconv"
	"go.starlark.net/starlark"
)

// Client interface defines the unified operations for message queue services
type Client interface {
	// Queue operations
	CreateQueue(ctx context.Context, name string, options QueueOptions) (*Queue, error)
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context, prefix string) ([]*Queue, error)
	GetQueue(ctx context.Context, name string) (*Queue, error)
	Exists(ctx context.Context, name string) (bool, error)
	Purge(ctx context.Context, name string) error
	GetInfo(ctx context.Context, name string) (*Queue, error)

	// Message operations
	Send(ctx context.Context, queueName, body string, options MessageOptions) (*MessageResult, error)
	Receive(ctx context.Context, queueName string, options ReceiveOptions) ([]*MessageResult, error)
	Delete(ctx context.Context, queueName string, messageIDs []string) ([]bool, error)

	// Message lock management
	Lock(ctx context.Context, queueName, messageID string, duration int) error
	Unlock(ctx context.Context, queueName, messageID string) error

	// Batch operations
	BatchSend(ctx context.Context, queueName string, messages []BatchMessage) ([]*MessageResult, error)

	// Specialized message operations
	Schedule(ctx context.Context, queueName, body string, scheduledTime time.Time, options MessageOptions) (*MessageResult, error)
	Cancel(ctx context.Context, queueName, messageID string) error
	Peek(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error)

	// Dead letter queue operations
	DeadLetterReceive(ctx context.Context, queueName string, maxCount int) ([]*MessageResult, error)
	DeadLetterRequeue(ctx context.Context, queueName, messageID string) error
	DeadLetterPurge(ctx context.Context, queueName string) error

	// Connection management
	Close() error
	GetClientInfo() map[string]interface{}
}

// ClientWrapper wraps the message queue client for Starlark
type ClientWrapper struct {
	client    Client
	methodMap map[string]func() starlark.Value
	allNames  []string
}

// Ensure ClientWrapper implements the required Starlark interfaces
var (
	_ starlark.Value    = (*ClientWrapper)(nil)
	_ starlark.HasAttrs = (*ClientWrapper)(nil)
)

// NewClientWrapper creates a new ClientWrapper with initialized method maps
func NewClientWrapper(client Client) *ClientWrapper {
	cw := &ClientWrapper{
		client: client,
	}
	fw := func(name string, sf dataconv.StarlarkFunc) func() starlark.Value {
		return func() starlark.Value {
			return starlark.NewBuiltin(ModuleName+"."+name, sf)
		}
	}

	// Initialize method map
	cw.methodMap = map[string]func() starlark.Value{
		// Client information
		"get_client_info": fw("get_client_info", cw.getClientInfo),

		// Queue operations
		"create_queue": fw("create_queue", cw.createQueue),
		"delete_queue": fw("delete_queue", cw.deleteQueue),
		"list_queues":  fw("list_queues", cw.listQueues),
		"get_queue":    fw("get_queue", cw.getQueue),
		"exists":       fw("exists", cw.exists),
		"purge":        fw("purge", cw.purge),
		"get_info":     fw("get_info", cw.getInfo),

		// Message operations
		"send":    fw("send", cw.send),
		"receive": fw("receive", cw.receive),
		"delete":  fw("delete", cw.delete),

		// Message lock management
		"lock":   fw("lock", cw.lock),
		"unlock": fw("unlock", cw.unlock),

		// Batch operations
		"batch_send": fw("batch_send", cw.batchSend),

		// Specialized message operations
		"schedule": fw("schedule", cw.schedule),
		"cancel":   fw("cancel", cw.cancel),
		"peek":     fw("peek", cw.peek),

		// Dead letter queue operations
		"dead_letter_receive": fw("dead_letter_receive", cw.deadLetterReceive),
		"dead_letter_requeue": fw("dead_letter_requeue", cw.deadLetterRequeue),
		"dead_letter_purge":   fw("dead_letter_purge", cw.deadLetterPurge),
	}

	// Collect all attribute names
	cw.allNames = make([]string, 0, len(cw.methodMap))
	for name := range cw.methodMap {
		cw.allNames = append(cw.allNames, name)
	}

	return cw
}

// Implement starlark.Value interface
func (cw *ClientWrapper) String() string {
	info := cw.client.GetClientInfo()
	serviceType, _ := info["service_type"].(string)
	return fmt.Sprintf("<mq.Client service_type=%s>", serviceType)
}

func (cw *ClientWrapper) Type() string {
	return "mq.Client"
}

func (cw *ClientWrapper) Freeze() {
	// Client is immutable after creation
}

func (cw *ClientWrapper) Truth() starlark.Bool {
	return starlark.True
}

func (cw *ClientWrapper) Hash() (uint32, error) {
	return 0, fmt.Errorf("unhashable type: %s", cw.Type())
}

// Implement starlark.HasAttrs interface
func (cw *ClientWrapper) Attr(name string) (starlark.Value, error) {
	// Check for methods using map lookup
	if methodFunc, exists := cw.methodMap[name]; exists {
		return methodFunc(), nil
	}

	return nil, starlark.NoSuchAttrError(fmt.Sprintf("%s has no .%s attribute", cw.Type(), name))
}

func (cw *ClientWrapper) AttrNames() []string {
	return cw.allNames
}

// getClientInfo returns client information
func (cw *ClientWrapper) getClientInfo(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 0); err != nil {
		return none, err
	}

	info := cw.client.GetClientInfo()
	return dataconv.Marshal(info)
}

// createQueue creates a new queue
func (cw *ClientWrapper) createQueue(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		name                               = ""
		lockDuration                       = 0
		retentionPeriod                    = 0
		maxDeliveryCount                   = 0
		deadLetterConfigVal starlark.Value = starlark.None
		enableSessions                     = false
		duplicateDetection                 = false
		duplicateWindowSecs                = 0
		maxQueueSize                       = int64(0)
	)

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"name", &name,
		"lock_duration?", &lockDuration,
		"retention_period?", &retentionPeriod,
		"max_delivery_count?", &maxDeliveryCount,
		"dead_letter_config?", &deadLetterConfigVal,
		"enable_sessions?", &enableSessions,
		"duplicate_detection?", &duplicateDetection,
		"duplicate_window_secs?", &duplicateWindowSecs,
		"max_queue_size?", &maxQueueSize,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	// Parse dead letter config
	var dlqConfig *DeadLetterConfig
	if deadLetterConfigVal != starlark.None {
		dlqDict, ok := deadLetterConfigVal.(*starlark.Dict)
		if !ok {
			return none, fmt.Errorf("dead_letter_config must be a dict")
		}

		dlqData, err := convertStarlarkDictToInterface(dlqDict)
		if err != nil {
			return none, fmt.Errorf("invalid dead_letter_config: %w", err)
		}

		dlqConfig = &DeadLetterConfig{}
		if enabled, ok := dlqData["enabled"].(bool); ok {
			dlqConfig.Enabled = enabled
		}
		if queueName, ok := dlqData["queue_name"].(string); ok {
			dlqConfig.QueueName = queueName
		}
		if maxDelCount, ok := dlqData["max_delivery_count"].(int); ok {
			dlqConfig.MaxDeliveryCount = maxDelCount
		}
	}

	options := QueueOptions{
		LockDuration:        lockDuration,
		RetentionPeriod:     retentionPeriod,
		MaxDeliveryCount:    maxDeliveryCount,
		DeadLetterConfig:    dlqConfig,
		EnableSessions:      enableSessions,
		DuplicateDetection:  duplicateDetection,
		DuplicateWindowSecs: duplicateWindowSecs,
		MaxQueueSize:        maxQueueSize,
	}

	queue, err := cw.client.CreateQueue(ctx, name, options)
	if err != nil {
		return none, NormalizeError("", "create_queue", err)
	}

	if queue == nil {
		return none, nil
	}

	return queue.Struct()
}

// deleteQueue deletes a queue
func (cw *ClientWrapper) deleteQueue(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var name = ""

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &name); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.DeleteQueue(ctx, name)
	if err != nil {
		return none, NormalizeError("", "delete_queue", err)
	}

	return starlark.Bool(true), nil
}

// listQueues lists all queues
func (cw *ClientWrapper) listQueues(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var prefix = ""

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"prefix?", &prefix,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	queues, err := cw.client.ListQueues(ctx, prefix)
	if err != nil {
		return none, NormalizeError("", "list_queues", err)
	}

	return queueSliceToStarlark(queues)
}

// getQueue gets queue information
func (cw *ClientWrapper) getQueue(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var name = ""

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &name); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	queue, err := cw.client.GetQueue(ctx, name)
	if err != nil {
		return none, NormalizeError("", "get_queue", err)
	}

	if queue == nil {
		return none, nil
	}

	return queue.Struct()
}

// exists checks if a queue exists
func (cw *ClientWrapper) exists(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var name = ""

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &name); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	exists, err := cw.client.Exists(ctx, name)
	if err != nil {
		return none, NormalizeError("", "exists", err)
	}

	return starlark.Bool(exists), nil
}

// purge purges all messages from a queue
func (cw *ClientWrapper) purge(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var name = ""

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &name); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.Purge(ctx, name)
	if err != nil {
		return none, NormalizeError("", "purge", err)
	}

	return starlark.Bool(true), nil
}

// getInfo gets detailed queue information
func (cw *ClientWrapper) getInfo(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var name = ""

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &name); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	queue, err := cw.client.GetInfo(ctx, name)
	if err != nil {
		return none, NormalizeError("", "get_info", err)
	}

	if queue == nil {
		return none, nil
	}

	return queue.Struct()
}

// Helper function to continue with the remaining methods...
// This file is getting quite long, so I'll continue with the remaining methods in the next part

// send sends a message to a queue
func (cw *ClientWrapper) send(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName                    = ""
		body                         = ""
		propertiesVal starlark.Value = starlark.None
		scheduledTime                = ""
		sessionID                    = ""
		correlationID                = ""
		replyTo                      = ""
		timeToLive                   = 0
		messageID                    = ""
	)

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"queue_name", &queueName,
		"body", &body,
		"properties?", &propertiesVal,
		"scheduled_time?", &scheduledTime,
		"session_id?", &sessionID,
		"correlation_id?", &correlationID,
		"reply_to?", &replyTo,
		"time_to_live?", &timeToLive,
		"message_id?", &messageID,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	// Parse properties
	var properties map[string]interface{}
	if propertiesVal != starlark.None {
		propDict, ok := propertiesVal.(*starlark.Dict)
		if !ok {
			return none, fmt.Errorf("properties must be a dict")
		}
		var err error
		properties, err = convertStarlarkDictToInterface(propDict)
		if err != nil {
			return none, fmt.Errorf("invalid properties: %w", err)
		}
	}

	// Parse scheduled time
	var scheduledTimePtr *time.Time
	if scheduledTime != "" {
		if t, err := time.Parse(time.RFC3339, scheduledTime); err == nil {
			scheduledTimePtr = &t
		} else {
			return none, fmt.Errorf("invalid scheduled_time format, expected RFC3339: %w", err)
		}
	}

	options := MessageOptions{
		Properties:    properties,
		ScheduledTime: scheduledTimePtr,
		SessionID:     sessionID,
		CorrelationID: correlationID,
		ReplyTo:       replyTo,
		TimeToLive:    timeToLive,
		MessageID:     messageID,
	}

	result, err := cw.client.Send(ctx, queueName, body, options)
	if err != nil {
		return none, NormalizeError("", "send", err)
	}

	return result.Struct()
}

// receive receives messages from a queue
func (cw *ClientWrapper) receive(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName    = ""
		maxCount     = 1
		waitTime     = 0
		lockDuration = 0
		peekOnly     = false
	)

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"queue_name", &queueName,
		"max_count?", &maxCount,
		"wait_time?", &waitTime,
		"lock_duration?", &lockDuration,
		"peek_only?", &peekOnly,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	options := ReceiveOptions{
		MaxCount: maxCount,
		WaitTime: waitTime,
		PeekOnly: peekOnly,
	}

	if lockDuration > 0 {
		options.LockDuration = &lockDuration
	}

	messages, err := cw.client.Receive(ctx, queueName, options)
	if err != nil {
		return none, NormalizeError("", "receive", err)
	}

	return messageResultSliceToStarlark(messages)
}

// delete deletes messages from a queue
func (cw *ClientWrapper) delete(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName  = ""
		messageIDs starlark.Value
	)

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 2, &queueName, &messageIDs); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	// Handle both single string and list of strings
	var ids []string
	switch v := messageIDs.(type) {
	case starlark.String:
		ids = []string{string(v)}
	case *starlark.List:
		for i := 0; i < v.Len(); i++ {
			if str, ok := v.Index(i).(starlark.String); ok {
				ids = append(ids, string(str))
			} else {
				return none, fmt.Errorf("message_ids must be string or list of strings")
			}
		}
	default:
		return none, fmt.Errorf("message_ids must be string or list of strings")
	}

	results, err := cw.client.Delete(ctx, queueName, ids)
	if err != nil {
		return none, NormalizeError("", "delete", err)
	}

	return boolSliceToStarlark(results), nil
}

// lock extends the lock duration of a message
func (cw *ClientWrapper) lock(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName    = ""
		messageID    = ""
		lockDuration = 0
	)

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 3, &queueName, &messageID, &lockDuration); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.Lock(ctx, queueName, messageID, lockDuration)
	if err != nil {
		return none, NormalizeError("", "lock", err)
	}

	return starlark.Bool(true), nil
}

// unlock releases the lock on a message
func (cw *ClientWrapper) unlock(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName = ""
		messageID = ""
	)

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 2, &queueName, &messageID); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.Unlock(ctx, queueName, messageID)
	if err != nil {
		return none, NormalizeError("", "unlock", err)
	}

	return starlark.Bool(true), nil
}

// batchSend sends multiple messages in a batch
func (cw *ClientWrapper) batchSend(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName = ""
		messages  starlark.Value
	)

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 2, &queueName, &messages); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	// Parse messages
	msgList, ok := messages.(*starlark.List)
	if !ok {
		return none, fmt.Errorf("messages must be a list")
	}

	batchMessages := make([]BatchMessage, msgList.Len())
	for i := 0; i < msgList.Len(); i++ {
		msgDict, ok := msgList.Index(i).(*starlark.Dict)
		if !ok {
			return none, fmt.Errorf("each message must be a dict")
		}

		msgData, err := convertStarlarkDictToInterface(msgDict)
		if err != nil {
			return none, fmt.Errorf("invalid message at index %d: %w", i, err)
		}

		bm := BatchMessage{}
		if body, ok := msgData["body"].(string); ok {
			bm.Body = body
		}
		if props, ok := msgData["properties"].(map[string]interface{}); ok {
			bm.Properties = props
		}
		if sessionID, ok := msgData["session_id"].(string); ok {
			bm.SessionID = sessionID
		}
		if correlationID, ok := msgData["correlation_id"].(string); ok {
			bm.CorrelationID = correlationID
		}
		if replyTo, ok := msgData["reply_to"].(string); ok {
			bm.ReplyTo = replyTo
		}
		if ttl, ok := msgData["time_to_live"].(int); ok {
			bm.TimeToLive = ttl
		}
		if msgID, ok := msgData["message_id"].(string); ok {
			bm.MessageID = msgID
		}

		batchMessages[i] = bm
	}

	results, err := cw.client.BatchSend(ctx, queueName, batchMessages)
	if err != nil {
		return none, NormalizeError("", "batch_send", err)
	}

	return messageResultSliceToStarlark(results)
}

// schedule schedules a message for future delivery
func (cw *ClientWrapper) schedule(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName                    = ""
		body                         = ""
		scheduledTime                = ""
		propertiesVal starlark.Value = starlark.None
		sessionID                    = ""
	)

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"queue_name", &queueName,
		"body", &body,
		"scheduled_time", &scheduledTime,
		"properties?", &propertiesVal,
		"session_id?", &sessionID,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	// Parse scheduled time
	t, err := time.Parse(time.RFC3339, scheduledTime)
	if err != nil {
		return none, fmt.Errorf("invalid scheduled_time format, expected RFC3339: %w", err)
	}

	// Parse properties
	var properties map[string]interface{}
	if propertiesVal != starlark.None {
		propDict, ok := propertiesVal.(*starlark.Dict)
		if !ok {
			return none, fmt.Errorf("properties must be a dict")
		}
		properties, err = convertStarlarkDictToInterface(propDict)
		if err != nil {
			return none, fmt.Errorf("invalid properties: %w", err)
		}
	}

	options := MessageOptions{
		Properties: properties,
		SessionID:  sessionID,
	}

	result, err := cw.client.Schedule(ctx, queueName, body, t, options)
	if err != nil {
		return none, NormalizeError("", "schedule", err)
	}

	return result.Struct()
}

// cancel cancels a scheduled message
func (cw *ClientWrapper) cancel(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName = ""
		messageID = ""
	)

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 2, &queueName, &messageID); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.Cancel(ctx, queueName, messageID)
	if err != nil {
		return none, NormalizeError("", "cancel", err)
	}

	return starlark.Bool(true), nil
}

// peek peeks at messages without receiving them
func (cw *ClientWrapper) peek(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName = ""
		maxCount  = 1
	)

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"queue_name", &queueName,
		"max_count?", &maxCount,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	messages, err := cw.client.Peek(ctx, queueName, maxCount)
	if err != nil {
		return none, NormalizeError("", "peek", err)
	}

	return messageResultSliceToStarlark(messages)
}

// deadLetterReceive receives messages from the dead letter queue
func (cw *ClientWrapper) deadLetterReceive(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName = ""
		maxCount  = 10
	)

	if err := starlark.UnpackArgs(b.Name(), args, kwargs,
		"queue_name", &queueName,
		"max_count?", &maxCount,
	); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	messages, err := cw.client.DeadLetterReceive(ctx, queueName, maxCount)
	if err != nil {
		return none, NormalizeError("", "dead_letter_receive", err)
	}

	return messageResultSliceToStarlark(messages)
}

// deadLetterRequeue moves a message back from dead letter queue to main queue
func (cw *ClientWrapper) deadLetterRequeue(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		queueName = ""
		messageID = ""
	)

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 2, &queueName, &messageID); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.DeadLetterRequeue(ctx, queueName, messageID)
	if err != nil {
		return none, NormalizeError("", "dead_letter_requeue", err)
	}

	return starlark.Bool(true), nil
}

// deadLetterPurge purges all messages from the dead letter queue
func (cw *ClientWrapper) deadLetterPurge(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var queueName = ""

	if err := starlark.UnpackPositionalArgs(b.Name(), args, kwargs, 1, &queueName); err != nil {
		return none, err
	}

	ctx := dataconv.GetThreadContext(thread)
	if ctx == nil {
		ctx = context.Background()
	}

	err := cw.client.DeadLetterPurge(ctx, queueName)
	if err != nil {
		return none, NormalizeError("", "dead_letter_purge", err)
	}

	return starlark.Bool(true), nil
}

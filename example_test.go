package mq

import (
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
	extraModules := []string{"go_idiomatic", "http", "json", "file", "path", "random", "time"}

	// Use the helper function from the base package
	base.RunStarlarkTests(t, ModuleName, moduleFactory, extraModules, "")
}

// TestAWSSQSStarlarkScripts runs AWS SQS specific Starlark test scripts.
func TestAWSSQSStarlarkScripts(t *testing.T) {
	// Create a module factory function that returns a fresh module loader for each test
	moduleFactory := func() starlet.ModuleLoader {
		return NewModule().LoadModule()
	}
	extraModules := []string{"go_idiomatic", "http", "json", "file", "path", "random", "time"}

	// Run AWS SQS specific tests in subdirectory
	base.RunStarlarkTests(t, ModuleName+"/aws-sqs", moduleFactory, extraModules, "")
}

// TestAzureServiceBusStarlarkScripts runs Azure Service Bus specific Starlark test scripts.
func TestAzureServiceBusStarlarkScripts(t *testing.T) {
	// Create a module factory function that returns a fresh module loader for each test
	moduleFactory := func() starlet.ModuleLoader {
		return NewModule().LoadModule()
	}
	extraModules := []string{"go_idiomatic", "http", "json", "file", "path", "random", "time"}

	// Run Azure Service Bus specific tests in subdirectory
	base.RunStarlarkTests(t, ModuleName+"/azure-servicebus", moduleFactory, extraModules, "")
}

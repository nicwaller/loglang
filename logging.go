package loglang

import (
	"context"
	"log/slog"
)

func ContextLogger(ctx context.Context) *slog.Logger {
	log := slog.Default()
	keys := []ContextKey{
		ContextKeyPipelineName,
		ContextKeyPluginType,
		ContextKeyPluginName,
	}
	for _, key := range keys {
		if value := ctx.Value(key); value != nil {
			log = log.With(string(key), value)
		}
	}
	return log
}

type ContextKey string

const (
	// ContextKeyPipelineName is the name of a pipeline
	ContextKeyPipelineName ContextKey = "pipelineName"

	// ContextKeyPluginType is the type of plugin (eg. "framing[lines]")
	ContextKeyPluginType ContextKey = "pluginType"

	// ContextKeyPluginName is the name of the plugin
	ContextKeyPluginName ContextKey = "pluginName"

	// ContextKeySchema is the schema used by a pipeline
	ContextKeySchema ContextKey = "schema"
)

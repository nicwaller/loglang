package loglang

type SchemaModel string

const (
	SchemaNotDefined   SchemaModel = ""
	SchemaFlat                     = "loglang/flat"
	SchemaECS                      = "loglang/ECS" // I don't agree with all Logstash choices
	SchemaLogstashFlat             = "logstash/legacy"
	SchemaLogstashECS              = "logstash/ecs"
)

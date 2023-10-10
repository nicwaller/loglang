package loglang

type SchemaModel string

const (
	SchemaNotDefined   SchemaModel = ""
	SchemaNone                     = "none" // when you really don't want any automatic fields
	SchemaFlat                     = "loglang/flat"
	SchemaECS                      = "loglang/ECS" // I don't agree with all Logstash choices
	SchemaLogstashFlat             = "logstash/legacy"
	SchemaLogstashECS              = "logstash/ecs"
)

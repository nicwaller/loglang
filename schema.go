package loglang

type SchemaModel string

const (
	SchemaNotDefined          SchemaModel = ""
	SchemaFlat                            = "flat"
	SchemaElasticCommonSchema             = "ECS"
)

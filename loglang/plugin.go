package loglang

import "io"

type InputPlugin struct {
	Name    string
	Type    string
	Run     func(chan Event) error
	Filters []FilterPlugin
}

type OutputPlugin struct {
	Name      string
	Run       func(Event) error
	Condition func(Event) bool
}

// TODO: PERF: maybe want to have number of goroutines specified as part of the plugin?
type FilterPlugin struct {
	Name string
	Run  func(Event, chan<- Event) error
}

// TODO: do I need separate StreamCodec and ChunkCodec?
type CodecPlugin struct {
	Name   string
	Encode func(Event) ([]byte, error)
	Decode func([]byte) (Event, error)
}

type FramingPlugin struct {
	Run func(io.Reader, chan []byte) error
}

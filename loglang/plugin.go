package loglang

type InputPlugin struct {
	Name string
	Type string
	Run  func(chan Event) error
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

package loglang

type InputPlugin struct {
	Name string
	Run  func(chan Event) error
}

type OutputPlugin struct {
	Name string
	Run  func(Event) error
}

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

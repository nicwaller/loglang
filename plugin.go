package loglang

import "io"

type InputPlugin interface {
	Run(chan Event) error
}

type OutputPlugin interface {
	Run(Event) error
}

type FilterPlugin func(Event, chan<- Event) error

type CodecPlugin interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
}

type FramingPlugin interface {
	Run(io.Reader, chan []byte) error
}

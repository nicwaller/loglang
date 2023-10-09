package loglang

import (
	"context"
	"io"
)

type InputPlugin interface {
	Run(context.Context, chan Event) error
}

type OutputPlugin interface {
	// TODO: Run() needs a better name or purpose
	Run(context.Context, Event) error
}

type FilterPlugin func(Event, chan<- Event) error

type CodecPlugin interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
}

type FramingPlugin interface {
	Run(context.Context, io.Reader, chan []byte) error
}

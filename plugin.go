package loglang

import (
	"context"
	"fmt"
	"io"
	"time"
)

type InputPlugin interface {
	Run(context.Context, BatchSender) error
}

type BatchSender func(...Event) BatchResult

type BatchResult struct {
	Total   int
	Dropped int
	Errors  int
	Success int
	Ok      bool
	Start   time.Time
	Finish  time.Time
}

func (r *BatchResult) Summary() string {
	return fmt.Sprintf("Ok=%b Total=%d Success=%d Dropped=%d Errors=%d",
		r.Ok, r.Total, r.Success, r.Dropped, r.Errors)
}

type OutputPlugin interface {
	// TODO: Run() needs a better name or purpose
	Run(context.Context, Event) error
}

type FilterPlugin func(event *Event, inject chan<- Event, drop func()) error

type CodecPlugin interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
}

type FramingPlugin interface {
	Run(context.Context, io.Reader, chan []byte) error
}

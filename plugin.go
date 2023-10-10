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
	TotalCount   int
	DropCount    int
	ErrorCount   int
	SuccessCount int
	Ok           bool
	Errors       []error
	Start        time.Time
	Finish       time.Time
}

func (r *BatchResult) Summary() string {
	return fmt.Sprintf("Ok=%t TotalCount=%d SuccessCount=%d DropCount=%d ErrorCount=%d",
		r.Ok, r.TotalCount, r.SuccessCount, r.DropCount, r.ErrorCount)
}

type OutputPlugin interface {
	// TODO: Run() needs a better Name or purpose
	Send(context.Context, []*Event) error
}

type FilterPlugin func(event *Event, inject chan<- Event, drop func()) error

type CodecPlugin interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
}

type FramingPlugin interface {
	Run(context.Context, io.Reader, chan []byte) error
}

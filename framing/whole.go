package framing

import (
	"context"
	"github.com/nicwaller/loglang"
)

//goland:noinspection GoUnusedExportedFunction
func Whole() loglang.FramingPlugin {
	return &whole{}
}

type whole struct{}

// Reads as much as possible and treats it as a single message
// It's the "no-op" of framing styles
// This is the default when no other framing is being used
// func (p *lines) Extract(ctx context.Context, reader io.Reader, out chan<- io.Reader) error {

func (p *whole) Extract(ctx context.Context, input <-chan []byte, output chan<- []byte) error {
loop:
	for {
		select {
		case frame := <-input:
			output <- frame

		case <-ctx.Done():
			break loop
		}
	}
	close(output)
	return nil
}

func (p *whole) Frameup(ctx context.Context, input <-chan []byte, output chan<- []byte) error {
loop:
	for {
		select {
		case frame := <-input:
			output <- frame

		case <-ctx.Done():
			break loop
		}
	}
	close(output)
	return nil
}

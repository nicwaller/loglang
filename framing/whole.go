package framing

import (
	"context"
	"fmt"
	"github.com/nicwaller/loglang"
	"io"
	"time"
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

func (p *whole) Extract(_ context.Context, streams <-chan io.Reader, out chan<- io.Reader) error {
	first := <-streams
	out <- first

	// check to see if we've been given too many packets on the channel
	select {
	case <-streams:
		return fmt.Errorf("whole() framing got multiple packets but expected only one")
	case <-time.After(time.Second):
		return nil
	}
}

func (p *whole) Frameup(_ context.Context, packets <-chan io.Reader, out io.Writer) error {
	first := <-packets
	_, err := io.Copy(out, first)
	if err != nil {
		return err
	}

	// check to see if we've been given too many packets on the channel
	select {
	case <-packets:
		return fmt.Errorf("whole() framing got multiple packets but expected only one")
	case <-time.After(time.Second):
		return nil
	}
}

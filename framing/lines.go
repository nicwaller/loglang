package framing

import (
	"bufio"
	"context"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Lines() loglang.FramingPlugin {
	return &lines{}
}

type lines struct{}

func (p *lines) Run(ctx context.Context, reader io.Reader, frames chan []byte) error {
	running := true
	go func() {
		<-ctx.Done()
		running = false
	}()

	scanner := bufio.NewScanner(reader)
	for running && scanner.Scan() {
		frames <- scanner.Bytes()
	}
	return nil
}

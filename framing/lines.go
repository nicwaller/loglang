package framing

import (
	"bufio"
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Lines() loglang.FramingPlugin {
	return &lines{}
}

type lines struct{}

func (p *lines) Run(reader io.Reader, frames chan []byte) error {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		frames <- scanner.Bytes()
	}
	return nil
}

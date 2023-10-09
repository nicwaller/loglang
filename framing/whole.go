package framing

import (
	"github.com/nicwaller/loglang"
	"io"
)

//goland:noinspection GoUnusedExportedFunction
func Whole() loglang.FramingPlugin {
	return &whole{}
}

type whole struct{}

// Reads as much as possible and treats it as a single message
// It's the "no-op" of framing styles
// This is the default when no other framing is being used
func (p *whole) Run(reader io.Reader, frames chan []byte) error {
	frame, err := io.ReadAll(reader)
	if err != nil {
		return err
	} else {
		frames <- frame
		return nil
	}
}

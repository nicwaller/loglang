package framing

import (
	"io"
	"loglang/loglang"
)

// Reads as much as possible and treats it as a single message
// It's the "no-op" of framing styles
// This is the default when no other framing is being used
func Whole() loglang.FramingPlugin {
	return loglang.FramingPlugin{
		Run: func(reader io.Reader, frames chan []byte) error {
			frame, err := io.ReadAll(reader)
			if err != nil {
				return err
			} else {
				frames <- frame
				return nil
			}
		},
	}
}

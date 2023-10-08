package framing

import (
	"bufio"
	"io"
	"loglang/loglang"
)

func Lines() loglang.FramingPlugin {
	return loglang.FramingPlugin{
		Run: func(reader io.Reader, frames chan []byte) error {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				frames <- scanner.Bytes()
			}
			return nil
		},
	}
}

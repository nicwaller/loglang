package output

import (
	"github.com/nicwaller/loglang"
	"os"
)

func StdOut(codec loglang.CodecPlugin) loglang.OutputPlugin {
	return loglang.OutputPlugin{
		Name: "",
		Run: func(evt loglang.Event) (err error) {
			dat, err := codec.Encode(evt)
			if err != nil {
				return err
			}
			_, err = os.Stdout.Write(dat)
			_, err = os.Stdout.Write([]byte("\n"))
			if err != nil {
				return err
			} else {
				return nil
			}
		},
	}
}

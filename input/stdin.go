package input

import (
	"context"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/framing"
	"os"
)

func Stdin() loglang.InputPlugin {
	return &stdInput{}
}

type stdInput struct {
	loglang.BaseInputPlugin
}

func (p *stdInput) Run(ctx context.Context, sender loglang.Sender) (err error) {
	// TODO: configure this elsewhere
	p.Codec = codec.Auto()
	//p.Framing = []loglang.FramingPlugin{framing.Auto()}
	// automatic framing is hard with stdin (peek how many bytes?)

	p.Framing = []loglang.FramingPlugin{framing.Lines()}
	sender.SetE2E(false)
	_, err = sender.SendRaw(ctx, p.eventTemplate(), os.Stdin)
	return
}

func (p *stdInput) eventTemplate() *loglang.Event {
	evt := loglang.NewEvent()

	switch p.Schema {
	case loglang.SchemaNone:
		// don't enrich with any automatic fields
	case loglang.SchemaLogstashFlat:
	case loglang.SchemaLogstashECS:
	case loglang.SchemaECS:
	case loglang.SchemaFlat:
		//evt.Field("transport").SetString("udp")
	}

	return &evt
}

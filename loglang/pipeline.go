package loglang

import (
	"fmt"
	"time"

	"log/slog"
)

func NewPipeline() Pipeline {
	var p Pipeline
	p.filters = []FilterPlugin{
		{
			Name: "populate @timestamp",
			Run: func(event Event, send chan<- Event) error {
				event.Field("@timestamp").Set(time.Now().Format(time.RFC3339))
				send <- event
				return nil
			},
		},
	}
	return p
}

type Pipeline struct {
	filters []FilterPlugin
}

// TODO: Inputs and Outputs should have codecs for converting between original and []byte

func (p *Pipeline) Run(inputs []InputPlugin, outputs []OutputPlugin) error {
	const ChanBufferSize = 2

	// all inputs are multiplexed to a single input channel
	inChan := make(chan Event, ChanBufferSize)
	// a single output channel does fan-out to all outputs
	outChan := make(chan Event, ChanBufferSize)

	// set up filters first, then outputs, then inputs LAST!

	slog.Debug("setting up filter channels")
	filterChannels := make([]chan Event, 0, len(p.filters))
	filterChannels = append(filterChannels, inChan)
	for i := 0; i < len(p.filters)-1; i++ {
		filterChannels = append(filterChannels, make(chan Event, ChanBufferSize))
	}
	filterChannels = append(filterChannels, outChan)

	slog.Debug("preparing filters")
	for i, f := range p.filters {
		filterIn := filterChannels[i]
		filterOut := filterChannels[i+1]
		filter := f
		go func() {
			for {
				select {
				case inEvt := <-filterIn:
					outEvt := inEvt.Copy()
					err := filter.Run(outEvt, filterOut)
					if err != nil {
						slog.Error(fmt.Sprintf("error from filter[%s]: %s", filter.Name, err.Error()))
					}
				case <-time.After(30 * time.Second):
					slog.Debug("no input for 30 seconds")
				}
			}
		}()
	}

	// goroutines to write outputs
	slog.Debug("preparing outputs")
	go func() {
		for {
			select {
			case outEvt := <-outChan:
				// TODO: PERF: run all outputs in parallel?
				for _, v := range outputs {
					err := v.Run(outEvt)
					if err != nil {
						slog.Error(fmt.Sprintf("output[%s] failed: %s", "?", err.Error()))
					}
				}
				break
			case <-time.After(30 * time.Second):
				slog.Debug("no output for 30 seconds")
				continue
			}
		}
	}()

	// start each input in a separate goroutine
	slog.Debug("preparing inputs")
	for _, v := range inputs {
		plugin := v
		go func() {
			err := plugin.Run(inChan)
			if err == nil {
				slog.Warn(fmt.Sprintf("input[%s] exited", plugin.Name))
			} else {
				slog.Error(fmt.Sprintf("input[%s] died: %s", plugin.Name, err))
			}
		}()
	}

	slog.Info("starting pipeline")
	for {
		time.Sleep(10 * time.Second)
		// TODO: wait for an OS signal or something?
	}
}

func (p *Pipeline) Add(f FilterPlugin) {
	p.filters = append(p.filters, f)
}

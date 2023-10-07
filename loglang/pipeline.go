package loglang

import (
	"fmt"
	"time"

	"log/slog"
)

func NewPipeline() Pipeline {
	var p Pipeline
	p.filters = make([]FilterPlugin, 0)
	return p
}

type Pipeline struct {
	filters []FilterPlugin
}

// TODO: Inputs and Outputs should have codecs for converting between original and []byte

func (p *Pipeline) Run(inputs []InputPlugin, outputs []OutputPlugin) error {
	const InBufferSize = 2
	const OutBufferSize = 2

	// all inputs are multiplexed to a single input channel
	inChan := make(chan Event, InBufferSize)
	// a single output channel does fan-out to all outputs
	outChan := make(chan Event, OutBufferSize)

	// set up filters first, then outputs, then inputs LAST!

	// goroutines to run filters
	slog.Debug("preparing filters")
	go func() {
		for {
			select {
			case inEvt := <-inChan:
				outEvt := inEvt.Copy()
				// TODO: PERF: run all filters in parallel
				for _, filter := range p.filters {
					tmpEvt, err := filter.Run(outEvt)
					if err == nil {
						outEvt = tmpEvt
					} else {
						slog.Error(fmt.Sprintf("error from filter[%s]: %s", filter.Name, err.Error()))
					}
				}
				outChan <- outEvt
				continue
			case <-time.After(30 * time.Second):
				slog.Debug("no input for 30 seconds")
				continue
			}
		}
	}()

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

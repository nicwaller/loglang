package loglang

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	"log/slog"
)

func NewPipeline() Pipeline {
	var p Pipeline
	p.filters = []FilterPlugin{
		{
			Name: "populate @timestamp",
			Run: func(event Event, send chan<- Event) error {
				event.Field("@timestamp").Default(time.Now().Format(time.RFC3339))
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
	combinedInputs := make(chan Event, ChanBufferSize)
	// a single output channel does fan-out to all outputs
	outChan := make(chan Event, ChanBufferSize)

	// set up filters first, then outputs, then inputs LAST!

	RunFilterChain(p.filters, combinedInputs, outChan)

	// goroutines to write outputs
	slog.Debug("preparing outputs")
	go func() {
		for {
			select {
			case outEvt := <-outChan:
				// TODO: PERF: run all outputs in parallel?
				for _, v := range outputs {
					if v.Condition == nil || v.Condition(outEvt) {
						err := v.Run(outEvt)
						if err != nil {
							slog.Error(fmt.Sprintf("output[%s] failed: %s", "?", err.Error()))
						}
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
		inChan := make(chan Event, ChanBufferSize)

		RunFilterChain(plugin.Filters, inChan, combinedInputs)

		// start pumping the input for real!
		go func() {
			err := plugin.Run(inChan)
			if err == nil {
				slog.Warn(fmt.Sprintf("input[%s] exited", plugin.Name))
			} else {
				slog.Error(fmt.Sprintf("input[%s] died: %s", plugin.Name, err))
			}
		}()
	}

	slog.Info("started pipeline")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	for _ = range c {
		slog.Info("Caught Ctrl-C SIGINT; exiting...")
		// TODO: pause inputs, then wait for all pipeline stages to finish.
		break
	}

	return nil
}

func (p *Pipeline) Add(f FilterPlugin) {
	p.filters = append(p.filters, f)
}

func RunFilterChain(filters []FilterPlugin, origin chan Event, destination chan Event) {
	// set up channels between each stage of the filter pipeline
	allChannels := make([]chan Event, 0)
	allChannels = append(allChannels, origin)
	for i := 0; i < len(filters); i++ {
		allChannels = append(allChannels, make(chan Event, 2))
	}

	// set up goroutines to pump each stage of the pipeline
	for i, f := range filters {
		filterIn := allChannels[i]
		filterOut := allChannels[i+1]
		filter := f // intermediate variable for goroutine
		//index := i  // intermediate variable for goroutine
		go func() {
			for {
				select {
				case inEvt := <-filterIn:
					outEvt := inEvt.Copy()
					err := filter.Run(outEvt, filterOut)
					if err != nil {
						slog.Error(fmt.Sprintf("error from filter[%s]: %s", filter.Name, err.Error()))
					}
				}
			}
		}()
	}
	go func() {
		for {
			select {
			case inEvt := <-allChannels[len(allChannels)-1]:
				destination <- inEvt
			case <-time.After(60 * time.Second):
				slog.Debug("no output from filter chain after 60 seconds")
			}
		}
	}()
}

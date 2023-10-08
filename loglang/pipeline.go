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
	combinedInputs := make(chan Event, ChanBufferSize)
	// a single output channel does fan-out to all outputs
	outChan := make(chan Event, ChanBufferSize)

	// set up filters first, then outputs, then inputs LAST!

	slog.Debug("setting up filter channels")
	filterChannels := make([]chan Event, 0, len(p.filters))
	filterChannels = append(filterChannels, combinedInputs)
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

		err := RunFilterChain(plugin.Filters, inChan, combinedInputs)
		if err != nil {
			slog.Error("entire filter chain failed")
			return err
		}

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

func RunFilterChain(filters []FilterPlugin, origin chan Event, destination chan Event) error {
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
		index := i  // intermediate variable for goroutine
		go func() {
			for {
				select {
				case inEvt := <-filterIn:
					outEvt := inEvt.Copy()
					slog.Debug(fmt.Sprintf("evt arrived input %s stage %d", filter.Name, index))
					err := filter.Run(outEvt, filterOut)
					if err != nil {
						slog.Error(fmt.Sprintf("error from filter[%s]: %s", filter.Name, err.Error()))
					}
				case <-time.After(5 * time.Second):
					slog.Debug("no input for 30 seconds redux")
					slog.Debug(fmt.Sprintf("no input from input %s filter stage %d for 30 seconds", filter.Name, index))
				}
			}
		}()
	}
	go func() {
		for {
			select {
			case inEvt := <-allChannels[len(allChannels)-1]:
				slog.Debug(fmt.Sprintf("evt arrived input end stage"))
				destination <- inEvt
			case <-time.After(5 * time.Second):
				slog.Debug("no input for 30 seconds redux redux")
				slog.Debug(fmt.Sprintf("no input from input filter stage for 30 seconds"))
			}
		}
	}()

	return nil
}

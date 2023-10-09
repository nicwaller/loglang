package loglang

import (
	"context"
	"fmt"
	"time"

	"log/slog"
)

func NewPipeline(name string, options PipelineOptions) *Pipeline {
	if options.Schema == SchemaNotDefined {
		options.Schema = SchemaECS
	}
	if options.StalledOutputThreshold == 0 {
		options.StalledOutputThreshold = 24 * time.Hour
	}
	if options.StalledInputThreshold == 0 {
		options.StalledInputThreshold = 24 * time.Hour
	}
	var p Pipeline
	p.opts = options
	p.Name = name

	p.Filter("default @timestamp", func(event *Event, inject chan<- Event, drop func()) error {
		event.Field("@timestamp").Default(time.Now().Format(time.RFC3339))
		return nil
	})

	if p.opts.MarkIngestionTime {
		f := Field{}

		if options.Schema == SchemaECS {
			f.Path = []string{"event", "ingested"}
		} else {
			f.Path = []string{"ingested"}
		}

		p.Filter("mark ingestion time", func(event *Event, events chan<- Event, drop func()) error {
			f.original = event
			f.Default(time.Now().Format(time.RFC3339))
			return nil
		})
	}

	return &p
}

type Pipeline struct {
	Name    string
	inputs  []NamedEntity[inputDetail]
	filters []NamedEntity[FilterPlugin]
	outputs []NamedEntity[OutputPlugin]
	opts    PipelineOptions
	stop    context.CancelFunc
}

type PipelineOptions struct {
	StalledInputThreshold  time.Duration
	StalledOutputThreshold time.Duration
	MarkIngestionTime      bool
	Schema                 SchemaModel
}

type inputDetail struct {
	plugin      InputPlugin
	filterChain []FilterPlugin
}

func (p *Pipeline) GetName() string {
	return p.Name
}

const ChanBufferSize = 2

// TODO: Inputs and Outputs should have codecs for converting between original and []byte

func (p *Pipeline) Run() error {
	ctx := context.Background()
	ctx, p.stop = context.WithCancel(ctx)
	ctx = context.WithValue(ctx, "pipeline", p.GetName())
	ctx = context.WithValue(ctx, "schema", p.opts.Schema)

	log := slog.Default()
	log = log.With("pipeline", p.GetName())

	// all inputs are multiplexed to a single input channel
	combinedInputs := make(chan Event, ChanBufferSize)
	// a single output channel does fan-out to all outputs
	outChan := make(chan Event, ChanBufferSize)

	// set up filters first, then outputs, then inputs LAST!

	log.Debug("preparing filter chain")
	filters := Map(func(t NamedEntity[FilterPlugin]) FilterPlugin {
		return t.value
	}, p.filters)
	RunFilterChain(ctx, filters, combinedInputs, outChan)
	log.Info(fmt.Sprintf("set up %d filters", len(p.filters)))

	if err := p.runOutputs(ctx, outChan); err != nil {
		return err
	}

	if err := p.runInputs(ctx, combinedInputs); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		log.Info("stopping pipeline")
	}

	return nil
}

func (p *Pipeline) Stop() {
	log := slog.Default()
	log = log.With("pipeline", p.GetName())

	log.Info("pipeline stop requested")
	p.stop()
}

func (p *Pipeline) runInputs(ctx context.Context, combinedInputs chan Event) error {
	log := slog.With("pipeline", ctx.Value("pipeline"))
	log.Debug("starting inputs")

	for _, entity := range p.inputs {
		plugin := entity.value.plugin

		log := slog.Default()
		log = log.With("pipeline", p.GetName())
		log = log.With("plugin", entity.name)

		inChan := make(chan Event, ChanBufferSize)

		RunFilterChain(ctx, entity.value.filterChain, inChan, combinedInputs)

		log.Info("starting input")
		go func() {
			err := plugin.Run(ctx, func(events ...Event) BatchResult {
				// TODO: prepare a batch
				for _, event := range events {
					inChan <- event
				}
				// TODO: wait for outputs to populate batch result
				return BatchResult{
					Dropped: 0,
					Errors:  0,
					Success: len(events),
					Ok:      true,
					Start:   time.Now(),
					Finish:  time.Now(),
				}
			})
			if err == nil {
				log.Info("input stopped")
			} else {
				log.Error("input failed", "error", err)
			}
		}()
	}
	return nil
}

func (p *Pipeline) runOutputs(ctx context.Context, events chan Event) error {
	log := slog.With("pipeline", ctx.Value("pipeline"))
	log.Debug("starting outputs")

	// set up an output channel for each output
	outputChannels := make([]chan Event, len(p.outputs))
	for i := 0; i < len(p.outputs); i++ {
		// TODO: PERF: we might want a larger buffer here
		// TODO: alert if length of output channel becomes large using len()
		outputChannels[i] = make(chan Event, 1)
	}

	// TODO: remove debugging to print channel sizes
	//go func() {
	//	for {
	//		fmt.Printf("len(events chan) = %d\n", len(events))
	//		for i := 0; i < len(p.outputs); i++ {
	//			fmt.Printf("len(outputChannels[%d]) = %d\n", i, len(outputChannels[i]))
	//		}
	//		time.Sleep(2 * time.Second)
	//	}
	//}()

	// set up fan-out replication of events
	alertThreshold := p.opts.StalledOutputThreshold
	go func() {
		log := slog.Default()
		log = log.With("pipeline", p.GetName())
	fanOut:
		for {
			select {
			case event := <-events:
				for i := 0; i < len(p.outputs); i++ {
					outputChannels[i] <- event
				}
			case <-time.After(alertThreshold):
				// TODO: make this customizable? PipelineOpts?
				log.Info("no output for 3 seconds")
				log.Info(fmt.Sprintf("len[chan0] == %d", len(outputChannels[0])))
			case <-ctx.Done():
				break fanOut
			}
		}
		log.Debug("halted fan-out")
	}()

	for i, namedOutput := range p.outputs {
		log := slog.Default()
		log = log.With("pipeline", p.GetName())
		log = log.With("plugin", namedOutput.name)

		output := namedOutput.value
		soloChan := outputChannels[i]

		log.Info("starting output")
		go func() {
		outputPump:
			for {
				select {
				case outEvt := <-soloChan:
					err := output.Run(ctx, outEvt)
					if err != nil {
						log.Error(fmt.Sprintf("output[%s] failed: %s", "?", err.Error()))
					}
				case <-ctx.Done():
					break outputPump
				}
			}
		}()
	}
	return nil
}

func (p *Pipeline) Input(name string, plugin InputPlugin, filters ...FilterPlugin) {
	p.inputs = append(p.inputs, NamedEntity[inputDetail]{
		name: name,
		value: inputDetail{
			plugin:      plugin,
			filterChain: filters,
		},
	})
}

func (p *Pipeline) Filter(name string, f FilterPlugin) {
	// TODO: this is where we associate a name with a filter
	p.filters = append(p.filters, NamedEntity[FilterPlugin]{
		name:  name,
		value: f,
	})
}

func (p *Pipeline) Output(name string, f OutputPlugin) {
	// TODO: this is where we associate a name with an output
	p.outputs = append(p.outputs, NamedEntity[OutputPlugin]{
		name:  name,
		value: f,
	})
}

func RunFilterChain(ctx context.Context, filters []FilterPlugin, origin chan Event, destination chan Event) {
	log := slog.Default()
	log = log.With("pipeline", ctx.Value("pipeline"))

	// set up channels between each stage of the filter Pipeline
	allChannels := make([]chan Event, 0)
	allChannels = append(allChannels, origin)
	for i := 0; i < len(filters); i++ {
		allChannels = append(allChannels, make(chan Event, 2))
	}

	// set up goroutines to pump each stage of the Pipeline
	for i, f := range filters {
		filterIn := allChannels[i]
		filterOut := allChannels[i+1]
		filter := f // intermediate variable for goroutine
		//index := i  // intermediate variable for goroutine
		go func() {
		filterPump:
			for {
				select {
				case event := <-filterIn:
					dropped := false
					dropFunc := func() {
						if dropped {
							log.Warn("drop() should only be called once")
						} else {
							dropped = true
						}
					}
					err := filter(&event, filterOut, dropFunc)
					if err != nil {
						// TODO: add Pipeline name
						// TODO: increment an error counter
						slog.Error(fmt.Sprintf("error from filter[%s]: %s", "?", err.Error()))
						filterOut <- event
					} else if dropped {
						// TODO: increment a drop counter
						// do not pass to next stage of filter pipeline
					} else {
						filterOut <- event
					}
				case <-ctx.Done():
					break filterPump
				}
			}
		}()
	}
	go func() {
	filterForwarder:
		for {
			select {
			case inEvt := <-allChannels[len(allChannels)-1]:
				destination <- inEvt
			case <-ctx.Done():
				log.Info("stopping filter chain")
				break filterForwarder
			}
		}
	}()
}

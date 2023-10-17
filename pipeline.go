package loglang

import (
	"context"
	"fmt"
	"sync"
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
	if options.SlowBatchWarning == 0 {
		options.SlowBatchWarning = 3 * time.Second
	}
	if options.BatchTimeout == 0 {
		options.BatchTimeout = time.Minute
	}
	var p Pipeline
	p.opts = options
	p.Name = name

	p.Filter("default @timestamp", func(event *Event, inject chan<- *Event, drop func()) error {
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

		p.Filter("mark ingestion time", func(event *Event, events chan<- *Event, drop func()) error {
			f.original = event
			f.Default(time.Now().Format(time.RFC3339))
			return nil
		})
	}

	return &p
}

type Pipeline struct {
	Name         string
	inputs       []NamedEntity[inputDetail]
	filters      []NamedEntity[FilterPlugin]
	outputs      []NamedEntity[OutputPlugin]
	opts         PipelineOptions
	stop         context.CancelCauseFunc
	OnCompletion func()
}

type PipelineOptions struct {
	StalledInputThreshold  time.Duration
	StalledOutputThreshold time.Duration
	MarkIngestionTime      bool
	Schema                 SchemaModel
	SlowBatchWarning       time.Duration
	BatchTimeout           time.Duration
}

type inputDetail struct {
	plugin      InputPlugin
	filterChain []NamedEntity[FilterPlugin]
}

func (p *Pipeline) GetName() string {
	return p.Name
}

const ChanBufferSize = 0

// TODO: Inputs and Outputs should have codecs for converting between original and []byte

func (p *Pipeline) Run() error {
	ctx := context.Background()
	ctx, p.stop = context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, ContextKeyPipelineName, p.GetName())
	ctx = context.WithValue(ctx, ContextKeySchema, p.opts.Schema)
	ctx = context.WithValue(ctx, ContextKeyPluginName, "pipeline")

	log := ContextLogger(ctx)
	log.Info("Starting Pipeline")

	// all inputs are multiplexed to a single input channel
	combinedInputs := make(chan *Event, ChanBufferSize)
	// a single output channel does fan-out to all outputs
	outChan := make(chan *Event, ChanBufferSize)

	// set up filters first, then outputs, then inputs LAST!

	log.Debug("preparing filter chain")
	RunFilterChain(ctx, p.filters, combinedInputs, outChan, true)
	log.Info(fmt.Sprintf("set up %d filters", len(p.filters)))

	if err := p.runOutputsBackground(ctx, outChan); err != nil {
		return err
	}

	if err := p.runInputsBackground(ctx, combinedInputs); err != nil {
		return err
	}

	<-ctx.Done()
	log.Info("Pipeline Finished", "cause", context.Cause(ctx))
	p.OnCompletion()
	return nil
}

func (p *Pipeline) Stop(reason string) {
	log := slog.Default()
	log = log.With(string(ContextKeyPipelineName), p.GetName())

	cause := fmt.Errorf("pipeline stop requested: %s", reason)
	p.stop(cause)
}

// TODO: should this function be runInput (singular) for readability?
func (p *Pipeline) runInputsBackground(pipelineContext context.Context, combinedInputs chan *Event) error {
	var allInputsComplete sync.WaitGroup

	if len(p.inputs) == 0 {
		return fmt.Errorf("no outputs configured")
	}

	for _, entity := range p.inputs {
		plugin := entity.Value.plugin
		pluginName := entity.Name

		allInputsComplete.Add(1)
		pluginContext := context.WithValue(pipelineContext, ContextKeyPluginName, pluginName)
		log := ContextLogger(pluginContext)

		inChan := make(chan *Event, ChanBufferSize)
		RunFilterChain(pluginContext, entity.Value.filterChain, inChan, combinedInputs, false)

		log.Info("starting input")
		go func() {
			sender := NewSender(pluginContext, inChan, plugin.Extract, len(p.outputs))
			err := plugin.Run(pluginContext, sender)
			if err != nil {
				log.Error("input failed", "error", err)
			} else {
				log.Info("input stopped", "cause", context.Cause(pluginContext))
			}
			allInputsComplete.Done()
		}()
	}
	go func() {
		allInputsComplete.Wait()
		p.stop(fmt.Errorf("all inputs complete"))
	}()
	return nil
}

func (p *Pipeline) runOutputsBackground(ctx context.Context, events chan *Event) error {
	log := slog.With(string(ContextKeyPipelineName), ctx.Value(ContextKeyPipelineName))
	log.Debug("starting outputs")

	if len(p.outputs) == 0 {
		return fmt.Errorf("no outputs configured")
	}

	// set up an output channel for each output
	outputChannels := make([]chan *Event, len(p.outputs))
	for i := 0; i < len(p.outputs); i++ {
		// TODO: PERF: we might want a larger buffer here
		// TODO: alert if length of output channel becomes large using len()
		outputChannels[i] = make(chan *Event, 1)
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
		log := ContextLogger(ctx)
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
	}()

	for i, namedOutput := range p.outputs {
		log := ContextLogger(ctx)

		output := namedOutput.Value
		outputName := namedOutput.Name
		soloChan := outputChannels[i]

		log.Info("starting output")
		go func() {
		outputPump:
			for {
				select {
				case outEvt, more := <-soloChan:
					if !more {
						log.Debug("outputPump saw closed channel")
						break outputPump
					}
					if outEvt == nil {
						log.Error("outputPump saw nil event")
						continue
					}
					// TODO: this is the opportunity to buffer and send several events at once
					err := output.Send(ctx, []*Event{outEvt})
					if err != nil {
						log.Error(fmt.Sprintf("output[%s] failed: %s", outputName, err.Error()))
						if outEvt.batch != nil {
							outEvt.batch.errorHappened <- err
						}
					} else {
						if outEvt.batch != nil {
							outEvt.batch.outputBurndown <- 1
						}
					}
				case <-ctx.Done():
					break outputPump
				}
			}
		}()
	}
	return nil
}

func (p *Pipeline) Input(name string, plugin InputPlugin, filters ...NamedEntity[FilterPlugin]) {
	p.inputs = append(p.inputs, NamedEntity[inputDetail]{
		Name: name,
		Value: inputDetail{
			plugin:      plugin,
			filterChain: filters,
		},
	})
}

func (p *Pipeline) Filter(name string, f FilterPlugin) {
	// TODO: this is where we associate a Name with a filter
	p.filters = append(p.filters, NamedEntity[FilterPlugin]{
		Name:  name,
		Value: f,
	})
}

func (p *Pipeline) Output(name string, f OutputPlugin) {
	// TODO: this is where we associate a Name with an output
	p.outputs = append(p.outputs, NamedEntity[OutputPlugin]{
		Name:  name,
		Value: f,
	})
}

func RunFilterChain(ctx context.Context, filters []NamedEntity[FilterPlugin], origin chan *Event, destination chan *Event, ack bool) {
	log := ContextLogger(ctx)

	// set up channels between each stage of the filter Pipeline
	allChannels := make([]chan *Event, 0)
	allChannels = append(allChannels, origin)
	for i := 0; i < len(filters); i++ {
		allChannels = append(allChannels, make(chan *Event, 2))
	}

	// set up goroutines to pump each stage of the Pipeline
	for i, entity := range filters {
		filterIn := allChannels[i]
		filterOut := allChannels[i+1]
		filterName := entity.Name
		filterFunc := entity.Value // intermediate variable for goroutine
		index := i                 // intermediate variable for goroutine
		go func() {
		filterPump:
			for {
				select {
				case event, more := <-filterIn:
					if !more {
						log.Debug("filterPump saw closed channel")
						close(filterOut)
						break filterPump
					}
					if event == nil {
						log.Error("filter chain received nil event; it's lost to the garbage collector",
							"index", index)
						continue
						// it's because events are getting garbage collected
					}
					dropped := false
					dropFunc := func() {
						if dropped {
							log.Warn("drop() should only be called once")
						} else {
							dropped = true
						}
					}
					err := filterFunc(event, filterOut, dropFunc)
					if err != nil {
						if event.batch != nil {
							event.batch.errorHappened <- err
						}
						log.Warn("filter error",
							"error", err,
							"filter", filterName,
						)
						// TODO: should events continue down the pipeline if a single filter stage fails?
						filterOut <- event
					} else if dropped {
						// do not pass to next stage of filter pipeline
						if ack && event.batch != nil {
							event.batch.dropHappened <- true
						}
					} else {
						if event.batch != nil {
							event.batch.filterBurndown <- 1
						}
						if event == nil {
							log.Debug("filterPump saw nil event")
						}
						filterOut <- event
					}
				case <-ctx.Done():
					close(filterOut)
					break filterPump
				}
			}
		}()
	}
	go func() {
	filterForwarder:
		for {
			select {
			case inEvt, more := <-allChannels[len(allChannels)-1]:
				if !more {
					log.Debug("filterForwarder saw closed channel")
					close(destination)
					break filterForwarder
				}
				if inEvt == nil {
					log.Error("filterForwarder saw nil event")
					continue
				}
				destination <- inEvt
			case <-ctx.Done():
				log.Info("stopping filter chain", "cause", context.Cause(ctx))
				close(destination)
				break filterForwarder
			}
		}
	}()
}

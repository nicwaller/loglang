package loglang

import (
	"context"
	"fmt"
	"sync"
	"time"
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

	p.ctx = context.Background()
	p.ctx, p.stop = context.WithCancelCause(p.ctx)
	p.ctx = context.WithValue(p.ctx, ContextKeyPipelineName, p.GetName())
	p.ctx = context.WithValue(p.ctx, ContextKeySchema, p.opts.Schema)
	p.ctx = context.WithValue(p.ctx, ContextKeyPluginName, "pipeline")

	p.Filter("default @timestamp", func(event *Event, inject chan<- *Event, drop func()) error {
		// PERF: maybe don't allocate a new field each time?
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

type NamedEntity[T any] struct {
	Value T
	Name  string
}

type Pipeline struct {
	Name    string // TODO: remove this
	inputs  []NamedEntity[inputDetail]
	filters []NamedEntity[FilterPlugin]
	outputs []NamedEntity[OutputConfig]
	opts    PipelineOptions
	ctx     context.Context
	stop    context.CancelCauseFunc
}

type OutputConfig struct {
	output  OutputPlugin
	framing FramingPlugin
	codec   CodecPlugin
}

type PipelineOptions struct {
	StalledInputThreshold  time.Duration
	StalledOutputThreshold time.Duration
	MarkIngestionTime      bool
	Schema                 SchemaModel
}

type inputDetail struct {
	plugin      InputPlugin
	filterChain []NamedEntity[FilterPlugin]
}

func (p *Pipeline) GetName() string {
	return p.Name
}

func (p *Pipeline) Run() error {
	if len(p.outputs) == 0 {
		return fmt.Errorf("no outputs configured")
	}
	if len(p.inputs) == 0 {
		return fmt.Errorf("no inputs configured")
	}
	if p.ctx == nil {
		return fmt.Errorf("pipeline missing context; make sure to use NewPipeline()")
	}

	log := ContextLogger(p.ctx)
	log.Info("Starting Pipeline")

	// all inputs are multiplexed to a single channel before going through filters
	preFilter := make(chan *Event)
	// a single output channel does fan-out to all outputs
	postFilter := make(chan *Event)

	go PumpFilterList(p.ctx, p.stop, preFilter, postFilter, p.filters, true)
	go p.runOutputs(postFilter)
	go p.runInputs(preFilter)

	<-p.ctx.Done()
	log.Info("Pipeline Finished", "cause", context.Cause(p.ctx))
	return nil
}

func (p *Pipeline) Stop(reason string) {
	cause := fmt.Errorf("pipeline stop requested: %s", reason)
	p.stop(cause)
}

func (p *Pipeline) runInputs(combinedInputs chan *Event) {
	var allInputsComplete sync.WaitGroup

	for _, entity := range p.inputs {
		pluginContext := context.WithValue(p.ctx, ContextKeyPluginName, entity.Name)
		go func() {
			p.runInput(pluginContext, p.stop, entity, combinedInputs)
			allInputsComplete.Done()
		}()
		allInputsComplete.Add(1)
	}

	allInputsComplete.Wait()
	p.stop(fmt.Errorf("all inputs complete"))
}

func (p *Pipeline) runInput(ctx context.Context, stop context.CancelCauseFunc, input NamedEntity[inputDetail], output chan *Event) {
	log := ContextLogger(ctx)
	log.Info("Starting Input")

	preFilter := make(chan *Event)
	postFilter := output

	go PumpFilterList(ctx, p.stop, preFilter, postFilter, input.Value.filterChain, false)

	sender := NewSender(ctx, preFilter, input.Value.plugin.Extract, len(p.outputs))
	err := input.Value.plugin.Run(ctx, sender)
	if err != nil {
		stop(fmt.Errorf("input failed: %w", err))
		log.Error("input failed", "error", err)
	} else {
		log.Info("input stopped", "cause", context.Cause(ctx))
	}
}

func (p *Pipeline) runOutputs(events chan *Event) {
	log := ContextLogger(p.ctx)
	log.Debug("starting outputs")

	// set up an output channel for each output
	outputChannels := make([]chan *Event, len(p.outputs))
	for i := 0; i < len(p.outputs); i++ {
		// TODO: PERF: we might want a larger buffer here
		// TODO: alert if length of output channel becomes large using len()
		outputChannels[i] = make(chan *Event, 1)
	}

	var countOutputs uint32 = uint32(len(p.outputs))

	for i, namedOutput := range p.outputs {
		pluginCtx := context.WithValue(p.ctx, ContextKeyPluginName, namedOutput.Name)
		// TODO: create a context for the output plugin with the plugin name
		log := ContextLogger(p.ctx)

		outputCfg := namedOutput.Value
		soloChan := outputChannels[i]

		log.Info("starting output")
		go PumpToFunction(pluginCtx, p.stop, soloChan, func(event *Event) error {
			// TODO: this is the opportunity to buffer and send several events at once
			err := outputCfg.output.Send(pluginCtx, []*Event{event}, outputCfg.codec, outputCfg.framing)
			// E2E handling
			if event.batch != nil {
				if err != nil {
					event.batch.errorHappened <- err
				}
				if countOutputs == event.finishedOutputs.Add(1) {
					event.batch.outputBurndown <- 1
				}
			}
			return err
		})
	}

	// set up fan-out replication of events
	// TODO: reinstate StalledOutputThreshold
	//alertThreshold := p.opts.StalledOutputThreshold
	PumpFanOut(p.ctx, p.stop, events, outputChannels)
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

// TODO: outputs should also have a filter chain
func (p *Pipeline) Output(name string, op OutputPlugin, cp CodecPlugin, fp FramingPlugin) {
	// TODO: this is where we associate a Name with an output
	p.outputs = append(p.outputs, NamedEntity[OutputConfig]{
		Name: name,
		Value: OutputConfig{
			output:  op,
			codec:   cp,
			framing: fp,
		},
	})
}

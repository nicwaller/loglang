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
	if options.SlowBatchWarning == 0 {
		options.SlowBatchWarning = 5 * time.Second
	}
	if options.BatchTimeout == 0 {
		options.BatchTimeout = time.Minute
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
	RunFilterChain(ctx, p.filters, combinedInputs, outChan, true)
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
		plugin := entity.Value.plugin
		pluginName := entity.Name

		log := slog.Default()
		log = log.With("pipeline", p.GetName())
		log = log.With("plugin", entity.Name)

		inChan := make(chan Event, ChanBufferSize)

		RunFilterChain(ctx, entity.Value.filterChain, inChan, combinedInputs, false)

		log.Info("starting input")
		go func() {
			pluginContext := context.WithValue(ctx, "plugin", pluginName)
			err := plugin.Run(pluginContext, func(events ...Event) BatchResult {
				fanout := len(events)
				pendingFilter := fanout
				pendingOutput := fanout * len(p.outputs)
				filterBurndown := make(chan bool)
				outputBurndown := make(chan bool)
				dropChan := make(chan bool)
				filterErrors := make(chan error)
				for _, event := range events {
					event.filterBurndown = filterBurndown
					event.outputBurndown = outputBurndown
					event.dropChan = dropChan
					event.filterErrors = filterErrors
					inChan <- event
				}
				done := make(chan bool)

				result := BatchResult{
					TotalCount:   fanout,
					DropCount:    0,
					ErrorCount:   0,
					SuccessCount: 0,
					Ok:           true,
					Errors:       make([]error, 0),
				}

				result.Start = time.Now()
				go func() {
					// TODO: customize this in pipeline options
					slowBatchWarning := time.After(p.opts.SlowBatchWarning)
					batchDeadline := time.After(p.opts.BatchTimeout)
					for pendingFilter > 0 || pendingOutput > 0 {
						select {
						case <-filterBurndown:
							pendingFilter -= 1
						case <-outputBurndown:
							pendingOutput -= 1
							// TODO: how do I increment success count? how do I know an event was published to all filters?
						case <-dropChan:
							pendingFilter -= 1
							pendingOutput -= fanout
							result.DropCount++
						case err := <-filterErrors:
							//result.Ok = false
							result.ErrorCount += 1
							result.Errors = append(result.Errors, err)
						case <-slowBatchWarning:
							log.Warn("batch is taking longer than expected")
						case <-batchDeadline:
							log.Error(fmt.Sprintf("batch timed out after %v", p.opts.BatchTimeout))
							result.Ok = false
							done <- true
							return
						}
					}
					done <- true
				}()

				// wait for batch to complete, either successfully or not
				select {
				case <-done:
					break
				}
				result.Finish = time.Now()

				return result
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
		log = log.With("plugin", namedOutput.Name)

		output := namedOutput.Value
		outputName := namedOutput.Name
		soloChan := outputChannels[i]

		log.Info("starting output")
		go func() {
		outputPump:
			for {
				select {
				case outEvt := <-soloChan:
					err := output.Run(ctx, outEvt)
					if err != nil {
						log.Error(fmt.Sprintf("output[%s] failed: %s", outputName, err.Error()))
						outEvt.filterErrors <- err
					} else {
						outEvt.outputBurndown <- true
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

func RunFilterChain(ctx context.Context, filters []NamedEntity[FilterPlugin], origin chan Event, destination chan Event, ack bool) {
	log := slog.Default()
	log = log.With("pipeline", ctx.Value("pipeline"))

	// set up channels between each stage of the filter Pipeline
	allChannels := make([]chan Event, 0)
	allChannels = append(allChannels, origin)
	for i := 0; i < len(filters); i++ {
		allChannels = append(allChannels, make(chan Event, 2))
	}

	// set up goroutines to pump each stage of the Pipeline
	for i, entity := range filters {
		filterIn := allChannels[i]
		filterOut := allChannels[i+1]
		filterName := entity.Name
		filterFunc := entity.Value // intermediate variable for goroutine
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
					err := filterFunc(&event, filterOut, dropFunc)
					if err != nil {
						event.filterErrors <- err
						log.Warn("filter error",
							"error", err,
							"filter", filterName,
						)
						// TODO: should events continue down the pipeline if a single filter stage fails?
						filterOut <- event
					} else if dropped {
						// do not pass to next stage of filter pipeline
						if ack {
							event.dropChan <- true
						}
					} else {
						event.filterBurndown <- true
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

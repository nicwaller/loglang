package main

import (
	"github.com/lmittmann/tint"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/input"
	"github.com/nicwaller/loglang/output"
	"log/slog"
	"os"
	"os/signal"
	"time"
)

func main() {
	setupLogging()
	slog.Info("Starting up")

	p := loglang.NewPipeline("demo", loglang.PipelineOptions{
		MarkIngestionTime: false,
		Schema:            loglang.SchemaFlat,
	})

	p.OnCompletion = func() {
		slog.Info("Goodbye")
		time.Sleep(time.Second)
	}

	//p.Input("heartbeat", input.Heartbeat(input.HeartbeatOptions{Interval: 1 * time.Second}))
	//p.Input("tcp/9998", input.NewTcpListener(9998, input.TcpListenerOptions{}))
	//p.Input("udp/9999", input.UdpListener(9999, input.UdpListenerOptions{
	//	Codec: codec.Json(),
	//}))

	// FIXME: http listener doesn't respond when using framing.Lines()
	//p.Input("http", input.HttpListener(2000, input.HttpListenerOptions{
	//	ReplyImmediately: false,
	//	//Codec:   codec.Json(),
	//	//Schema:  loglang.SchemaLogstashECS,
	//}))

	p.Input("stdin", input.Stdin())
	p.Output("stdout", output.StdOut(output.StdoutOptions{
		Codec: codec.Json(),
	}))

	//p.Output("slack", output.Slack(output.SlackOptions{
	//	BotToken:        os.Getenv("BOT_TOKEN"),
	//	FallbackChannel: "test-3",
	//	DetailFields:    true,
	//}))

	//p.Filter("delay", func(event *loglang.Event, inject chan<- loglang.Event, drop func()) error {
	//	time.Sleep(1400 * time.Millisecond)
	//	return nil
	//})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		_, _ = os.Stderr.WriteString("\n") // ^C appears in terminal, and I want a newline after that to keep things clean
		slog.Info("Caught Ctrl-C SIGINT")
		p.Stop("SIGINT")
	}()

	if err := p.Run(); err != nil {
		slog.Error(err.Error())
	}
	slog.Info("Exiting")
}

func setupLogging() {
	// TODO: can we automatically switch to debug output when certain errors occur?
	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level: slog.LevelDebug,
			//Level:      slog.LevelInfo,
			TimeFormat: time.Kitchen,
		}),
	))
}

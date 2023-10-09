package main

import (
	"github.com/lmittmann/tint"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/input"
	"github.com/nicwaller/loglang/output"
	"log/slog"
	"os"
	"time"
)

func main() {
	setupLogging()

	pipeline := loglang.NewPipeline()

	inputs := []loglang.InputPlugin{
		//input.Generator(input.GeneratorOptions{
		//	Interval: 5 * time.Second,
		//}),
		//input.UdpListener("udptest", "udp", 9999, framing.Whole(), codec.Kv()),
		//input.TcpListener("tcptest", "tcp", 9998, framing.Whole(), codec.Plain("message")),
		input.GelfUDP(9997),
	}

	//slackOut := output.Slack(output.SlackOptions{
	//	BotToken: os.Getenv("BOT_TOKEN"),
	//	Channel:  "test-3",
	//})
	//slackOut.Condition = func(event loglang.Event) bool {
	//	return event.Field("type").GetString() == "slack"
	//}

	outputs := []loglang.OutputPlugin{
		output.StdOut(codec.Kv()),
		//slackOut,
	}

	//pipeline.Add(loglang.FilterPlugin{
	//	Name: "noop",
	//	Run: func(event loglang.Event, send chan<- loglang.Event) error {
	//		// send the original event
	//		send <- event
	//		// sometimes inject another event for Slack
	//		//count := event.Field("count").GetInt()
	//		//if count%2 == 0 && count >= 2 {
	//		//	send <- loglang.Event{Fields: map[string]any{
	//		//		"type":    "slack",
	//		//		"message": fmt.Sprintf("Count (%d) is even", count),
	//		//	}}
	//		//}
	//		return nil
	//	},
	//})

	//pipeline.Add(filter.Json("test1", "message"))
	//pipeline.Add(filter.Rename("?", "msg", "message"))

	_ = pipeline.Run(inputs, outputs)
}

func setupLogging() {
	w := os.Stderr

	// create a new logger
	//logger := slog.New(tint.NewHandler(w, nil))

	// set global logger with custom options
	slog.SetDefault(slog.New(
		tint.NewHandler(w, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.Kitchen,
		}),
	))
}

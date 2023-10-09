package output

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/nicwaller/loglang"
	"io"
	"log/slog"
	"net/http"
	"time"
)

//goland:noinspection GoUnusedExportedFunction
func Slack(opts SlackOptions) loglang.OutputPlugin {
	return &slackOutput{
		opts: opts,
	}
}

type slackOutput struct {
	opts         SlackOptions
	sendFailures int
}

type SlackOptions struct {
	BotToken string
	Channel  string // TODO: fallback channel
}

func (p *slackOutput) Run(ctx context.Context, event loglang.Event) error {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
	)

	// TODO: validate token immediately
	// TODO: outputs should use context and process more than one event at a time

	if p.sendFailures > 3 {
		time.Sleep(time.Second)
		return fmt.Errorf("gave up posting to Slack after %d tries", p.sendFailures)
	}
	// HTTP endpoint
	posturl := "https://slack.com/api/chat.postMessage"

	// JSON body
	bb, err := json.Marshal(slackChatPostMessage{
		//Token:   opts.BotToken,
		Channel: loglang.Coalesce(p.opts.Channel, event.Field("channel").GetString()).(string),
		Text:    event.Field("message").GetString(),
	})

	// Create a HTTP post request
	req, err := http.NewRequest("POST", posturl, bytes.NewBuffer(bb))
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.opts.BotToken))
	req.Header.Add("Content-Type", "application/json") // essential for Slack
	if err != nil {
		p.sendFailures++
		log.Error("failed posting to slack", "error", err)
		return fmt.Errorf("failed posting to Slack: %w", err)
	}

	// Send it
	client := &http.Client{
		Timeout: time.Second * 5,
	}
	res, err := client.Do(req)
	if err != nil {
		p.sendFailures++
		log.Error("failed posting to slack", "error", err)
		return fmt.Errorf("failed posting to Slack: %w", err)
	}
	bbbb, _ := io.ReadAll(res.Body)

	var resp slackApiResponse
	err = json.Unmarshal(bbbb, &resp)
	if err != nil {
		p.sendFailures++
		return fmt.Errorf("that's weird... we failed to unmarshal response from Slack API")
	}

	if !resp.Ok {
		// TODO: validate Slack bot token much earlier
		log.Error("Slack API rejected our message. This is usually due to a missing or incorrect BOT_TOKEN.")
		log.Debug(resp.Warning)
		return fmt.Errorf("Slack API rejected our message. This is usually due to a missing or incorrect BOT_TOKEN.")
	}

	return nil
}

type slackChatPostMessage struct {
	Channel string `json:"channel"`
	Text    string `json:"text"`
}

type slackApiResponse struct {
	Ok        bool   `json:"ok"`
	ChannelId string `json:"channel"`
	Timestamp string `json:"ts"`
	Warning   string `json:"warning"`
}

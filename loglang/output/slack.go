package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"loglang/loglang"
	"net/http"
	"time"
)

func Slack(opts SlackOptions) loglang.OutputPlugin {
	failedSend := 0
	return loglang.OutputPlugin{
		Name: "slack",
		Run: func(evt loglang.Event) (err error) {
			if failedSend > 3 {
				return fmt.Errorf("gave up posting to Slack after %d tries", failedSend)
			}
			// HTTP endpoint
			posturl := "https://slack.com/api/chat.postMessage"

			// JSON body
			bb, err := json.Marshal(slackChatPostMessage{
				//Token:   opts.BotToken,
				Channel: loglang.Coalesce(opts.Channel, evt.Field("channel").GetString()).(string),
				Text:    evt.Field("message").GetString(),
			})

			fmt.Println(string(bb))

			// Create a HTTP post request
			req, err := http.NewRequest("POST", posturl, bytes.NewBuffer(bb))
			req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", opts.BotToken))
			req.Header.Add("Content-Type", "application/json") // essential for Slack
			if err != nil {
				failedSend++
				return fmt.Errorf("failed posting to Slack: %w", err)
			}

			// Send it
			client := &http.Client{
				Timeout: time.Second * 5,
			}
			res, err := client.Do(req)
			if err != nil {
				failedSend++
				return fmt.Errorf("failed posting to Slack: %w", err)
			}
			bbbb, _ := io.ReadAll(res.Body)

			var resp slackApiResponse
			err = json.Unmarshal(bbbb, &resp)
			if err != nil {
				failedSend++
				return fmt.Errorf("that's weird... we failed to unmarshal response from Slack API")
			}

			if !resp.Ok {
				// TODO: validate Slack bot token much earlier
				slog.Debug(resp.Warning)
				return fmt.Errorf("Slack API rejected our message. This is usually due to a missing or incorrect BOT_TOKEN.")
			}

			return nil
		},
	}
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

type SlackOptions struct {
	BotToken string
	Channel  string
}

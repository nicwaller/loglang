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
	"os"
	"regexp"
	"strings"
	"time"
)

//goland:noinspection GoUnusedExportedFunction
func Slack(opts SlackOptions) loglang.OutputPlugin {
	fatal := func(message string) {
		slog.Error(message)
		// pause for 2 seconds to avoid a very hot crash-restart loop
		time.Sleep(time.Second * 2)
		os.Exit(1)

	}
	if opts.ApiRoot == "" {
		opts.ApiRoot = "https://slack.com/api/"
	}
	if opts.BotToken == "" {
		fatal("missing Slack bot token")
	}
	botTokenPattern := regexp.MustCompile(`xoxb-[0-9]{11,13}-[0-9]{11,13}-[0-9a-zA-Z]{24}`)
	if !botTokenPattern.MatchString(opts.BotToken) {
		fatal("provided Slack token does not match expected xoxb- pattern")
	}
	p := slackOutput{
		opts: opts,
	}
	if p.canAuthenticate(context.Background()) {
		slog.Info("successfully authenticated with Slack")
	} else {
		fatal("failed Slack authentication")
	}
	return &p
}

type slackOutput struct {
	opts         SlackOptions
	sendFailures int
}

type SlackOptions struct {
	BotToken        string
	ApiRoot         string
	FallbackChannel string
	IconEmoji       string
	IconUrl         string
	DetailFields    bool
}

func (p *slackOutput) Send(ctx context.Context, events []*loglang.Event) error {
	// TODO: validate token immediately
	// TODO: outputs should use context and process more than one event at a time

	if p.sendFailures > 3 {
		time.Sleep(time.Second)
		return fmt.Errorf("gave up posting to Slack after %d tries", p.sendFailures)
	}

	for _, event := range events {
		if err := p.sendOne(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

func (p *slackOutput) sendOne(ctx context.Context, event *loglang.Event) error {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
	)

	message := loglang.CoalesceStr(
		event.Field("slack", "message").GetString(),
		event.Field("slack.message").GetString(),
		event.Field("message").GetString(),
		event.Field("msg").GetString(),
	)
	if message == "" {
		if p.opts.DetailFields {
			message = "<missing message>"
		} else {
			return fmt.Errorf("cannot send to Slack because no message text could be found")
		}
	}

	channel := loglang.CoalesceStr(
		event.Field("slack", "channel").GetString(),
		event.Field("slack.channel").GetString(),
		event.Field("channel").GetString(),
		p.opts.FallbackChannel,
	)
	if channel == "" {
		return fmt.Errorf("cannot send to Slack because no channel was selected")
	}

	level := loglang.CoalesceStr(
		event.Field("log", "level").GetString(),
		event.Field("log.level").GetString(),
		event.Field("level").GetString(),
	)
	autoEmojiStr := levelToEmoji[level]
	emojiStr := loglang.CoalesceStr(
		event.Field("slack", "emoji").GetString(),
		event.Field("slack.emoji").GetString(),
		autoEmojiStr,
		event.Field("emoji").GetString(),
	)
	if emojiStr != "" {
		message = emojiStr + " " + message
	}

	x := event.Field("error", "message").GetString()
	fmt.Println(x)
	eventErrText := loglang.CoalesceStr(
		event.Field("error", "stack_trace").GetString(),
		event.Field("error", "message").GetString(),
		event.Field("error", "type").GetString(),
		event.Field("error", "code").GetString(),
		event.Field("error").GetString(),
	)
	if eventErrText != "" {
		message = strings.Join([]string{
			message,
			"```",
			eventErrText,
			"```",
		}, "\n")
	}

	username := loglang.CoalesceStr(
		event.Field("slack", "username").GetString(),
		event.Field("slack.username").GetString(),
		event.Field("user", "full_name").GetString(),
		event.Field("user", "name").GetString(),
		event.Field("user", "email").GetString(),
		event.Field("username").GetString(),
		event.Field("user").GetString(),
	)

	icon_emoji := loglang.CoalesceStr(
		event.Field("slack", "icon_emoji").GetString(),
		event.Field("slack", "icon").GetString(),
		event.Field("slack.icon").GetString(),
		p.opts.IconEmoji,
	)
	if icon_emoji != "" {
		icon_emoji = ":" + strings.Trim(icon_emoji, ":") + ":"
	}

	icon_url := loglang.CoalesceStr(
		event.Field("slack", "icon_url").GetString(),
		p.opts.IconUrl,
	)

	blocks := make([]slackBlockKitTextElement, 0)
	event.TraverseFields(func(field loglang.Field) {
		if len(blocks) == 10 {
			// Slack says: no more than 10 allowed!
			return
		}
		if strings.HasPrefix(field.Path[0], "@") {
			// ignore the special fields, especially @timestamp and @metadata
			return
		}
		if field.Path[0] == "message" {
			// don't send the message in two places
			return
		}
		if len(field.Path) == 1 && field.Path[0] == "error" {
			// an error message here is already handled elsewhere
			return
		}
		if field.Path[0] == "slack" || strings.HasPrefix(field.Path[0], "slack.") {
			// ignore fields that cause special behaviour for Slack
			// TODO: should it be @slack instead?
			// or maybe [@metadata][slack][emoji]?
			return
		}
		val := field.GetString()
		if strings.ContainsAny(val, "\r\n") {
			// ugh no, multi-line attributes look awful as context elements
			return
		}

		blocks = append(blocks, slackBlockKitTextElement{
			Type: "mrkdwn",
			Text: strings.Join([]string{
				strings.Join(field.Path, "."),
				//field.String(),
				val,
			}, ": ") + "\n",
		})
	})

	msg := slackChatPostMessage{
		Channel:   channel,
		Text:      message,
		IconEmoji: icon_emoji,
		IconUrl:   icon_url,
		Username:  username,
	}
	if p.opts.DetailFields {
		msg.Blocks = slackBlocks([]json.RawMessage{
			slackSectionText(message),
			slackContextBlock(blocks),
		})
	}

	response, err := p.api(ctx, chatPostMessage, msg)
	if err != nil {
		return err
	}
	// TODO: include channel and timestamp in this log message?
	// TODO: include metadata?
	log.Debug("posted to Slack")
	fmt.Println(response)

	return nil
}

func (p *slackOutput) canAuthenticate(ctx context.Context) bool {
	var emptyObject map[string]string
	response, err := p.api(ctx, authTest, emptyObject)
	if err != nil {
		return false
	}
	return response.Ok
}

func (p *slackOutput) api(ctx context.Context, method SlackApiMethod, payload any) (slackApiResponse, error) {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
	)

	// JSON body
	encodedPayload, err := json.Marshal(payload)
	if err != nil {
		return slackApiResponse{}, err
	}
	fmt.Println(string(encodedPayload))

	// Create a HTTP post request
	posturl := fmt.Sprintf("%s%s", p.opts.ApiRoot, method)
	req, err := http.NewRequest("POST", posturl, bytes.NewBuffer(encodedPayload))
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", p.opts.BotToken))
	req.Header.Add("Content-Type", "application/json; charset=utf-8") // essential for Slack
	if err != nil {
		p.sendFailures++
		log.Error("failed posting to slack", "error", err)
		return slackApiResponse{}, fmt.Errorf("failed posting to Slack: %w", err)
	}

	// Send it
	client := &http.Client{
		Timeout: time.Second * 5,
	}
	res, err := client.Do(req)
	if err != nil {
		p.sendFailures++
		log.Error("failed posting to slack", "error", err)
		return slackApiResponse{}, fmt.Errorf("failed posting to Slack: %w", err)
	}
	bbbb, _ := io.ReadAll(res.Body)

	var resp slackApiResponse
	err = json.Unmarshal(bbbb, &resp)
	if err != nil {
		p.sendFailures++
		return resp, fmt.Errorf("that's weird... we failed to unmarshal response from Slack API")
	}

	if !resp.Ok {
		// TODO: validate Slack bot token much earlier
		log.Error("Slack API rejected our message. This is usually due to a missing or incorrect BOT_TOKEN.")
		log.Debug(resp.Warning)
		log.Debug(resp.Error)
		return resp, fmt.Errorf("Slack API rejected our message. This is usually due to a missing or incorrect BOT_TOKEN.")
	}

	return resp, nil
}

type SlackApiMethod string

const (
	chatPostMessage SlackApiMethod = "chat.postMessage"
	authTest        SlackApiMethod = "auth.test"
)

type slackChatPostMessage struct {
	Channel   string          `json:"channel"`
	Text      string          `json:"text"`
	IconEmoji string          `json:"icon_emoji,omitempty"`
	IconUrl   string          `json:"icon_url,omitempty"`
	Username  string          `json:"username,omitempty"`
	Blocks    json.RawMessage `json:"blocks"`
}

type slackApiResponse struct {
	Ok        bool   `json:"ok"`
	ChannelId string `json:"channel"`
	Timestamp string `json:"ts"`
	Warning   string `json:"warning"`
	Error     string `json:"error"`
}

var levelToEmoji = map[string]string{
	"important": ":exclamation:",
	"alert":     ":exclamation:",
	"error":     ":boom:",
	"warn":      ":warning:",
	"warning":   ":warning:",
	"info":      ":information_source:",
	"success":   ":tada:",
	"debug":     ":mag:",
	"trace":     ":mag:",
	// numeric levels intentionally left out
	// they are not guaranteed to be consistent between languages
}

// FIXME: these JSON shenanigans fail when pipeline using SchemaNone

func slackBlocks(blocks []json.RawMessage) json.RawMessage {
	var sb strings.Builder
	sb.WriteString(`[`)
	i := 0
	for ; i < len(blocks)-1; i++ {
		sb.Write(blocks[i])
		sb.WriteString(`,`)
	}
	sb.Write(blocks[i])
	sb.WriteString(`]`)
	return []byte(sb.String())
}

func slackSectionText(text string) json.RawMessage {
	var sb strings.Builder
	sb.WriteString(`{`)
	sb.WriteString(`  "type": "section",`)
	sb.WriteString(`  "text": `)
	bb, _ := json.Marshal(slackBlockKitTextElement{
		Type: "mrkdwn",
		Text: text,
	})
	sb.Write(bb)

	sb.WriteString(`  }`)
	return []byte(sb.String())
}

func slackContextBlock(elements []slackBlockKitTextElement) json.RawMessage {
	if len(elements) == 0 {
		return json.RawMessage(``)
	}

	var sb strings.Builder
	sb.WriteString(`{`)
	sb.WriteString(`  "type": "context",`)
	sb.WriteString(`  "elements": [`)
	i := 0
	for ; i < len(elements)-1; i++ {
		e := elements[i]
		bb, _ := json.Marshal(e)
		sb.Write(bb)
		sb.WriteString(`,`)
	}
	bb, _ := json.Marshal(elements[i])
	sb.Write(bb)
	sb.WriteString(`  ]`)
	sb.WriteString(`}`)
	return []byte(sb.String())
}

type slackBlockKitTextElement struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

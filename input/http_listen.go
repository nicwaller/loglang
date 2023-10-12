package input

import (
	"context"
	"fmt"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/framing"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

func HttpListener(port int, opts HttpListenerOptions) loglang.InputPlugin {
	h := &httpListener{
		port: port,
		opts: opts,
	}
	// FIXME: this needs to be more global?
	h.Framing = []loglang.FramingPlugin{framing.Lines()}
	h.Codec = codec.Auto()
	return h
}

type httpListener struct {
	loglang.BaseInputPlugin
	opts HttpListenerOptions
	port int
}

type HttpListenerOptions struct {
	ReplyImmediately bool
}

func (p *httpListener) Run(ctx context.Context, sender loglang.Sender) error {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
		"server.port", strconv.Itoa(p.port),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		eventTemplate := p.httpEventTemplate(request)

		writer.Header().Add("Server", "loglang 0.0.0")
		writer.Header().Add("Content-Type", "text/plain")

		var doSending func() (*loglang.BatchResult, error)
		switch request.Header.Get("Content-Type") {
		case "text/plain":
			doSending = func() (*loglang.BatchResult, error) {
				return sender.SendWithFramingCodec(
					request.Context(),
					eventTemplate,
					framing.Lines(),
					codec.Plain("message"),
					request.Body,
				)
			}
		case "application/json":
			doSending = func() (*loglang.BatchResult, error) {
				return sender.SendWithFramingCodec(
					request.Context(),
					eventTemplate,
					framing.Whole(),
					codec.Json(),
					request.Body,
				)
			}
		case "application/x-ndjson":
			doSending = func() (*loglang.BatchResult, error) {
				return sender.SendWithFramingCodec(
					request.Context(),
					eventTemplate,
					framing.Lines(),
					codec.Json(),
					request.Body,
				)
			}
		default:
			doSending = func() (*loglang.BatchResult, error) {
				return sender.SendRaw(
					request.Context(),
					eventTemplate,
					request.Body,
				)
			}
		}

		// FIXME: not yet, what if there is an error?
		if p.opts.ReplyImmediately {
			writer.WriteHeader(http.StatusAccepted)
			// the only way to send the response immediately is by returning immediately,
			// so we need to move sending into a separate goroutine.
			go func() {
				result, err := doSending()
				if err != nil {
					log.Error("doSending() failed", "result", result)
				}
			}()
		} else {
			result, err := doSending()
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				log.Error("doSending() failed", "error", err, "result", result)
				return
			} else {
				writer.WriteHeader(http.StatusOK)
			}
			if result != nil {
				p.writeSummary(ctx, writer, result)
			}
		}

		// FIXME: this is a dirty hack to keep the reader open long enough
		time.Sleep(200 * time.Millisecond)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", p.port),
		Handler: mux,
	}

	log.Debug("starting listener")
	go func() {
		err := server.ListenAndServe()
		if err != nil {
			log.Error("failed to listen and serve", "error", err)
		}
	}()
	log.Info("started listening on " + server.Addr)

	<-ctx.Done()
	// TODO: should use a timeout on server shutdown
	_ = server.Shutdown(context.TODO())

	return nil
}

func (p *httpListener) httpEventTemplate(request *http.Request) *loglang.Event {
	evt := loglang.NewEvent()

	switch p.Schema {
	case loglang.SchemaNone:
		// don't enrich with any automatic fields
	case loglang.SchemaLogstashFlat:
		evt.Field("host").SetString(request.RemoteAddr)
		// TODO: should I use .Default() here to avoid overwriting fields sent to us?
		evt.Field("headers", "http_version").SetString("1.1") // TODO: is this right?
		evt.Field("headers", "http_user_agent").SetString(request.Header.Get("User-Agent"))
		evt.Field("headers", "http_host").SetString(request.Host)
		evt.Field("headers", "request_method").SetString(request.Method)
		evt.Field("headers", "request_path").SetString(request.URL.Path)
		evt.Field("headers", "content_length").SetInt(int(request.ContentLength))
		evt.Field("headers", "content_type").SetString(request.Header.Get("Content-Type"))
	case loglang.SchemaLogstashECS:
		evt.Field("host", "ip").SetString(request.RemoteAddr)
		evt.Field("http", "version").SetString("1.1")
		evt.Field("http", "method").SetString(request.Method)
		evt.Field("http", "request", "body", "bytes").SetInt(int(request.ContentLength))
		evt.Field("http", "request", "mime_type").SetString(request.Header.Get("Content-Type"))
		evt.Field("url", "domain").SetString(request.Host)
		evt.Field("url", "port").SetInt(p.port)
		evt.Field("url", "path").SetString(request.URL.Path)
		evt.Field("user_agent", "original").SetString(request.Header.Get("User-Agent"))
	case loglang.SchemaFlat:
		evt.Field("host").SetString(request.RemoteAddr)
		evt.Field("headers", "http_version").SetString("1.1")
		evt.Field("headers", "http_user_agent").SetString(request.Header.Get("User-Agent"))
		evt.Field("headers", "http_host").SetString(request.Host)
		evt.Field("headers", "request_method").SetString(request.Method)
		evt.Field("headers", "request_path").SetString(request.URL.Path)
		evt.Field("headers", "content_length").SetInt(int(request.ContentLength))
		evt.Field("headers", "content_type").SetString(request.Header.Get("Content-Type"))
	case loglang.SchemaECS:
		evt.Field("host", "ip").SetString(request.RemoteAddr)
		evt.Field("http", "version").SetString("1.1")
		evt.Field("http", "method").SetString(request.Method)
		evt.Field("http", "request", "body", "bytes").SetInt(int(request.ContentLength))
		evt.Field("http", "request", "mime_type").SetString(request.Header.Get("Content-Type"))
		evt.Field("url", "domain").SetString(request.Host)
		evt.Field("url", "port").SetInt(p.port)
		evt.Field("url", "path").SetString(request.URL.Path)
		evt.Field("user_agent", "original").SetString(request.Header.Get("User-Agent"))
	}

	return &evt
}

func (p *httpListener) writeSummary(ctx context.Context, writer http.ResponseWriter, result *loglang.BatchResult) {
	if result.Ok {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(result.Summary()))
	} else {
		log := slog.Default().With(
			"pipeline", ctx.Value("pipeline"),
			"plugin", ctx.Value("plugin"),
			"server.port", strconv.Itoa(p.port),
		)

		log.Warn("failed or incomplete batch from HTTP listener", "error", result)
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

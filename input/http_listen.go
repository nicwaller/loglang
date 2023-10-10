package input

import (
	"bytes"
	"context"
	"fmt"
	"github.com/nicwaller/loglang"
	"github.com/nicwaller/loglang/codec"
	"github.com/nicwaller/loglang/framing"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

func HttpListener(port int, opts HttpListenerOptions) loglang.InputPlugin {
	if opts.Framing == nil {
		opts.Framing = framing.Whole()
	}
	if opts.Codec == nil {
		opts.Codec = codec.Json()
	}
	return &httpListener{
		port: port,
		opts: opts,
	}
}

type httpListener struct {
	opts HttpListenerOptions
	port int
}

type HttpListenerOptions struct {
	Framing loglang.FramingPlugin
	Codec   loglang.CodecPlugin
	Schema  loglang.SchemaModel
}

func (p *httpListener) Run(ctx context.Context, send loglang.BatchSender) error {
	log := slog.Default().With(
		"pipeline", ctx.Value("pipeline"),
		"plugin", ctx.Value("plugin"),
		"server.port", strconv.Itoa(p.port),
	)

	schema := p.opts.Schema
	if schema == loglang.SchemaNotDefined {
		if pipelineSchema, ok := ctx.Value("schema").(loglang.SchemaModel); ok {
			schema = pipelineSchema
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Add("Server", "loglang 0.0.0")
		writer.Header().Add("Content-Type", "text/plain")
		// TODO: wait to deliver message
		writer.WriteHeader(http.StatusOK)
		// TODO: think about HTTP response codes
		// 200 OK
		// 201 Created
		// 202 Accepted

		bb, err := io.ReadAll(request.Body)
		if err != nil {
			log.Warn("weird failure reading from HTTP connection", "error", err)
		}

		// FIXME: think very carefull about what happens when receiving a 0 byte payload
		frames := make(chan []byte)
		go func() {
			err := p.opts.Framing.Run(ctx, bytes.NewReader(bb), frames)
			if err != nil {
				log.Error(err.Error())
			}
		}()
		go func() {
			for {
				// TODO: handle context termination
				// TODO: maybe also a timeout?
				select {
				case frame := <-frames:
					evt, err := p.opts.Codec.Decode(frame)
					if err != nil {
						log.Error(fmt.Errorf("lost whole or part of HTTP body: %w", err).Error())
					}
					switch schema {
					case loglang.SchemaLogstashFlat:
						evt.Field("host").SetString(request.RemoteAddr)
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
					send(evt)
				}
			}
		}()
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

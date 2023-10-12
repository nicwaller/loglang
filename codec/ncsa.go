package codec

import (
	"bufio"
	"bytes"
	"github.com/nicwaller/loglang"
	"strconv"
	"strings"
)

// NCSA Common Log format
// https://en.wikipedia.org/wiki/Common_Log_Format

func NCSACommonLog() loglang.CodecPlugin {
	return &ncsaCommonLog{}
}

type ncsaCommonLog struct {
	schema loglang.SchemaModel
}

func (p *ncsaCommonLog) Encode(_ loglang.Event) ([]byte, error) {
	panic("not implemented")
}

func (p *ncsaCommonLog) Decode(dat []byte) (loglang.Event, error) {
	s := bufio.NewScanner(bytes.NewReader(dat))
	s.Split(splitNcsa)

	var host string
	if s.Scan() {
		host = s.Text()
	}

	var identUser string
	if s.Scan() {
		identUser = s.Text()
	}

	var authuser string

	if s.Scan() {
		authuser = s.Text()
	}

	var datestamp string
	if s.Scan() {
		datestamp = s.Text()
	}

	var method string
	var path string
	var httpVersion string
	if s.Scan() {
		request := s.Text()
		var middle string
		var httpProto string
		method, middle, _ = strings.Cut(request, " ")
		path, httpProto, _ = strings.Cut(middle, " ")
		_, httpVersion, _ = strings.Cut(httpProto, "/")
	}

	var httpStatusCode int
	if s.Scan() {
		httpStatusCode, _ = strconv.Atoi(s.Text())
	}

	var requestByteCount int
	if s.Scan() {
		requestByteCount, _ = strconv.Atoi(s.Text())
	}

	var referrer string
	if s.Scan() {
		referrer = s.Text()
	}

	var useragent string
	if s.Scan() {
		useragent = s.Text()
	}

	evt := loglang.NewEvent()

	switch p.schema {
	case loglang.SchemaLogstashFlat:
		// https://github.com/logstash-plugins/logstash-patterns-core/blob/10d9a9318bfee2e2e320e02a67057a193661ebd9/patterns/httpd#L5
		evt.Field("clientip").SetString(host)
		evt.Field("ident").SetString(identUser)
		evt.Field("auth", "id").SetString(authuser)
		evt.Field("timestamp").SetString(datestamp)
		evt.Field("verb").SetString(method)
		evt.Field("request").SetString(path)
		evt.Field("httpversion").SetString(httpVersion)
		evt.Field("bytes").SetInt(requestByteCount)
		evt.Field("response").SetInt(httpStatusCode)
		evt.Field("referrer").SetString(referrer)
		evt.Field("agent").SetString(useragent)

	case loglang.SchemaFlat:
		evt.Field("@timestamp").SetString(datestamp)
		evt.Field("http_version").SetString(httpVersion)
		evt.Field("http_referrer").SetString(referrer)
		evt.Field("http_method").SetString(method)
		evt.Field("bytes").SetInt(requestByteCount)
		evt.Field("http_status_code").SetInt(httpStatusCode)
		evt.Field("host").SetString(host) // ???
		//evt.Field("user", "name").SetString(authuser)
		//evt.Field("user", "id").SetString(identUser)
		evt.Field("user_agent").SetString(useragent)
		evt.Field("path").SetString(path)

	case loglang.SchemaLogstashECS:
		//	https://www.elastic.co/guide/en/elasticsearch/reference/current/common-log-format-example.html
		evt.Field("@timestamp").SetString(datestamp)
		evt.Field("http", "version").SetString(httpVersion)
		evt.Field("http", "request", "referrer").SetString(referrer)
		evt.Field("http", "request", "method").SetString(method)
		evt.Field("http", "response", "body", "bytes").SetInt(requestByteCount)
		evt.Field("http", "response", "status_code").SetInt(httpStatusCode)
		evt.Field("source", "ip").SetString(host)
		evt.Field("user", "name").SetString(identUser)
		evt.Field("user", "id").SetString(authuser)
		evt.Field("user_agent").SetString(useragent)
		evt.Field("url", "original").SetString(path)

	case loglang.SchemaECS:
		evt.Field("@timestamp").SetString(datestamp)
		evt.Field("http", "version").SetString(httpVersion)
		evt.Field("http", "request", "referrer").SetString(referrer)
		evt.Field("http", "request", "method").SetString(method)
		evt.Field("http", "response", "body", "bytes").SetInt(requestByteCount)
		evt.Field("http", "response", "status_code").SetInt(httpStatusCode)
		evt.Field("host", "hostname").SetString(host) // ???
		evt.Field("user", "name").SetString(authuser)
		evt.Field("user", "id").SetString(identUser)
		evt.Field("user_agent", "original").SetString(useragent)
		evt.Field("url", "path").SetString(path)
	}

	return evt, nil
}

var (
	splitNcsa = func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if data[0] == '[' {
			if i := bytes.IndexByte(data, ']'); i >= 0 {
				return i + 2, data[1:i], nil
			} else {
				// uh... corrupt string?
				return len(data), data, nil
			}
		}
		if data[0] == '"' {
			offset := 1
			if i := bytes.IndexByte(data[offset:], '"'); i >= 0 {
				i += offset
				return i + 2, data[1:i], nil
			} else {
				// uh... corrupt string?
				return len(data), data, nil
			}
		}
		if i := bytes.IndexByte(data, ' '); i >= 0 {
			return i + 1, data[0:i], nil
		}
		// If we're at EOF, we have a final, non-terminated field. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	}
)

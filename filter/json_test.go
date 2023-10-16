package filter

import (
	"github.com/nicwaller/loglang"
	"testing"
)

func TestJson(t *testing.T) {
	filter := Json("", "message")
	//return func(event *loglang.Event, inject chan<- *loglang.Event, drop func()) error {
	injector := make(chan *loglang.Event, 1)
	dropper := func() {
		t.Error("should not drop event")
	}

	evt := loglang.NewEvent()
	evt.Field("message").SetString(`{"error": "too_awesome"}`)
	err := filter(&evt, injector, dropper)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-injector:
		t.Error("should not inject events")
	default:
		// good
	}
}

package filter

import (
	"github.com/nicwaller/loglang"
)

// Replace the value of a field with a new value, or add the field if it doesn’t already exist.
func Replace(name, field string, content string) loglang.FilterPlugin {
	return func(event loglang.Event, send chan<- loglang.Event) error {
		event.Field(field).SetString(content)
		send <- event
		return nil
	}
}

func Remove(name string, field string) loglang.FilterPlugin {
	return func(event loglang.Event, send chan<- loglang.Event) error {
		event.Field(field).Delete()
		send <- event
		return nil
	}
}

// FIXME: rename doesn't support deep fields
func Rename(name string, oldField string, newField string) loglang.FilterPlugin {
	return func(event loglang.Event, send chan<- loglang.Event) error {
		oldF := event.Field(oldField)
		newF := event.Field(newField)
		newF.Set(oldF.MustGet())
		oldF.Delete()
		send <- event
		return nil
	}
}

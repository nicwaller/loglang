package filter

import (
	"github.com/nicwaller/loglang"
)

// Replace the value of a field with a new value, or add the field if it doesn’t already exist.
func Replace(field string, content string) loglang.FilterPlugin {
	return func(event *loglang.Event, inject chan<- loglang.Event, drop func()) error {
		event.Field(field).SetString(content)
		return nil
	}
}

func Remove(field string) loglang.FilterPlugin {
	return func(event *loglang.Event, inject chan<- loglang.Event, drop func()) error {
		event.Field(field).Delete()
		return nil
	}
}

// FIXME: rename doesn't support deep fields
func Rename(oldField string, newField string) loglang.FilterPlugin {
	return func(event *loglang.Event, inject chan<- loglang.Event, drop func()) error {
		oldF := event.Field(oldField)
		newF := event.Field(newField)
		newF.Set(oldF.MustGet())
		oldF.Delete()
		return nil
	}
}

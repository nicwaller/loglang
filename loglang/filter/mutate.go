package filter

import "loglang/loglang"

// Replace the value of a field with a new value, or add the field if it doesn’t already exist.
func Replace(name, field string, content string) loglang.FilterPlugin {
	return loglang.FilterPlugin{
		Name: name,
		Run: func(event loglang.Event) (loglang.Event, error) {
			event.OrderedFields[field] = content
			return event, nil
		},
	}
}

func Remove(name string, field string) loglang.FilterPlugin {
	return loglang.FilterPlugin{
		Name: name,
		Run: func(event loglang.Event) (loglang.Event, error) {
			delete(event.OrderedFields, field)
			return event, nil
		},
	}
}

func Rename(name string, oldField string, newField string) loglang.FilterPlugin {
	return loglang.FilterPlugin{
		Name: name,
		Run: func(event loglang.Event) (loglang.Event, error) {
			event.OrderedFields[newField] = event.OrderedFields[oldField]
			delete(event.OrderedFields, oldField)
			return event, nil
		},
	}
}

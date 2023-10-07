package filter

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"loglang/loglang"
	"strings"
)

var x loglang.FilterPlugin

// JSON filter doesn't make sense...?
func Json(name string, sourceField string) loglang.FilterPlugin {
	return loglang.FilterPlugin{
		Name: name,
		Run: func(event loglang.Event, send chan<- loglang.Event) error {
			source := event.Field(sourceField).GetString()
			if !strings.HasPrefix(source, "{") ||
				!strings.HasSuffix(source, "}") {
				return fmt.Errorf("dropped event: field [%s] doesn't look like JSON", sourceField)
			}
			var body map[string]interface{}
			if err := json.Unmarshal([]byte(source), &body); err != nil {
				slog.Debug(fmt.Sprintf("from %s: %s", source, err.Error()))
				return fmt.Errorf("dropped event: %s", err.Error())
			}
			for k, v := range body {
				if strVal, ok := v.(string); ok {
					event.Field(k).SetString(strVal)
				} else if intVal, ok := v.(int); ok {
					event.Field(k).SetInt(intVal)
				} else {
					return fmt.Errorf("unhandled field type")
				}
			}
			send <- event
			return nil
		},
	}
}
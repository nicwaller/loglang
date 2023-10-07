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
		Run: func(event loglang.Event) (loglang.Event, error) {
			source := event.GetString(sourceField)
			if !strings.HasPrefix(source, "{") ||
				!strings.HasSuffix(source, "}") {
				//log.Warn().Msg("json filter discarding message because it doesn't look like JSON")
				return event, fmt.Errorf("field [%s] doesn't look like JSON", sourceField)
			}
			var body map[string]interface{}
			if err := json.Unmarshal([]byte(source), &body); err != nil {
				slog.Debug(fmt.Sprintf("from %s: %s", source, err.Error()))
				return event, err
			}
			for k, v := range body {
				if strVal, ok := v.(string); ok {
					event.SetString(k, strVal)
				} else if intVal, ok := v.(int); ok {
					event.SetInt(k, intVal)
				} else {
					return event, fmt.Errorf("unhandled field type")
				}
			}
			return event, nil
		},
	}
}

package loglang

import (
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
)

// represents exactly one (possibly nested) field
// like a terrible version of XPath or JSONPath
type Field struct {
	Path     []string
	original *Event
}

func (fld *Field) MustGet() any {
	v, _ := fld.Get()
	return v
}

func (fld *Field) Get() (any, error) {
	if fld.original == nil {
		return nil, fmt.Errorf("cannot Field.Get() because there is no linked event")
	}
	if len(fld.Path) == 0 {
		return nil, fmt.Errorf("cannot traverse empty Path")
	}

	var level map[string]any
	level = fld.original.Fields
	for depth, key := range fld.Path {
		inner, keyExists := level[key]
		if !keyExists {
			return nil, fmt.Errorf("tried to Field.Get() on non-existent key")
		}
		if depth == len(fld.Path)-1 {
			return inner, nil
		}
		if innerMap, isMap := inner.(map[string]any); isMap {
			level = innerMap
		}
	}
	panic("impossible")
}

func (fld *Field) Default(value any) {
	err := fld.set(value, false)
	if err != nil {
		slog.Warn(err.Error())
	}
}

func (fld *Field) Set(value any) {
	err := fld.set(value, true)
	if err != nil {
		slog.Warn(err.Error())
	}
}

func (fld *Field) SetCarefully(value any) error {
	return fld.set(value, true)
}

func (fld *Field) set(value any, overwrite bool) error {
	if fld.original == nil {
		return fmt.Errorf("cannot Field.Get() because there is no linked event")
	}
	if len(fld.Path) == 0 {
		return fmt.Errorf("cannot traverse empty Path")
	}

	var level map[string]any
	level = fld.original.Fields
	for i := 0; i < len(fld.Path)-1; i++ {
		key := fld.Path[i]
		inner, keyExists := level[key]
		if !keyExists {
			level[key] = make(map[string]any)
		} else if keyExists {
			if _, isMap := inner.(map[string]any); isMap {
				// good
			} else {
				// damn
				slog.Warn(strings.Join(fld.Path[:i+1], ".") + " is getting implicitly overwritten; make sure to delete it first")
				// do we want to preserve the original value like this?
				// level["_"+key] = level[key]
				level[key] = make(map[string]any)
			}
		}
		level = level[key].(map[string]any)
	}

	leafKey := fld.Path[len(fld.Path)-1]
	if _, exists := level[leafKey]; exists && !overwrite {
		// being quiet is okay if we explicitly do not want overwrites
		return nil
	}

	switch value.(type) {
	case string:
		level[leafKey] = value
	case int:
		level[leafKey] = value
	case int64:
		level[leafKey] = value
	case float64:
		level[leafKey] = value
	case bool:
		level[leafKey] = value
	default:
		return fmt.Errorf("failed Set(); rejected type %v %v", reflect.TypeOf(value), value)
	}

	return nil
}

func (fld *Field) SetString(value string) {
	fld.Set(value)
}

func (fld *Field) SetInt(value int) {
	fld.Set(value)
}

func (fld *Field) SetFloat(value float64) {
	fld.Set(value)
}

func (fld *Field) SetBool(value bool) {
	fld.Set(value)
}

func (fld *Field) GetString() string {
	rawValue, err := fld.Get()
	if err != nil {
		return ""
	}

	switch v := rawValue.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case float64:
		// TODO: there's a better way
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		panic("unsupported type")
	}
}

func (fld *Field) GetInt() int {
	rawValue, err := fld.Get()
	if err != nil {
		return 0
	}

	switch v := rawValue.(type) {
	case string:
		vv, err := strconv.Atoi(v)
		if err != nil {
			return vv
		} else {
			return 0
		}
	case int:
		return v
	case float64:
		// TODO: there's a better way
		return int(v)
	case bool:
		if v == false {
			return 0
		} else {
			return 1
		}
	default:
		panic("unsupported type")
	}
}

func (fld *Field) GetFloat() float64 {
	rawValue, err := fld.Get()
	if err != nil {
		return 0
	}

	switch v := rawValue.(type) {
	case string:
		vv, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return vv
		} else {
			return 0.0
		}
	case int:
		return float64(v)
	case float64:
		return v
	case bool:
		if v == false {
			return 0.0
		} else {
			return 1.0
		}
	default:
		panic("unsupported type")
	}
}

func (fld *Field) GetBool(field string) bool {
	rawValue, err := fld.Get()
	if err != nil {
		return false
	}

	switch v := rawValue.(type) {
	case string:
		bb, err := strconv.ParseBool(v)
		if err != nil {
			return bb
		} else {
			return false
		}
	case int:
		return v > 0
	case float64:
		return v > 0
	case bool:
		return v
	default:
		panic("unsupported type")
	}
}

func (fld *Field) Delete() {
	_ = fld.DeleteCarefully()
}

func (fld *Field) DeleteCarefully() error {
	if fld.original == nil {
		return fmt.Errorf("cannot Field.Get() because there is no linked event")
	}
	if len(fld.Path) == 0 {
		return fmt.Errorf("cannot traverse empty Path")
	}

	var level map[string]any
	level = fld.original.Fields
	for i := 0; i < len(fld.Path)-1; i++ {
		key := fld.Path[i]
		inner, keyExists := level[key]
		if !keyExists {
			level[key] = make(map[string]any)
		} else if keyExists {
			if _, isMap := inner.(map[string]any); isMap {
				// good
			} else {
				// damn
				slog.Warn("key is getting overwritten")
				level[key] = make(map[string]any)
			}
		}
		level = level[key].(map[string]any)
	}

	delete(level, fld.Path[len(fld.Path)-1])
	//fld.original.touch(fld.Path[0])
	return nil
}

func (fld *Field) String() string {
	var sb strings.Builder
	for _, v := range fld.Path {
		sb.WriteString(`[` + v + `]`)
	}
	return sb.String()
}

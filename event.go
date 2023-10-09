package loglang

import (
	"sort"
)

type Event struct {
	// start with flat, maybe add hierarchy later
	// start with strings, maybe other types later
	Fields map[string]interface{}
	//touchOrder []string
}

func NewEvent() Event {
	var newEvt Event
	newEvt.Fields = make(map[string]interface{})
	//newEvt.touchOrder = make([]string, 0)
	return newEvt
}

func (evt *Event) Field(path ...string) *Field {
	// warning: don't try to be clever and split the path components
	//on "." to get smaller path components. It must be possible to
	//specify fields that contain a "." in the name!
	//
	// no need to verify that the field currently exists
	// because we can also use this for setting values
	return &Field{
		Path:     path,
		original: evt,
	}
}

func (evt *Event) Set(field string, value any) {
	evt.Field(field).Set(value)
}

func (evt *Event) Get(field string) any {
	return evt.Field(field).MustGet()
}

// returned in the order they were touched
// for deterministic encoding to kv, json, whatever
//func (evt *Event) OrderedFields() []string {
//	return evt.touchOrder
//}

// depth first
// might be better with channel or callback or iterator?
type fieldCb func(field Field)

func (evt *Event) TraverseFields(cb fieldCb) {
	evt.traverseFields(cb, []string{}, &evt.Fields)
}

func (evt *Event) traverseFields(cb fieldCb, prefix []string, from *map[string]any) {
	// getting an ordered set of keys is essential for deterministic encoding, especially for the kv codec
	keys := make([]string, 0, len(*from))
	for k := range *from {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// now traverse the keys in order
	for _, k := range keys {
		v := (*from)[k]
		if z, isMap := v.(map[string]any); isMap {
			evt.traverseFields(cb, append(prefix, k), &z)
		} else {
			cb(Field{
				Path:     append(prefix, k),
				original: evt,
			})
		}
	}
}

func (evt *Event) Copy() Event {
	newEvt := NewEvent()
	for k, v := range evt.Fields {
		newEvt.Fields[k] = v
	}
	//newEvt.touchOrder = make([]string, len(evt.touchOrder))
	//copy(newEvt.touchOrder, evt.touchOrder)
	return newEvt
}

//func (evt *Event) touch(field string) {
//	newOrder := make([]string, 0)
//	for _, v := range evt.touchOrder {
//		if v != field {
//			newOrder = append(newOrder, v)
//		}
//	}
//	newOrder = append(newOrder, field)
//	evt.touchOrder = newOrder
//}

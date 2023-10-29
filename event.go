package loglang

import (
	"sort"
	"sync/atomic"
)

type Event struct {
	Fields map[string]interface{}

	// the event may have been received as part of a batch with other events
	// and we want to support end-to-end acknowledgement for the whole batch
	batch *publishingBatch

	// if using end-to-end acknowledgement, count finsihed output plugins
	// when this reaches len(outputs) the event is fully delivered
	// and the batch can be notified
	finishedOutputs *atomic.Uint32
}

func NewEvent() Event {
	var newEvt Event
	newEvt.Fields = make(map[string]interface{})
	newEvt.finishedOutputs = &atomic.Uint32{}
	return newEvt
}

func (evt *Event) Field(path ...string) *Field {
	// warning: don't try to be clever and split the path components
	//on "." to get smaller path components. It must be possible to
	//specify fields that contain a "." in the Name!
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
	evt.traverseFields(true, cb, []string{}, &evt.Fields)
}

func (evt *Event) traverseFields(inOrder bool, cb fieldCb, prefix []string, from *map[string]any) {
	// getting an ordered set of keys is essential for deterministic encoding, especially for the kv codec
	keys := make([]string, 0, len(*from))
	for k := range *from {
		keys = append(keys, k)
	}
	if inOrder {
		sort.Strings(keys)
	}
	// now traverse the keys in order
	for _, k := range keys {
		v := (*from)[k]
		if z, isMap := v.(map[string]any); isMap {
			evt.traverseFields(inOrder, cb, append(prefix, k), &z)
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

func (evt *Event) Merge(template *Event, overwrite bool) {
	template.traverseFields(false, func(field Field) {
		v := field.MustGet()
		field.original = evt
		if overwrite {
			field.Set(v)
		} else {
			field.Default(v)
		}
	}, []string{}, &evt.Fields)
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

package dataloader

import (
	"github.com/preston-wagner/unicycle"
)

type query[KEY_TYPE comparable, VALUE_TYPE any] struct {
	key     KEY_TYPE
	promise *unicycle.Promise[VALUE_TYPE]
}

type batch[KEY_TYPE comparable, VALUE_TYPE any] map[KEY_TYPE][]*unicycle.Promise[VALUE_TYPE]

func (btch batch[KEY_TYPE, VALUE_TYPE]) addToBatch(incomingQuery query[KEY_TYPE, VALUE_TYPE]) {
	_, ok := btch[incomingQuery.key]
	if !ok {
		btch[incomingQuery.key] = []*unicycle.Promise[VALUE_TYPE]{}
	}
	btch[incomingQuery.key] = append(btch[incomingQuery.key], incomingQuery.promise)
}

func (btch batch[KEY_TYPE, VALUE_TYPE]) resolveAll(values map[KEY_TYPE]VALUE_TYPE, errs map[KEY_TYPE]error) {
	for key := range btch {
		if value, ok := values[key]; ok {
			btch.resolveKey(key, value)
		} else if err, ok := errs[key]; ok {
			btch.rejectKey(key, err)
		} else {
			btch.rejectKey(key, ErrMissingResponse)
		}
	}
}

func (btch batch[KEY_TYPE, VALUE_TYPE]) resolveKey(key KEY_TYPE, value VALUE_TYPE) {
	for _, promise := range btch[key] {
		promise.Resolve(value, nil)
	}
}

func (btch batch[KEY_TYPE, VALUE_TYPE]) rejectKey(key KEY_TYPE, err error) {
	for _, promise := range btch[key] {
		promise.Resolve(unicycle.ZeroValue[VALUE_TYPE](), err)
	}
}

func (btch batch[KEY_TYPE, VALUE_TYPE]) rejectAll(err error) {
	for key := range btch {
		btch.rejectKey(key, err)
	}
}

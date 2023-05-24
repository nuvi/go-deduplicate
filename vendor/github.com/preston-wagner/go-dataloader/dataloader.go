package dataloader

import (
	"sync"

	"github.com/preston-wagner/unicycle"
)

type DataLoader[KEY_TYPE comparable, VALUE_TYPE any] struct {
	queryBatcher *QueryBatcher[KEY_TYPE, VALUE_TYPE]
	promiseCache map[KEY_TYPE]*unicycle.Promise[VALUE_TYPE]
	lock         *sync.RWMutex
}

func NewDataLoader[KEY_TYPE comparable, VALUE_TYPE any](getter Getter[KEY_TYPE, VALUE_TYPE], maxConcurrentBatches, maxBatchSize int) *DataLoader[KEY_TYPE, VALUE_TYPE] {
	return &DataLoader[KEY_TYPE, VALUE_TYPE]{
		queryBatcher: NewQueryBatcher(getter, maxConcurrentBatches, maxBatchSize),
		promiseCache: map[KEY_TYPE]*unicycle.Promise[VALUE_TYPE]{},
		lock:         &sync.RWMutex{},
	}
}

func (dataLoader *DataLoader[KEY_TYPE, VALUE_TYPE]) Load(key KEY_TYPE) (VALUE_TYPE, error) {
	return dataLoader.LoadPromise(key).Await()
}

func (dataLoader *DataLoader[KEY_TYPE, VALUE_TYPE]) LoadPromise(key KEY_TYPE) *unicycle.Promise[VALUE_TYPE] {
	dataLoader.lock.RLock()
	promise, ok := dataLoader.promiseCache[key]
	dataLoader.lock.RUnlock()
	if !ok {
		dataLoader.lock.Lock()
		defer dataLoader.lock.Unlock()
		promise, ok = dataLoader.promiseCache[key] // it's possible it was set immediately after RUnlock on another goroutine
		if !ok {
			promise = dataLoader.queryBatcher.LoadPromise(key)
			dataLoader.promiseCache[key] = promise
		}
	}
	return promise
}

func (dataLoader *DataLoader[KEY_TYPE, VALUE_TYPE]) Close() {
	dataLoader.queryBatcher.Close()
}

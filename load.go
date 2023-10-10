package deduplicate

import (
	"errors"
	"time"

	"github.com/nuvi/go-dataloader"
	"github.com/nuvi/unicycle/defaults"
)

// this allows us to make sure expensive tasks are only ever run once, across all pods
func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) Load(key KEY_TYPE) (VALUE_TYPE, error) {
	// check if success in memory
	value, ok := tp.completedCache.Get(key)
	if ok {
		return value, nil
	}

	// check if failure in memory
	err, ok := tp.failureCache.Get(key)
	if ok {
		return defaults.ZeroValue[VALUE_TYPE](), err
	}

	// get canonical database key
	keyStr, err := tp.getKeyStr(key)
	if err != nil {
		return defaults.ZeroValue[VALUE_TYPE](), err
	}

	// check if success in database
	value, err = tp.getCompletedTask(keyStr)
	if err == nil {
		go tp.completedCache.Set(key, value)
		return value, nil
	} else if !errors.Is(err, dataloader.ErrMissingResponse) {
		return defaults.ZeroValue[VALUE_TYPE](), err
	}

	// check if failure in database
	err = tp.getFailedTask(keyStr)
	if err != nil {
		go tp.failureCache.Set(key, err)
		return defaults.ZeroValue[VALUE_TYPE](), err
	}

	// if none of the above are true, start a new task
	err = tp.createPendingTask(keyStr)
	if err != nil {
		if errors.Is(err, errPendingStarted) { // if the task is already pending, wait on result
			return tp.awaitPendingTask(keyStr, key)
		}
		return defaults.ZeroValue[VALUE_TYPE](), err
	}

	value, err = tp.getter(key)
	if err != nil {
		go tp.failureCache.Set(key, err)
		go tp.createFailedTask(keyStr, err)
		return defaults.ZeroValue[VALUE_TYPE](), err
	} else {
		go tp.completedCache.Set(key, value)
		go tp.createCompletedTask(keyStr, value)
		return value, nil
	}
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) awaitPendingTask(keyStr string, key KEY_TYPE) (VALUE_TYPE, error) {
	pendingTask, err := tp.getPendingTask(keyStr)
	if err != nil {
		return defaults.ZeroValue[VALUE_TYPE](), err
	}

	backoff := time.Second

	for {
		// check if pending task expired
		if pendingTask.isExpired(tp.pendingTTL) {
			return defaults.ZeroValue[VALUE_TYPE](), errPendingTimeout
		}

		// exponential backoff before next database check
		time.Sleep(backoff)
		backoff *= 2

		// check if success in database
		value, err := tp.getCompletedTask(keyStr)
		if err == nil {
			go tp.completedCache.Set(key, value)
			return value, nil
		} else if !errors.Is(err, dataloader.ErrMissingResponse) {
			return defaults.ZeroValue[VALUE_TYPE](), err
		}

		// check if failure in database
		err = tp.getFailedTask(keyStr)
		if err != nil {
			go tp.failureCache.Set(key, err)
			return defaults.ZeroValue[VALUE_TYPE](), err
		}
	}
}

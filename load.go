package deduplicate

import (
	"errors"
	"log"
	"time"

	"github.com/preston-wagner/go-dataloader"
	"github.com/preston-wagner/unicycle"
)

// this allows us to make sure expensive tasks are only ever run once, across all pods
func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) Load(key KEY_TYPE) (VALUE_TYPE, error) {
	keyStr, err := tp.getKeyStr(key)
	if err != nil {
		return unicycle.ZeroValue[VALUE_TYPE](), err
	}

	// check if task has already succeeded
	value, err := tp.getCompletedTask(keyStr)
	if err == nil {
		return value, nil
	} else if !errors.Is(err, dataloader.ErrMissingResponse) {
		return unicycle.ZeroValue[VALUE_TYPE](), err
	}

	// check if task has already failed
	err = tp.getFailedTask(keyStr)
	if err != nil {
		return unicycle.ZeroValue[VALUE_TYPE](), err
	}

	// if neither of the above are true, start a new task
	err = tp.createPendingTask(keyStr)
	if err != nil {
		if errors.Is(err, errPendingStarted) { // if the task is already pending, wait and retry
			time.Sleep(time.Second)
			return tp.Load(key)
		}
		return unicycle.ZeroValue[VALUE_TYPE](), err
	}

	value, err = tp.getter(key)
	if err != nil {
		return unicycle.ZeroValue[VALUE_TYPE](), tp.createFailedTask(keyStr, err)
	}
	err = tp.createCompletedTask(keyStr, value)
	if err != nil {
		log.Println(err) // if the actual task succeeded, we don't want to return an error even if chaching the result fails somehow
	}

	return value, nil
}

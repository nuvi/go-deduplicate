package deduplicate

import (
	"errors"
	"strings"
	"time"

	"github.com/preston-wagner/unicycle"
)

type PendingTask struct {
	Key       string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"default:NOW()"`
}

var errPendingTimeout = errors.New("pending task timed out")
var errPendingStarted = errors.New("this task has already been started")

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) getPendingTasks(keys []string) (map[string]PendingTask, map[string]error) {
	pendingTasks := []PendingTask{}
	result := tp.db.Where("key IN ?", keys).Take(&pendingTasks)
	if result.Error != nil {
		errs := map[string]error{}
		for _, key := range keys {
			errs[key] = result.Error
		}
		return nil, errs
	}
	return unicycle.KeyBy(pendingTasks, func(pt PendingTask) string { return pt.Key }), nil
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) createPendingTask(key string) error {
	result := tp.db.Create(&PendingTask{
		Key: key,
	})
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), "duplicate key value violates unique constraint") {
			pendingTask, err := tp.pendingTaskBatcher.Load(key)
			if err != nil {
				return err
			}
			if pendingTask.CreatedAt.Before(time.Now().Add(-tp.pendingTTL)) {
				return errPendingTimeout
			}
			return errPendingStarted
		}
		return result.Error
	}
	return nil
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) deletePendingTask(key string) error {
	result := tp.db.Where("key = ?", key).Delete(&PendingTask{})
	return result.Error
}

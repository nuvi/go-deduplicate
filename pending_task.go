package deduplicate

import (
	"errors"
	"strings"
	"time"
)

type PendingTask struct {
	Key       string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"default:NOW()"`
}

func (pt PendingTask) isExpired(ttl time.Duration) bool {
	return pt.CreatedAt.Before(time.Now().Add(-ttl))
}

var errPendingTimeout = errors.New("pending task timed out")
var errPendingStarted = errors.New("this task has already been started")

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) getPendingTask(keyStr string) (PendingTask, error) {
	return tp.pendingTaskBatcher.Load(keyStr)
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) createPendingTask(keyStr string) error {
	result := tp.db.Create(&PendingTask{
		Key:       keyStr,
		CreatedAt: time.Now(),
	})
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), "duplicate key value violates unique constraint") {
			return errPendingStarted
		}
		return result.Error
	}
	return nil
}

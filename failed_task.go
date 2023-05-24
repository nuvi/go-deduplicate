package deduplicate

import (
	"errors"
	"log"
	"time"

	"github.com/preston-wagner/go-dataloader"
)

type FailedTask struct {
	Key         string    `gorm:"primaryKey"`
	CreatedAt   time.Time `gorm:"default:NOW()"`
	ErrorString string
}

func (ft FailedTask) Error() string {
	return "cached failure: " + ft.ErrorString
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) getFailedTask(keyStr string) error {
	failedTask, err := tp.failedTaskBatcher.Load(keyStr)
	if err != nil {
		if errors.Is(err, dataloader.ErrMissingResponse) {
			return nil
		}
		return err
	}
	return failedTask
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) createFailedTask(key string, prior error) error {
	failedTask := FailedTask{Key: key, ErrorString: prior.Error()}
	result := tp.db.Create(&failedTask)
	if result.Error != nil {
		return result.Error
	}

	if err := tp.deletePendingTask(key); err != nil {
		log.Println(err)
	}

	return failedTask
}

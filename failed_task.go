package deduplicate

import (
	"errors"
	"log"
	"time"

	"gorm.io/gorm"
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
	var failedTask FailedTask
	result := tp.db.Where("key = ?", keyStr).Take(&failedTask)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil
		}
		return result.Error
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

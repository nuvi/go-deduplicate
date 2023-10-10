package deduplicate

import (
	"errors"
	"log"
	"time"

	"github.com/nuvi/go-dataloader"
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

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) createFailedTask(keyStr string, prior error) {
	result := tp.db.Create(&FailedTask{
		Key:         keyStr,
		CreatedAt:   time.Now(),
		ErrorString: prior.Error(),
	})
	if result.Error != nil {
		log.Println(result.Error)
	}
}

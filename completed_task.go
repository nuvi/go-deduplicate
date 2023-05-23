package deduplicate

import (
	"encoding/json"
	"log"
	"time"

	"github.com/preston-wagner/unicycle"
)

type CompletedTask struct {
	Key       string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"default:NOW()"`
	Value     string
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) getCompletedTask(keyStr string) (VALUE_TYPE, error) {
	var completedTask CompletedTask
	result := tp.db.Where("key = ?", keyStr).Take(&completedTask)
	if result.Error != nil {
		return unicycle.ZeroValue[VALUE_TYPE](), result.Error
	} else {
		var value VALUE_TYPE
		err := json.Unmarshal([]byte(completedTask.Value), &value)
		return value, err
	}
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) createCompletedTask(key string, value VALUE_TYPE) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	completedTask := CompletedTask{Key: key, Value: string(bytes)}
	result := tp.db.Create(&completedTask)
	if result.Error != nil {
		return result.Error
	}
	if err := tp.deletePendingTask(key); err != nil {
		log.Println(err)
	}
	return nil
}

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
	completedTask, err := tp.completedTaskBatcher.Load(keyStr)
	if err != nil {
		return unicycle.ZeroValue[VALUE_TYPE](), err
	}
	var value VALUE_TYPE
	err = json.Unmarshal([]byte(completedTask.Value), &value)
	return value, err
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) createCompletedTask(keyStr string, value VALUE_TYPE) {
	bytes, err := json.Marshal(value)
	if err != nil {
		log.Println(err)
	} else {
		result := tp.db.Create(&CompletedTask{
			Key:       keyStr,
			CreatedAt: time.Now(),
			Value:     string(bytes),
		})
		if result.Error != nil {
			log.Println(result.Error)
		}
	}
}

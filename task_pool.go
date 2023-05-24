package deduplicate

import (
	"log"
	"time"

	"github.com/preston-wagner/go-dataloader"
	"github.com/preston-wagner/go-dataloader/gorm"
	"github.com/preston-wagner/unicycle"
	"gorm.io/gorm"
)

type TaskPool[KEY_TYPE comparable, VALUE_TYPE any] struct {
	db     *gorm.DB
	getter func(KEY_TYPE) (VALUE_TYPE, error)

	pendingTaskBatcher   *dataloader.QueryBatcher[string, PendingTask]
	completedTaskBatcher *dataloader.QueryBatcher[string, CompletedTask]
	failedTaskBatcher    *dataloader.QueryBatcher[string, FailedTask]

	pendingTTL time.Duration
	valueTTL   time.Duration

	getterName string
}

func NewTaskPool[KEY_TYPE comparable, VALUE_TYPE any](
	db *gorm.DB,
	getter func(KEY_TYPE) (VALUE_TYPE, error),
	pendingTTL time.Duration,
	valueTTL time.Duration,
	maxConcurrentBatches int,
	maxBatchSize int,
) (*TaskPool[KEY_TYPE, VALUE_TYPE], error) {
	err := db.AutoMigrate(
		&PendingTask{},
		&CompletedTask{},
		&FailedTask{},
	)
	if err != nil {
		return nil, err
	}

	toReturn := TaskPool[KEY_TYPE, VALUE_TYPE]{
		db:         db,
		getter:     getter,
		pendingTTL: pendingTTL,
		valueTTL:   valueTTL,
		getterName: getFunctionName(getter),
	}

	toReturn.pendingTaskBatcher = dataloader.NewQueryBatcher(
		gormLoader.GormGetter[string, PendingTask](db, "key", func(task PendingTask) string { return task.Key }),
		maxConcurrentBatches,
		maxBatchSize,
	)
	toReturn.completedTaskBatcher = dataloader.NewQueryBatcher(
		gormLoader.GormGetter[string, CompletedTask](db, "key", func(task CompletedTask) string { return task.Key }),
		maxConcurrentBatches,
		maxBatchSize,
	)
	toReturn.failedTaskBatcher = dataloader.NewQueryBatcher(
		gormLoader.GormGetter[string, FailedTask](db, "key", func(task FailedTask) string { return task.Key }),
		maxConcurrentBatches,
		maxBatchSize,
	)

	unicycle.Repeat(toReturn.reap, valueTTL/4, true)

	return &toReturn, nil
}

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) reap() {
	expiredCutoff := time.Now().Add(-tp.valueTTL)
	dbc := tp.db.Where("key LIKE ?", tp.getterName+"-%").Where("created_at < ?", expiredCutoff).Delete(&PendingTask{})
	if dbc.Error != nil {
		log.Printf("error clearing expired pending task cache: %v", dbc.Error)
	}
	dbc = tp.db.Where("key LIKE ?", tp.getterName+"-%").Where("created_at < ?", expiredCutoff).Delete(&CompletedTask{})
	if dbc.Error != nil {
		log.Printf("error clearing expired completed task cache: %v", dbc.Error)
	}
	dbc = tp.db.Where("key LIKE ?", tp.getterName+"-%").Where("created_at < ?", expiredCutoff).Delete(&FailedTask{})
	if dbc.Error != nil {
		log.Printf("error clearing expired failed task cache: %v", dbc.Error)
	}
}

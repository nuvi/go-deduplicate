package deduplicate

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nuvi/go-dockerdb"
	"github.com/preston-wagner/unicycle"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type SlowInput struct {
	ID string
}

type SlowOutput struct {
	ID      string
	Name    string
	OtherId int
}

func slowTask(input SlowInput) (SlowOutput, error) {
	time.Sleep(time.Second * 3)
	otherId, err := strconv.Atoi(input.ID)
	if err != nil {
		return SlowOutput{}, err
	}
	name := strconv.Itoa(rand.Int())
	return SlowOutput{
		ID:      input.ID,
		Name:    name,
		OtherId: otherId,
	}, nil
}

func deduplicationTester[KEY_TYPE comparable, VALUE_TYPE any](t *testing.T, getter func(KEY_TYPE) (VALUE_TYPE, error)) func(KEY_TYPE) (VALUE_TYPE, error) {
	calledKeys := unicycle.Set[KEY_TYPE]{}
	lock := &sync.RWMutex{}
	return func(key KEY_TYPE) (VALUE_TYPE, error) {
		lock.Lock()
		if calledKeys.Has(key) {
			t.Error("deduplicationTester found that key", key, "was passed to a getter more than once!")
		} else {
			calledKeys.Add(key)
		}
		lock.Unlock()
		return getter(key)
	}
}

func TestGormGetter(t *testing.T) {
	container, connectURL := dockerdb.SetupSuite()
	defer dockerdb.StopContainer(container)

	db, err := gorm.Open(postgres.Open(connectURL), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}

	deduplicationTrackingTask := deduplicationTester(t, slowTask)

	slowTaskPool1, err := NewTaskPool(
		db,
		deduplicationTrackingTask,
		time.Second*10,
		time.Minute,
		3,
		9999,
	)
	if err != nil {
		t.Fatal(err)
	}

	// separate pool to simulate a second pod
	slowTaskPool2, err := NewTaskPool(
		db,
		deduplicationTrackingTask,
		time.Second*10,
		time.Minute,
		3,
		9999,
	)
	if err != nil {
		t.Fatal(err)
	}

	// test that the "query" is only made once
	promissories := unicycle.AwaitAll(
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool1.Load(SlowInput{ID: "7"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool2.Load(SlowInput{ID: "7"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool1.Load(SlowInput{ID: "7"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool2.Load(SlowInput{ID: "7"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			time.Sleep(time.Second)
			return slowTaskPool1.Load(SlowInput{ID: "7"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			time.Sleep(time.Second * 2)
			return slowTaskPool2.Load(SlowInput{ID: "7"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			time.Sleep(time.Second * 4)
			return slowTaskPool1.Load(SlowInput{ID: "7"})
		}),
	)

	for _, prm := range promissories {
		if prm.Err != nil {
			t.Fatal(err)
		}
	}

	assert.Equal(t, promissories[0].Value.ID, "7")
	assert.Equal(t, promissories[0].Value.OtherId, 7)
	assert.Equal(t, promissories[0].Value, promissories[1].Value)
	assert.Equal(t, promissories[0].Value, promissories[2].Value)
	assert.Equal(t, promissories[0].Value, promissories[3].Value)
	assert.Equal(t, promissories[0].Value, promissories[4].Value)

	// test that errors in the getter are returned correctly
	_, err = slowTaskPool1.Load(SlowInput{ID: "bad"})
	assert.Error(t, err)

	// test that the "query" is only made once even when it returns an error
	unicycle.AwaitAll(
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool1.Load(SlowInput{ID: "bad"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool2.Load(SlowInput{ID: "bad"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool1.Load(SlowInput{ID: "bad"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			return slowTaskPool2.Load(SlowInput{ID: "bad"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			time.Sleep(time.Second)
			return slowTaskPool1.Load(SlowInput{ID: "bad"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			time.Sleep(time.Second * 2)
			return slowTaskPool2.Load(SlowInput{ID: "bad"})
		}),
		unicycle.WrapInPromise(func() (SlowOutput, error) {
			time.Sleep(time.Second * 4)
			return slowTaskPool1.Load(SlowInput{ID: "bad"})
		}),
	)
}

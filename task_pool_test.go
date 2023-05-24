package deduplicate

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/nuvi/go-dockerdb"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
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

func TestGormGetter(t *testing.T) {
	container, connectURL := dockerdb.SetupSuite()
	defer dockerdb.StopContainer(container)

	db, err := gorm.Open(postgres.Open(connectURL))
	if err != nil {
		t.Fatal(err)
	}

	slowTaskPool, err := NewTaskPool(
		db,
		slowTask,
		time.Second*10,
		time.Minute,
		3,
		9999,
	)
	if err != nil {
		t.Fatal(err)
	}

	// test that the "query" is only made once
	res1, err := slowTaskPool.Load(SlowInput{ID: "7"})
	if err != nil {
		t.Fatal(err)
	}
	res2, err := slowTaskPool.Load(SlowInput{ID: "7"})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, res1, res2)

	// test that errors in the getter are returned correctly
	_, err = slowTaskPool.Load(SlowInput{ID: "bad"})
	assert.Error(t, err)
}

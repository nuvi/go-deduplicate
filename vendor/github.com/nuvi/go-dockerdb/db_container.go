package dockerdb

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func StopContainer(container testcontainers.Container) {
	oneSecond := time.Second
	container.Stop(context.Background(), &oneSecond)
}

func SetupSuite() (testcontainers.Container, string) {
	const postgresDockerImage = "postgres:14.1-alpine" // if we need to pin a specific image
	const dbname = "postgres"

	port := "5432/tcp"
	var env = map[string]string{
		"POSTGRES_PASSWORD": "password",
		"POSTGRES_USER":     "postgres",
		"POSTGRES_DB":       dbname,
	}
	dbURL := func(host string, port nat.Port) string {
		return fmt.Sprintf("postgres://postgres:password@%s:%s/%s?sslmode=disable", host, port.Port(), dbname)
	}
	req := testcontainers.ContainerRequest{
		Image:        postgresDockerImage,
		ExposedPorts: []string{port},
		Cmd:          []string{"postgres", "-c", "fsync=off"},
		Env:          env,
		WaitingFor: wait.ForSQL(nat.Port(port), "postgres", dbURL).WithStartupTimeout(time.Second * 5).
			WithQuery("SELECT 10"),
	}

	postgresContainer, err := testcontainers.GenericContainer(context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
	if err != nil {
		log.Fatalf("error creating postgres container: %v", err)
	}

	host, err := postgresContainer.Endpoint(context.Background(), "")
	if err != nil {
		log.Fatalf("error setting up test suite, could not get the DB host from the container, err: %v", err)
	}
	hostPort := strings.Split(host, ":")
	if len(hostPort) < 2 {
		log.Fatalf("error continer endpoint returned non-host:port format host, err: %v", err)
	}
	connectURL := dbURL(hostPort[0], nat.Port(hostPort[1]))

	return postgresContainer, connectURL
}

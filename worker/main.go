package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/benmanns/goworker"
)

// main ..
func main() {
	workerSettings := goworker.WorkerSettings{
		URI:            "redis://redis:6379",
		Connections:    2,
		Queues:         []string{"increment"},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    1,
		Namespace:      "increment:",
		IntervalFloat:  1.0,
	}
	goworker.SetSettings(workerSettings)
	goworker.Register("Increment", incrementWorker)
	log.Println("start go worker")
	go func() {
		if err := goworker.Work(); err != nil {
			log.Println("failed to start worker: ", err)
		}
	}()
	time.Sleep(15 * time.Second)
	log.Println("kill go worker")
	err := exec.Command("kill", "1").Run()
	if err != nil {
		log.Println("Failed to kill: ", err)
	}
	time.Sleep(3 * time.Second)
}

func incrementWorker(queue string, args ...interface{}) error {
	osSignalCh := make(chan os.Signal, 1)
	signal.Notify(osSignalCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer signal.Stop(osSignalCh)

	jsonCount, ok := args[0].(json.Number)
	if !ok {
		log.Println("invalid parameter: count")
		return errors.New("invalid parameter: count")
	}
	int64Count, err := jsonCount.Int64()
	if err != nil {
		return err
	}
	count := (int)(int64Count)
	log.Println("start increment task: count = ", count)

	done := make(chan error)
	go func() {
		err := incrementWithRecover(&count)
		done <- err
	}()

	for {
		select {
		case killSignal := <-osSignalCh:
			switch killSignal {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				// Requeue
				log.Println("Requeue the task: count=", count)
				if err := goworker.Enqueue(&goworker.Job{
					Queue: "increment",
					Payload: goworker.Payload{
						Class: "Increment",
						Args:  []interface{}{count},
					},
				}); err != nil {
					log.Println("enqueue failed: ", err)
				}

				return errors.New("received kill signal")
			}
		case err := <-done:
			return err
		}
	}
}

func incrementWithRecover(count *int) (e error) {

	defer func() {
		if err := recover(); err != nil {
			err, _ := err.(error)
			log.Println("recovered: ", err)
			e = err
		}
	}()

	for {
		log.Println("count: ", *count)
		*count++
		time.Sleep(time.Second)
	}
}

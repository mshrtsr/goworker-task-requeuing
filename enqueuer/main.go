package main

import (
	"log"
	"time"
	"github.com/benmanns/goworker"
)


func main(){
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
	const number = 0
	time.Sleep(5*time.Second)
	log.Println("enqueue the task: number=", number)
	if err := goworker.Enqueue(&goworker.Job{
		Queue: "increment",
		Payload: goworker.Payload{
			Class: "Increment",
			Args:  []interface{}{number},
		},
	}); err != nil {
		log.Println("enqueue failed: ", err)
	}

}
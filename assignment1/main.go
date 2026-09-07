package main

// Deadlock prevention: We are removing the fourth Coffman necessary condition for a deadlock, Circular wait.
// We do this by having all philosophers start reaching for their left fork, except one that first reaches for the right one.

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

var globalEatCount int = 0

type ForkRequest struct {
	philosopher string
	reply_ch    chan bool
	finished_ch chan bool
}

func philospher(name string, firstForkChoice_ch chan ForkRequest, secondForkChoice_ch chan ForkRequest) {
	eatCount := 0
	for {

		// random 1;5
		think_time := rand.Intn(5) + 1
		fmt.Printf("%s is thinking for %d seconds\n", name, think_time)
		time.Sleep(time.Duration(think_time) * time.Second)

		replyFirstForkChoice_ch := make(chan bool)
		finishedFirstForkChoice_ch := make(chan bool)
		requestFirstForkChoice := ForkRequest{name, replyFirstForkChoice_ch, finishedFirstForkChoice_ch}

		firstForkChoice_ch <- requestFirstForkChoice
		<-replyFirstForkChoice_ch

		replySecondForkChoice_ch := make(chan bool)
		finishedSecondForkChoice_ch := make(chan bool)
		requestSecondForkChoice := ForkRequest{name, replySecondForkChoice_ch, finishedSecondForkChoice_ch}

		secondForkChoice_ch <- requestSecondForkChoice
		<-replySecondForkChoice_ch

		eat_time := rand.Intn(5) + 1
		fmt.Printf("%s is eating for %d seconds\n", name, eat_time)
		time.Sleep(time.Duration(eat_time) * time.Second)
		eatCount++
		//fmt.Println(name + " " + strconv.Itoa(eatCount))
		if eatCount == 3 {
			globalEatCount++
		}
		if globalEatCount == 5 {
			fmt.Println("All philosophers have eaten at least three times!")
			os.Exit(0)
		}

		finishedFirstForkChoice_ch <- true
		finishedSecondForkChoice_ch <- true
	}

}
func fork(name string, fork_ch chan ForkRequest) {
	for {
		req := <-fork_ch
		fmt.Println(name + "got request from Philosoper " + req.philosopher)
		req.reply_ch <- true
		fmt.Println(name + " granted access to " + req.philosopher)
		<-req.finished_ch

	}
}

func main() {

	fork1_ch := make(chan ForkRequest)
	fork2_ch := make(chan ForkRequest)
	fork3_ch := make(chan ForkRequest)
	fork4_ch := make(chan ForkRequest)
	fork5_ch := make(chan ForkRequest)

	go philospher("Philosopher1", fork1_ch, fork2_ch)
	go philospher("Philosopher2", fork2_ch, fork3_ch)
	go philospher("Philosopher3", fork3_ch, fork4_ch)
	go philospher("Philosopher4", fork4_ch, fork5_ch)
	go philospher("Philosopher5", fork1_ch, fork5_ch)
	go fork("Fork1", fork1_ch)
	go fork("Fork2", fork2_ch)
	go fork("Fork3", fork3_ch)
	go fork("Fork4", fork4_ch)
	go fork("Fork5", fork5_ch)

	for {
	}

}

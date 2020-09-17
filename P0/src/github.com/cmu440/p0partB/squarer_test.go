// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

func TestYourFirstGoTest(t *testing.T) {
	fmt.Println("Running TestYourFirstGoTest.")
	input := make(chan int, 1)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)

	//routine sends 100 ints to the channel
	go func() {
		for i := 1; i <= 100; i++ {
			input <- i
		}
	}()

	//test receives only 50 squares and then closes the squarer
	for i := 1; i <= 50; i++ {
		timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
		select {
		case <-timeoutChan:
			t.Error("Test timed out.")
		case <-squares:
		}
	}
	sq.Close()

	//test receives square even after closing the squarer
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
	case <-squares:
		t.Error("Bad Squarer")
	}
}

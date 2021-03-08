package raft

import (
	"log"
	"math"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Mini(nums... int) int {
	min := math.MaxInt32
	for _, n := range nums {
		if n < min {
			min = n
		}
	}
	return min
}

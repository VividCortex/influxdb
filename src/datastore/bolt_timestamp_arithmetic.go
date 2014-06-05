package datastore

import (
	"math"
)

func utoi(i uint64) int64 {
	return int64(i - 1 - math.MaxInt64)

}

func itou(i int64) uint64 {
	return uint64(i) + math.MaxInt64 + 1
}

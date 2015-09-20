package transaction

import (
	"errors"
	"math"
	"sort"
	"time"
)

//returns the average milliseconds of the durations
func calculateAverageOffset(tripTimes []time.Duration, offsets []time.Duration) (float64, error) {
	if len(tripTimes) == 0 {
		return 0, errors.New("finalizing transaction with 0 iterations")
	}
	if len(tripTimes) != len(offsets) {
		return 0, errors.New("finalizing transaction with invalid iterations")
	}

	maxNanosecs := math.MaxFloat64
	//first we need to calculate the 80th percentile of the tripTimes
	//we only want to keep track of those and discard the others
	if len(tripTimes) > 2 {
		sortedTimes := make([]float64, len(tripTimes))
		for i, v := range tripTimes {
			sortedTimes[i] = float64(v.Nanoseconds())
		}
		sort.Float64s(sortedTimes)
		percentIndex := int64(float64(len(sortedTimes)) * 0.8)
		maxNanosecs = sortedTimes[percentIndex]
	}
	var n float64
	var totalTimes float64
	var totalOffsets float64
	count := 0.0
	for i, v := range tripTimes {
		n = float64(v.Nanoseconds())
		//only accept this trip if its less than the max allowed time
		if n < maxNanosecs {
			totalTimes += n / 1000000
			totalOffsets += float64(offsets[i]) / 1000000
			count++
		}
	}
	//totalTimes is the total of all the RTTs but offset is only affected 1 way
	//so divide RTT by 2 to get one-way time
	return (totalOffsets + (totalTimes / 2)) / count, nil
}

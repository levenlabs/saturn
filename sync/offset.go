package sync

import (
	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
	"math"
	"sort"
	"time"
)

//returns the average milliseconds of the durations
func calculateAverageOffset(tripTimes []time.Duration, offsets []int64) float64 {
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
	return (totalOffsets + (totalTimes / 2)) / count
}

func calcOffsetForTransaction(t *Transaction) {
	if len(t.TripTimes) == 0 {
		llog.Error("finalizing transaction with 0 iterations", llog.KV{"src": t.Addr})
		return
	}
	if len(t.TripTimes) != len(t.Offsets) {
		llog.Error("finalizing transaction with invalid iterations", llog.KV{
			"trips":   len(t.TripTimes),
			"offsets": len(t.Offsets),
			"src":     t.Addr,
		})
		return
	}
	offsetInMS := calculateAverageOffset(t.TripTimes, t.Offsets)
	kv := llog.KV{"name": t.Name, "offset": offsetInMS}
	llog.Info("slave offset", kv)

	if config.Threshold < math.Abs(offsetInMS) {
		llog.Warn("slave offset is over threshold", kv)
	}
}

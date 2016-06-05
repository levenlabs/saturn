package transaction

import (
	"errors"
	"math"
	"sort"
)

type Trip struct {
	MasterDiff int64
	SlaveDiff  int64
	RTT        int64
}

type int64s []int64

func (a int64s) Len() int           { return len(a) }
func (a int64s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64s) Less(i, j int) bool { return a[i] < a[j] }

// returns the average milliseconds of the durations
// and also includes the average latency in milliseconds
func calculateAverageOffsetLatency(trips []Trip) (float64, float64, error) {
	if len(trips) == 0 {
		return 0, 0, errors.New("finalizing transaction with 0 trips")
	}

	var maxNanosecs int64 = math.MaxInt64
	//first we need to calculate the 80th percentile of the trips
	//we only want to keep track of those and discard the others
	if len(trips) > 2 {
		sortedTimes := make(int64s, len(trips))
		for i, t := range trips {
			sortedTimes[i] = t.RTT
		}
		sort.Sort(sortedTimes)
		// make sure we have at most the length of the trips - 1 and at least 3
		percentIndex := int64(math.Min(float64(len(trips))-1, math.Max(3, (float64(len(sortedTimes))*0.6))))
		maxNanosecs = sortedTimes[percentIndex]
	}
	var latency float64
	var offset float64
	var totalLatency float64
	var totalOffsets float64
	var count float64
	for _, t := range trips {
		//ignore trips that are greater than max allowed time
		if t.RTT >= maxNanosecs {
			continue
		}
		// assume the latency is half the RTT
		latency = float64(t.RTT) / 2.0
		// the Slave will have the opposite offset (since it's subtracting
		// its time minus the master's) so we negate it here
		// divide by 2 to get the average
		offset = ((float64(t.MasterDiff) + latency) - (float64(t.SlaveDiff) + latency)) / 2.0
		totalOffsets += float64(offset) / 1e6
		totalLatency += latency / 1e6
		count++
	}
	return totalOffsets / count, totalLatency / count, nil
}

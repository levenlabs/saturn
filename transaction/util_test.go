package transaction

import (
	. "testing"

	"github.com/stretchr/testify/assert"
)

func TestCalcAvg1(t *T) {
	trips := []Trip{
		// offset: 10 latency: 1
		Trip{
			MasterDiff: 9000000,
			SlaveDiff:  -11000000,
			RTT:        2000000,
		},
	}
	o, l, err := calculateAverageOffsetLatency(trips)
	assert.Nil(t, err)
	assert.Equal(t, float64(10), o)
	assert.Equal(t, float64(1), l)
}

func TestCalcAvg2(t *T) {
	trips := []Trip{
		// offset: 10 latency: 1
		Trip{
			MasterDiff: 9000000,
			SlaveDiff:  -11000000,
			RTT:        2000000,
		},

		// offset: 20 latency: 1
		Trip{
			MasterDiff: 19000000,
			SlaveDiff:  -21000000,
			RTT:        2000000,
		},
	}
	o, l, err := calculateAverageOffsetLatency(trips)
	assert.Nil(t, err)
	assert.Equal(t, float64(15), o)
	assert.Equal(t, float64(1), l)
}

func TestCalcAvg3(t *T) {
	trips := []Trip{
		// offset: 10 latency: 1
		Trip{
			MasterDiff: 9000000,
			SlaveDiff:  -11000000,
			RTT:        2000000,
		},

		// offset: 20 latency: 1
		Trip{
			MasterDiff: 19000000,
			SlaveDiff:  -21000000,
			RTT:        2000000,
		},

		// offset: 40 latency: 2
		// this should be eliminated
		Trip{
			MasterDiff: 38000000,
			SlaveDiff:  -32000000,
			RTT:        4000000,
		},
	}
	o, l, err := calculateAverageOffsetLatency(trips)
	assert.Nil(t, err)
	assert.Equal(t, float64(15), o)
	assert.Equal(t, float64(1), l)
}

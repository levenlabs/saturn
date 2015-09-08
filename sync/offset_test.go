package sync

import (
	"github.com/stretchr/testify/assert"
	. "testing"
	"time"
)

func TestCalcAvg1(t *T) {
	d := []time.Duration{
		time.Duration(2) * time.Millisecond,
	}
	o := []int64{
		1000000,
	}
	a := calculateAverageOffset(d, o)
	assert.Equal(t, float64(2), a)
}

func TestCalcAvg2(t *T) {
	d := []time.Duration{
		time.Duration(1) * time.Millisecond,
		time.Duration(1) * time.Millisecond,
	}
	o := []int64{
		1000000,
		1000000,
	}
	a := calculateAverageOffset(d, o)
	assert.Equal(t, float64(1.5), a)
}

func TestCalcAvg3(t *T) {
	//the outlier (2) should be eliminated so this should match TestCalcAvg2
	d := []time.Duration{
		time.Duration(1) * time.Millisecond,
		time.Duration(1) * time.Millisecond,
		time.Duration(2) * time.Millisecond,
	}
	o := []int64{
		1000000,
		1000000,
		9000000,
	}
	a := calculateAverageOffset(d, o)
	assert.Equal(t, float64(1.5), a)
}

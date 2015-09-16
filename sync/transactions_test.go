package sync

import (
	"github.com/levenlabs/saturn/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	. "testing"
	"time"
)

var addr, _ = net.ResolveUDPAddr("udp", "127.0.0.1:1234")
var addr2, _ = net.ResolveUDPAddr("udp", "127.0.0.2:1234")

func TestSignVerifyReport(t *T) {
	now := time.Now()
	req := makeReport(now)
	assert.True(t, verifyReport(req))
	assert.Equal(t, int32(1), req.Seq)
}

func TestSignVerifyResponse(t *T) {
	now := time.Now()
	trans := generateTransactionID(addr.IP)
	resp := makeResponse(now, 0, trans, 1)
	assert.True(t, verifyResponse(resp))
	assert.Equal(t, int32(2), resp.Seq)
	assert.Equal(t, trans, resp.Trans)
}

func TestCleanup(t *T) {
	tr := InitTransaction(addr, nil)
	tr.Track()
	assert.Equal(t, 1, len(transactions))
	tr.Close()
	assert.Equal(t, 0, len(transactions))
}

func TestTransactionStores(t *T) {
	now := time.Now()
	future := now.Add(time.Second)

	tr := InitTransaction(addr, nil)
	defer tr.Close()
	diff := future.UnixNano() - now.UnixNano()
	tr.StoreOffset(diff)
	tripTime := time.Duration(100)
	tr.StoreTripTime(tripTime)
	tr.Track()

	require.NotNil(t, tr.Offsets)
	require.NotNil(t, tr.TripTimes)
	assert.Equal(t, tr.Offsets[0], diff)
	assert.Equal(t, tr.TripTimes[0], tripTime)
}

func TestHandleReport(t *T) {
	now := time.Now()
	future := now.Add(time.Second)

	tr := InitTransaction(addr, nil)
	tr.seq = 1 //simulate reading them
	defer tr.Close()
	req := makeReport(future)
	b := handleReport(req, addr, tr)
	require.NotNil(t, b)

	assert.Equal(t, 1, len(transactions))
	v, ok := transactions[getTransactionKey(addr.IP, tr.ID)]
	assert.True(t, ok)
	require.NotNil(t, v.Offsets)
	require.Nil(t, v.TripTimes)
	assert.Len(t, v.Offsets, 1)
	assert.True(t, v.Offsets[0] > 1000)
}

func TestTransactionAging(t *T) {
	//subtract a minute so it expires
	past := time.Now().Add(time.Minute * -1)
	tr := InitTransaction(addr, nil)
	tr.LastResponse = past
	tr.Track()
	assert.Equal(t, 1, len(transactions))
	//cleanup
	removeOldTransactions()
	assert.Equal(t, 0, len(transactions))
}

func TestHandleReportName(t *T) {
	now := time.Now()

	config.Name = "Test"
	tr := InitTransaction(addr, nil)
	defer tr.Close()
	req := makeReport(now)
	tr.seq = 1 //simulate reading them
	b := handleReport(req, addr, tr)
	require.NotNil(t, b)
	assert.Equal(t, config.Name, tr.Name)

	config.Name = ""
	tr = InitTransaction(addr, nil)
	defer tr.Close()
	req = makeReport(now)
	tr.seq = 1 //simulate reading them
	b = handleReport(req, addr, tr)
	require.NotNil(t, b)
	assert.Equal(t, addr.IP.String(), tr.Name)
}

func TestHandleResponse(t *T) {
	now := time.Now()
	future := now.Add(time.Second)

	tr := InitTransaction(addr, nil)
	defer tr.Close()
	req := makeReport(future)

	//make response to report
	diff := req.Time - now.UnixNano()
	resp := makeResponse(now, diff, 1, req.Seq)
	tr.seq = 2 //simulate reading them

	b := handleResponse(resp, addr, tr)
	require.NotNil(t, b)

	assert.True(t, tr.SeqMatches(resp.Seq))
	//if Seq is out of order than it should be rejected
	assert.False(t, tr.SeqMatches(resp.Seq+1))
	assert.False(t, tr.SeqMatches(resp.Seq-1))
}

func TestMultipleIPsSameID(t *T) {
	tr1 := InitTransaction(addr, nil)
	defer tr1.Close()
	tr1.ID = 1
	tr1.Track()

	tr2 := InitTransaction(addr2, nil)
	defer tr2.Close()
	tr2.ID = 1
	tr2.Track()

	assert.Equal(t, 2, len(transactions))
}

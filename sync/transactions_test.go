package sync

import (
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
	req := makeReport(addr.IP, now)
	assert.True(t, verifyReport(req))
	assert.Equal(t, int32(1), req.Seq)
}

func TestSignVerifyResponse(t *T) {
	now := time.Now()
	trans := generateTransID(addr.IP)
	resp := makeResponse(now, 0, trans, 1)
	assert.True(t, verifyResponse(resp))
	assert.Equal(t, int32(2), resp.Seq)
	assert.Equal(t, trans, resp.Trans)
}

func TestHandleReport(t *T) {
	now := time.Now()

	//make report that's 1 second in the future
	future := now.Add(time.Second)
	req := makeReport(addr.IP, future)

	//handle report
	recordNewRequest(req, now, addr)

	assert.Equal(t, 1, len(transactions))
	tMutex.RLock()
	v, ok := transactions[getTransactionKey(addr.IP, req.Trans)]
	assert.True(t, ok)
	require.NotNil(t, v.Offsets)
	require.NotNil(t, v.TripTimes)
	assert.Equal(t, v.Offsets[0], future.UnixNano()-now.UnixNano())
	tMutex.RUnlock()

	//now cleanup
	cleanupTransaction(addr.IP, req.Trans)
	assert.Equal(t, 0, len(transactions))
}

func TestHandleReportName(t *T) {
	now := time.Now()

	//make report that's 1 second in the future
	future := now.Add(time.Second)
	req := makeReport(addr.IP, future)
	req.Name = "Test"

	//handle report
	recordNewRequest(req, now, addr)

	tMutex.RLock()
	v, ok := transactions[getTransactionKey(addr.IP, req.Trans)]
	assert.True(t, ok)
	assert.Equal(t, req.Name, v.Name)
	tMutex.RUnlock()

	//now cleanup
	cleanupTransaction(addr.IP, req.Trans)
	assert.Equal(t, 0, len(transactions))

	//make report with no name
	req = makeReport(addr.IP, future)

	//handle report
	recordNewRequest(req, now, addr)

	tMutex.RLock()
	v, ok = transactions[getTransactionKey(addr.IP, req.Trans)]
	assert.True(t, ok)
	assert.Equal(t, addr.IP.String(), v.Name)
	tMutex.RUnlock()
}

func TestHandleResponse(t *T) {
	now := time.Now()

	//make report that's 1 second in the future
	future := now.Add(time.Second)
	req := makeReport(addr.IP, future)

	//handle report
	recordNewRequest(req, now, addr)

	//make response to report
	diff := req.Time - now.UnixNano()
	resp := makeResponse(now, diff, req.Trans, req.Seq)

	//verify response
	assert.NotNil(t, verifyExistingTransaction(resp, addr))

	//if IP is different than it should be rejected
	assert.Nil(t, verifyExistingTransaction(resp, addr2))

	//if Seq is out of order than it should be rejected
	resp.Seq = resp.Seq + 1
	assert.Nil(t, verifyExistingTransaction(resp, addr))
	resp.Seq = resp.Seq - 1

	//if random transactionID it should be rejected
	resp.Trans = generateTransID(addr.IP)
	assert.Nil(t, verifyExistingTransaction(resp, addr))
	resp.Trans = req.Trans

	//now cleanup
	cleanupTransaction(addr.IP, req.Trans)
	assert.Equal(t, 0, len(transactions))
}

func TestMultipleTransactionIDs(t *T) {
	now := time.Now()

	//make report that's 1 second in the future
	future := now.Add(time.Second)
	req := makeReport(addr.IP, future)

	//handle report
	recordNewRequest(req, now, addr)
	//handle report from different IP
	recordNewRequest(req, now, addr2)

	assert.Equal(t, 2, len(transactions))

	//now cleanup
	cleanupTransaction(addr.IP, req.Trans)
	cleanupTransaction(addr2.IP, req.Trans)
	assert.Equal(t, 0, len(transactions))
}

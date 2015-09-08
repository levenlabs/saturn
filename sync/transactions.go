package sync

import (
	"crypto/hmac"
	"crypto/sha256"
	"github.com/golang/protobuf/proto"
	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
	. "github.com/levenlabs/saturn/proto"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var macSep = []byte(":")
var transactions = map[string]*Transaction{}
var tMutex = &sync.RWMutex{}

type Transaction struct {
	IP           net.IP
	LastSeq      int32
	TripTimes    []time.Duration
	LastResponse time.Time
	Offsets      []int64
}

func getTransactionKey(ip net.IP, t int32) string {
	return strings.Join([]string{ip.String(), strconv.FormatInt(int64(t), 36)}, "|")
}

func cleanupTransaction(ip net.IP, trans int32) {
	tMutex.Lock()
	delete(transactions, getTransactionKey(ip, trans))
	tMutex.Unlock()
}

func transactionExists(ip net.IP, trans int32) bool {
	tMutex.RLock()
	_, ok := transactions[getTransactionKey(ip, trans)]
	defer tMutex.RUnlock()
	return ok
}

func generateTransID(ip net.IP) int32 {
	var n int32
	var ok bool
	tMutex.RLock()
	defer tMutex.RUnlock()
	for {
		n = rand.Int31()
		//make sure this ID doesn't already exist
		if _, ok = transactions[getTransactionKey(ip, n)]; !ok {
			break
		}
	}
	return n
}

func signReport(req *ReportRequest) []byte {
	mac := hmac.New(sha256.New, config.HMACKey)
	mac.Write([]byte(strconv.FormatInt(req.Time, 36)))
	mac.Write(macSep)
	mac.Write([]byte(req.Name))
	mac.Write(macSep)
	mac.Write([]byte(req.Reply))
	mac.Write(macSep)
	mac.Write([]byte(strconv.FormatInt(int64(req.Trans), 36)))
	mac.Write(macSep)
	mac.Write([]byte(strconv.FormatInt(int64(req.Seq), 36)))
	return mac.Sum(nil)
}

func signResponse(resp *ReportResponse) []byte {
	mac := hmac.New(sha256.New, config.HMACKey)
	mac.Write([]byte(strconv.FormatInt(resp.Diff, 36)))
	mac.Write(macSep)
	mac.Write([]byte(strconv.FormatInt(resp.Time, 36)))
	mac.Write(macSep)
	mac.Write([]byte(resp.Reply))
	mac.Write(macSep)
	mac.Write([]byte(strconv.FormatInt(int64(resp.Trans), 36)))
	mac.Write(macSep)
	mac.Write([]byte(strconv.FormatInt(int64(resp.Seq), 36)))
	return mac.Sum(nil)
}

func verifyReport(req *ReportRequest) bool {
	return hmac.Equal(signReport(req), req.Sig)
}

func verifyResponse(resp *ReportResponse) bool {
	return hmac.Equal(signResponse(resp), resp.Sig)
}

func send(t byte, addr *net.UDPAddr, d []byte) {
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		llog.Error("dial error in SendReport", llog.KV{"err": err})
		return
	}
	defer c.Close()
	_, err = c.Write(append([]byte{t}, d...))
	if err != nil {
		llog.Error("write error in SendReport", llog.KV{"err": err})
	}
}

func sendResponse(resp *ReportResponse, reply string, srcAddr *net.UDPAddr) error {
	destAddr, err := correctReply(reply, srcAddr)
	if err != nil {
		return err
	}

	llog.Debug("send resp", llog.KV{"dest": destAddr, "seq": resp.Seq, "diff": resp.Diff})
	d, err := proto.Marshal(resp)
	if err != nil {
		return err
	}

	tMutex.Lock()
	t, ok := transactions[getTransactionKey(srcAddr.IP, resp.Trans)]
	if !ok {
		llog.Warn("sending response without a transaction ", llog.KV{"dest": destAddr, "seq": resp.Seq})
	} else {
		t.LastSeq = resp.Seq
	}
	tMutex.Unlock()

	send(RESPONSE, destAddr, d)
	return nil
}

func makeReport(src net.IP, now time.Time) *ReportRequest {
	req := &ReportRequest{
		Time:  now.UnixNano(),
		Name:  config.Name,
		Trans: generateTransID(src),
		Seq:   1,
		Reply: config.ListenAddr,
	}
	req.Sig = signReport(req)
	return req
}

func makeResponse(now time.Time, diff int64, trans int32, lastSeq int32) *ReportResponse {
	resp := &ReportResponse{
		Diff:  diff,
		Time:  now.UnixNano(),
		Reply: config.ListenAddr,
		Trans: trans,
		Seq:   lastSeq + 1,
	}
	resp.Sig = signResponse(resp)
	return resp
}

func startTransaction(trans int32, srcAddr *net.UDPAddr, now time.Time) {
	t := &Transaction{
		IP:           srcAddr.IP,
		LastSeq:      1,
		LastResponse: now,
	}
	tMutex.RLock()
	defer tMutex.RUnlock()
	k := getTransactionKey(srcAddr.IP, trans)
	transactions[k] = t
	go func(t *Transaction, k string) {
		for {
			//timeout all transactions 1 minute after the last response
			time.Sleep(t.LastResponse.Add(time.Minute).Sub(time.Now()))
			if time.Now().Sub(t.LastResponse).Minutes() < 1.0 {
				continue
			}
			tMutex.RLock()
			t2, ok := transactions[k]
			tMutex.RUnlock()
			//delete it from the map only if the pointer is the same
			if ok && t2 == t {
				//this should *always* happen for the
				tMutex.Lock()
				delete(transactions, k)
				tMutex.Unlock()
			}
			break
		}
	}(t, k)
}

func recordNewRequest(req *ReportRequest, now time.Time, srcAddr *net.UDPAddr) {
	startTransaction(req.Trans, srcAddr, now)
	//now create the TripTimes array to include the estimated number of triptimes
	tMutex.RLock()
	t, _ := transactions[getTransactionKey(srcAddr.IP, req.Trans)]
	defer tMutex.RUnlock()
	t.TripTimes = make([]time.Duration, 0, config.Iterations+1)
	t.Offsets = make([]int64, 0, config.Iterations+1)
	//store the first offset and we'll calculate the trip time when we get the first response
	diff := req.Time - now.UnixNano()
	t.Offsets = append(t.Offsets, diff)
}

func correctReply(replyAddr string, srcAddr *net.UDPAddr) (*net.UDPAddr, error) {
	//determine the addr to send based on the received message
	destAddr, err := net.ResolveUDPAddr("udp", replyAddr)
	if err != nil {
		return nil, err
	}
	//if they're listening on all interfaces then use the srcAddr
	if destAddr.IP.IsUnspecified() {
		destAddr.IP = srcAddr.IP
	}
	return destAddr, nil
}

func verifyExistingTransaction(resp *ReportResponse, srcAddr *net.UDPAddr) *Transaction {
	tMutex.RLock()
	t, ok := transactions[getTransactionKey(srcAddr.IP, resp.Trans)]
	defer tMutex.RUnlock()
	if !ok {
		llog.Warn("received message for old transaction", llog.KV{
			"addr":        srcAddr,
			"seq":         resp.Seq,
			"transaction": resp.Trans,
		})
		return nil
	}
	if !t.IP.Equal(srcAddr.IP) {
		llog.Warn("mid-transaction IP change detected", llog.KV{
			"addr":        srcAddr,
			"origAddr":    t.IP.String(),
			"transaction": resp.Trans,
		})
		return nil
	}
	if t.LastSeq+1 != resp.Seq {
		llog.Warn("mid-transaction seq out of order", llog.KV{
			"addr":        srcAddr,
			"seq":         resp.Seq,
			"expectedSeq": (t.LastSeq + 1),
			"transaction": resp.Trans,
		})
		return nil
	}
	return t
}

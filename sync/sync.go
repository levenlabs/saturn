// The sync package handles all the syncronization between clients
package sync

import (
	"crypto/hmac"
	"crypto/sha256"
	"github.com/golang/protobuf/proto"
	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
	. "github.com/levenlabs/saturn/proto"
	"math"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

var lastTransID int32
var macSep = []byte(":")
var defaultReq = &ReportRequest{}
var defaultResp = &ReportResponse{}

var transactions = map[int32]*Transaction{}
var tMutex = &sync.RWMutex{}

const (
	REPORT   = byte(2)
	RESPONSE = byte(3)
)

type Transaction struct {
	IP           net.IP
	LastSeq      int32
	TripTimes    []time.Duration
	LastResponse time.Time
	Offsets      []int64
}

//todo: in reality we should check the last N reqs
func generateTransID() int32 {
	var n int32
	for {
		n = rand.Int31()
		if n != lastTransID {
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

func startTransaction(id int32, srcAddr *net.UDPAddr, now time.Time) {
	t := &Transaction{
		IP:           srcAddr.IP,
		LastSeq:      1,
		LastResponse: now,
	}
	tMutex.RLock()
	defer tMutex.RUnlock()
	transactions[id] = t
	go func(t *Transaction, id int32) {
		for {
			//timeout all transactions 1 minute after the last response
			time.Sleep(t.LastResponse.Add(time.Minute).Sub(time.Now()))
			if time.Now().Sub(t.LastResponse).Minutes() < 1.0 {
				continue
			}
			tMutex.RLock()
			t2, ok := transactions[id]
			tMutex.RUnlock()
			//delete it from the map only if the pointer is the same
			if ok && t2 == t {
				//this should *always* happen for the
				tMutex.Lock()
				delete(transactions, id)
				tMutex.Unlock()
			}
			break
		}
	}(t, id)
}

func SendReport(serverAddr *net.UDPAddr) {
	now := time.Now()
	req := &ReportRequest{
		Time:  now.UnixNano(),
		Name:  config.Name,
		Trans: generateTransID(),
		Seq:   1,
		Reply: config.ListenAddr,
	}
	req.Sig = signReport(req)

	d, err := proto.Marshal(req)
	if err != nil {
		llog.Error("marshal error in SendReport", llog.KV{"err": err})
		return
	}
	startTransaction(req.Trans, serverAddr, now)
	send(REPORT, serverAddr, d)
}

func HandleMessage(d []byte, srcAddr *net.UDPAddr) {
	if len(d) < 2 {
		return
	}
	var msg proto.Message
	var fn func(proto.Message, *net.UDPAddr)
	switch d[0] {
	case REPORT:
		msg = defaultReq
		fn = HandleReport
	case RESPONSE:
		msg = defaultResp
		fn = HandleResponse
	default:
		llog.Warn("received invalid first byte", llog.KV{"byte": d[0], "src": srcAddr})
		return
	}
	err := proto.Unmarshal(d[1:], msg)
	if err != nil {
		llog.Warn("unmarshaling error", llog.KV{"err": err, "src": srcAddr})
		return
	}
	fn(msg, srcAddr)
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

func sendResponse(diff int64, trans int32, seq int32, reply string, srcAddr *net.UDPAddr) error {
	destAddr, err := correctReply(reply, srcAddr)
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	resp := &ReportResponse{
		Diff:  diff,
		Time:  now,
		Reply: config.ListenAddr,
		Trans: trans,
		Seq:   seq + 1,
	}
	resp.Sig = signResponse(resp)

	llog.Debug("send resp", llog.KV{"dest": destAddr, "seq": resp.Seq, "diff": resp.Diff})
	d, err := proto.Marshal(resp)
	if err != nil {
		return err
	}

	tMutex.Lock()
	t, ok := transactions[trans]
	if !ok {
		llog.Warn("sending response without a transaction ", llog.KV{"dest": destAddr, "seq": resp.Seq})
	} else {
		t.LastSeq = resp.Seq
	}
	tMutex.Unlock()

	send(RESPONSE, destAddr, d)
	return nil
}

func HandleReport(msg proto.Message, srcAddr *net.UDPAddr) {
	req, ok := msg.(*ReportRequest)
	if !ok {
		return
	}
	llog.Debug("received req", llog.KV{"req": req, "addr": srcAddr})
	if !verifyReport(req) {
		llog.Warn("received unsigned report", llog.KV{"addr": srcAddr})
		return
	}

	now := time.Now()
	diff := req.Time - now.UnixNano()
	tMutex.RLock()
	_, ok = transactions[req.Trans]
	tMutex.RUnlock()
	if ok {
		llog.Warn("received duplicate report transactionID", llog.KV{"addr": srcAddr})
		return
	}
	if req.Seq != 1 {
		llog.Warn("received report with 1 seq other than 1", llog.KV{"addr": srcAddr, "seq": req.Seq})
		return
	}
	startTransaction(req.Trans, srcAddr, now)
	//now create the TripTimes array to include the estimated number of triptimes
	tMutex.RLock()
	t, _ := transactions[req.Trans]
	tMutex.RUnlock()
	t.TripTimes = make([]time.Duration, 0, config.Iterations+1)
	t.Offsets = make([]int64, 0, config.Iterations+1)
	//store the first offset and we'll calculate the trip time when we get the first response
	t.Offsets = append(t.Offsets, diff)

	err := sendResponse(diff, req.Trans, req.Seq, req.Reply, srcAddr)
	if err != nil {
		llog.Error("sendResponse error in HandleReport", llog.KV{"err": err})
	}
}

func verifyExistingTransaction(resp *ReportResponse, srcAddr *net.UDPAddr) *Transaction {
	tMutex.RLock()
	t, ok := transactions[resp.Trans]
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

func HandleResponse(msg proto.Message, srcAddr *net.UDPAddr) {
	resp, ok := msg.(*ReportResponse)
	if !ok {
		return
	}
	llog.Debug("received resp", llog.KV{"resp": resp, "addr": srcAddr})
	if !verifyResponse(resp) {
		llog.Warn("received unsigned response", llog.KV{"addr": srcAddr})
		return
	}

	now := time.Now()
	diff := resp.Time - now.UnixNano()
	t := verifyExistingTransaction(resp, srcAddr)
	if t == nil {
		return
	}
	//we're writing to the transaction but we're including that in this mutex
	tMutex.Lock()
	//only the master should be recording the TripTimes so only do this if its not null
	if t.TripTimes != nil {

		//if this is the 3rd packet then we need to include the RTT for the first offest
		if resp.Seq == 3 && len(t.Offsets) == 1 {
			t.TripTimes = append(t.TripTimes, time.Duration(resp.Diff-t.Offsets[0]))
		}

		t.TripTimes = append(t.TripTimes, now.Sub(t.LastResponse))
		t.Offsets = append(t.Offsets, diff)
	}
	t.LastSeq = resp.Seq
	t.LastResponse = now
	tMutex.Unlock()

	//first trip is free and then each iteration is 2 trips
	//seq starts at 1 so after 1 iteration it'll be at 3
	//only the master can terminate a sequence
	if config.IsMaster && (resp.Seq/2) >= config.Iterations {
		finalizeTransaction(resp.Trans)
		return
	}

	err := sendResponse(diff, resp.Trans, resp.Seq, resp.Reply, srcAddr)
	if err != nil {
		llog.Error("sendResponse error in HandleReport", llog.KV{"err": err})
	}
}

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

func finalizeTransaction(id int32) {
	tMutex.RLock()
	t, ok := transactions[id]
	defer func(id int32) {
		tMutex.RUnlock()
		cleanupTransaction(id)
	}(id)
	if !ok {
		return
	}
	iters := math.Ceil(float64(t.LastSeq) / float64(2))
	if iters == 0 {
		llog.Error("finalizing transaction with 0 iterations", llog.KV{"id": id, "ip": t.IP.String()})
		return
	}
	if len(t.TripTimes) != len(t.Offsets) {
		llog.Error("finalizing transaction with invalid iterations", llog.KV{
			"trips":   len(t.TripTimes),
			"offsets": len(t.Offsets),
			"ip":      t.IP.String(),
		})
		return
	}
	offsetInMS := calculateAverageOffset(t.TripTimes, t.Offsets)
	llog.Info("slave offset", llog.KV{"ip": t.IP.String(), "offset": offsetInMS})

	if config.Threshold < math.Abs(offsetInMS) {
		llog.Warn("slave offset is over threshold", llog.KV{"ip": t.IP.String(), "offset": offsetInMS})
	}
}

func cleanupTransaction(id int32) {
	tMutex.Lock()
	delete(transactions, id)
	tMutex.Unlock()
}

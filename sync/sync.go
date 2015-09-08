// The sync package handles all the syncronization between clients
package sync

import (
	"github.com/golang/protobuf/proto"
	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
	. "github.com/levenlabs/saturn/proto"
	"net"
	"time"
)

var defaultReq = &ReportRequest{}
var defaultResp = &ReportResponse{}

const (
	REPORT   = byte(2)
	RESPONSE = byte(3)
)

func SendReport(serverAddr *net.UDPAddr) {
	now := time.Now()
	req := makeReport(serverAddr.IP, now)
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
	if transactionExists(srcAddr.IP, req.Trans) {
		llog.Warn("received duplicate report transactionID", llog.KV{"addr": srcAddr})
		return
	}
	if req.Seq != 1 {
		llog.Warn("received report with 1 seq other than 1", llog.KV{"addr": srcAddr, "seq": req.Seq})
		return
	}
	recordNewRequest(req, now, srcAddr)

	diff := req.Time - now.UnixNano()
	resp := makeResponse(now, diff, req.Trans, req.Seq)
	err := sendResponse(resp, req.Reply, srcAddr)
	if err != nil {
		llog.Error("sendResponse error in HandleReport", llog.KV{"err": err})
	}
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
	//only the master should be recording the TripTimes so only do this if its not nil
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
		calcOffsetForTransaction(srcAddr.IP, resp.Trans)
		cleanupTransaction(srcAddr.IP, resp.Trans)
		return
	}

	newResp := makeResponse(now, diff, resp.Trans, resp.Seq)
	err := sendResponse(newResp, resp.Reply, srcAddr)
	if err != nil {
		llog.Error("sendResponse error in HandleReport", llog.KV{"err": err})
	}
}

// The sync package handles all the syncronization between clients
package sync

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
	. "github.com/levenlabs/saturn/proto"
	"net"
	"strconv"
	"time"
)

type MsgType byte

const (
	Init     MsgType = 1
	Report           = 2
	Response         = 3
)

func SendReport(serverAddr *net.UDPAddr) {
	//this is making a pretend job for a report to send that we can funnel through
	//the normal flow of packets
	c, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		llog.Error("dial error in SendReport", llog.KV{"err": err})
		return
	}
	t := InitTransaction(serverAddr, c)
	//we need to start reading off the connection
	go t.ReaderSpin()

	go func() {
		t.JobSpin()
		c.Close()
	}()
	t.NewJob(&Job{
		MType:   Init,
		SrcAddr: serverAddr,
	})
}

func DecodeMessage(d []byte, srcAddr *net.UDPAddr) (*Job, error) {
	if len(d) < 2 {
		return nil, errors.New("Invalid message length")
	}
	var msg proto.Message
	mt := MsgType(d[0])
	switch mt {
	case Report:
		msg = &ReportRequest{}
	case Response:
		msg = &ReportResponse{}
	default:
		return nil, errors.New("received invalid first byte: " + strconv.Itoa(int(d[0])))
	}
	err := proto.Unmarshal(d[1:], msg)
	if err != nil {
		return nil, err
	}
	return &Job{
		SrcAddr: srcAddr,
		MType:   mt,
		Msg:     msg,
	}, nil
}

func getTransIDFromProto(mt MsgType, msg proto.Message) int32 {
	switch mt {
	case Response:
		if r, ok := msg.(*ReportResponse); ok {
			return r.Trans
		}
	}
	return 0
}

func EnsureTransaction(job *Job, conn *net.UDPConn) *Transaction {
	var t *Transaction
	switch job.MType {
	case Report:
		t = InitTransaction(job.SrcAddr, conn)
		//start spinning for jobs
		go t.JobSpin()
	case Response:
		t = getTransaction(job.SrcAddr.IP, getTransIDFromProto(job.MType, job.Msg))
	}
	return t
}

func handleJob(j *Job, t *Transaction) []byte {
	kv := llog.KV{"src": j.SrcAddr}
	switch j.MType {
	case Report:
		if req, ok := j.Msg.(*ReportRequest); ok {
			return handleReport(req, j.SrcAddr, t)
		} else {
			llog.Error("error casting to ReportRequest in handleJob", kv)
		}
	case Response:
		if resp, ok := j.Msg.(*ReportResponse); ok {
			return handleResponse(resp, j.SrcAddr, t)
		} else {
			llog.Error("error casting to ReportRequest in handleJob", kv)
		}
	}
	return nil
}

func handleReport(req *ReportRequest, srcAddr *net.UDPAddr, t *Transaction) []byte {
	kv := llog.KV{"addr": srcAddr, "seq": req.Seq}
	llog.Debug("received report", kv)
	if !verifyReport(req) {
		llog.Warn("received unsigned report", kv)
		return nil
	}
	if !t.SeqMatches(req.Seq) {
		llog.Warn("failed to match seq from report", kv)
		return nil
	}
	t.SetName(req.Name)
	//we want to start tracking this transaction now that its valid and we get a report
	t.Track()
	kv["trans"] = t.ID
	llog.Debug("stored report transaction", kv)

	now := time.Now()
	diff := req.Time - now.UnixNano()
	//store the first offset and we'll calculate the trip time when we get the first response
	t.StoreOffset(diff)
	resp := makeResponse(now, diff, t.ID, req.Seq)
	b, err := GetResponseBytes(resp)
	if err != nil {
		llog.Error("sendResponse error in handleReport", llog.KV{"err": err})
		return nil
	}
	return b
}

func handleResponse(resp *ReportResponse, srcAddr *net.UDPAddr, t *Transaction) []byte {
	kv := llog.KV{"addr": srcAddr, "seq": resp.Seq, "trans": resp.Trans}
	llog.Debug("received response", kv)
	if !verifyResponse(resp) {
		llog.Warn("received unsigned response", kv)
		return nil
	}
	if !t.SeqMatches(resp.Seq) {
		llog.Warn("failed to match seq from response", kv)
		return nil
	}

	now := time.Now()
	diff := resp.Time - now.UnixNano()
	//if this is the 3rd packet then we need to include the RTT for the first offest
	if len(t.Offsets) == 1 && len(t.TripTimes) == 0 {
		t.StoreTripTime(time.Duration(resp.Diff - t.Offsets[0]))
	}
	t.StoreTripTime(now.Sub(t.LastResponse))
	t.StoreOffset(diff)

	//first trip is free and then each iteration is 2 trips
	//seq starts at 1 so after 1 iteration it'll be at 3
	//only the master can terminate a sequence
	if config.IsMaster && (resp.Seq/2) >= config.Iterations {
		calcOffsetForTransaction(t)
		return nil
	}

	newResp := makeResponse(now, diff, resp.Trans, resp.Seq)
	b, err := GetResponseBytes(newResp)
	if err != nil {
		llog.Error("sendResponse error in handleResponse", llog.KV{"err": err})
	}
	return b
}

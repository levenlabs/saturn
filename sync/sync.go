// The sync package handles all the syncronization between clients
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
	"time"
)

var lastTransID int32
var macSep = []byte(":")
var defaultReq = &ReportRequest{}
var defaultResp = &ReportResponse{}

const (
	REPORT   = byte(2)
	RESPONSE = byte(3)
)

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

func SendReport(serverAddr *net.UDPAddr) {
	req := &ReportRequest{
		Time:  time.Now().UnixNano(),
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

func sendResponse(reqTime int64, trans int32, seq int32, reply string, srcAddr *net.UDPAddr) error {
	destAddr, err := correctReply(reply, srcAddr)
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	resp := &ReportResponse{
		Diff:  reqTime - now,
		Time:  now,
		Reply: config.ListenAddr,
		Trans: trans,
		Seq:   seq + 1,
	}
	resp.Sig = signResponse(resp)

	llog.Info("send resp", llog.KV{"dest": destAddr, "seq": resp.Seq, "diff": resp.Diff})
	d, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	send(RESPONSE, destAddr, d)
	return nil
}

func HandleReport(msg proto.Message, srcAddr *net.UDPAddr) {
	req, ok := msg.(*ReportRequest)
	if !ok {
		return
	}
	llog.Info("received req", llog.KV{"req": req, "addr": srcAddr})
	if !verifyReport(req) {
		llog.Warn("received unsigned report", llog.KV{"addr": srcAddr})
		return
	}

	err := sendResponse(req.Time, req.Trans, req.Seq, req.Reply, srcAddr)
	if err != nil {
		llog.Error("sendResponse error in HandleReport", llog.KV{"err": err})
	}
}

func HandleResponse(msg proto.Message, srcAddr *net.UDPAddr) {
	resp, ok := msg.(*ReportResponse)
	if !ok {
		return
	}
	llog.Info("received resp", llog.KV{"resp": resp, "addr": srcAddr})
	if !verifyResponse(resp) {
		llog.Warn("received unsigned response", llog.KV{"addr": srcAddr})
		return
	}

	//first trip is free and then each iteration is 2 trips
	//seq starts at 1 so after 1 iteration it'll be at 3
	//only the master can terminate a sequence
	if config.IsMaster && (resp.Seq/2) >= config.Iterations {
		return
	}

	err := sendResponse(resp.Time, resp.Trans, resp.Seq, resp.Reply, srcAddr)
	if err != nil {
		llog.Error("sendResponse error in HandleReport", llog.KV{"err": err})
	}
}

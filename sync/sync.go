// The sync package handles all the syncronization between clients
package sync
import (
	"github.com/levenlabs/saturn/config"
	"github.com/golang/protobuf/proto"
	. "github.com/levenlabs/saturn/proto"
	"time"
	"math/rand"
	"crypto/sha256"
	"crypto/hmac"
	"strconv"
	"github.com/levenlabs/go-llog"
	"net"
)

var lastTransID int32
var macSep = []byte(":")

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
	mac.Write([]byte(strconv.FormatInt(int64(req.Trans), 36)))
	mac.Write(macSep)
	mac.Write([]byte(req.Reply))
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

func send(addr *net.UDPAddr, d []byte) {
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		llog.Error("dial error in SendReport", llog.KV{"err": err})
		return
	}
	defer c.Close()
	_, err = c.Write(d)
	if err != nil {
		llog.Error("write error in SendReport", llog.KV{"err": err})
	}
}

func SendReport(serverAddr *net.UDPAddr) {
	req := &ReportRequest{
		Time: time.Now().UnixNano(),
		Name: config.Name,
		Trans: generateTransID(),
		Seq: 1,
		Reply: config.ListenAddr,
	}
	req.Sig = signReport(req)

	d, err := proto.Marshal(req)
	if err != nil {
		llog.Error("marshal error in SendReport", llog.KV{"err": err})
		return
	}
	send(serverAddr, d)
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

	//determine the addr to send based on the received message
	destAddr, err := net.ResolveUDPAddr("udp", req.Reply)
	if err != nil {
		llog.Error("error parsing req reply address", llog.KV{"err": err, "addr": req.Reply})
		return
	}
	//if they're listening on all interfaces then use the srcAddr
	if destAddr.IP.IsUnspecified() {
		destAddr.IP = srcAddr.IP
	}

	now := time.Now().UnixNano()
	resp := &ReportResponse{
		Diff: req.Time - now,
		Time: now,
		Seq: req.Seq + 1,
		Trans: req.Trans,
	}
	resp.Sig = signResponse(resp)

	d, err := proto.Marshal(resp)
	if err != nil {
		llog.Error("marshal error in HandleReport", llog.KV{"err": err})
		return
	}

	send(destAddr, d)
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
}

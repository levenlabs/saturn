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

//todo: currently the average size of udp mesages is ~80 bytes so should we make this smaller?
const BufferSize = 160

type Transaction struct {
	ID           int32
	Addr         *net.UDPAddr
	Name         string
	TripTimes    []time.Duration
	Offsets      []int64
	LastResponse time.Time
	seq          int32
	conn         *net.UDPConn
	jobCh        chan *Job
	closed       bool
}

type Job struct {
	MType   MsgType
	Msg     proto.Message
	SrcAddr *net.UDPAddr
}

func init() {
	go func() {
		for range time.Tick(1 * time.Minute) {
			removeOldTransactions()
		}
	}()
}

func removeOldTransactions() {
	now := time.Now()
	tMutex.RLock()
	var oldTransactions []*Transaction
	for _, t := range transactions {
		//timeout all transactions 1 minute after the last response
		if now.Sub(t.LastResponse).Minutes() < 1.0 {
			continue
		}
		llog.Warn("had to clean up stale transaction", llog.KV{
			"seq":   t.seq,
			"trans": t.ID,
			"src":   t.Addr,
		})
		oldTransactions = append(oldTransactions, t)
	}
	tMutex.RUnlock()
	for _, t := range oldTransactions {
		t.Close()
	}
}

func getTransactionKey(ip net.IP, t int32) string {
	return strings.Join([]string{ip.String(), strconv.FormatInt(int64(t), 36)}, "|")
}

func getTransaction(ip net.IP, trans int32) *Transaction {
	tMutex.RLock()
	t, _ := transactions[getTransactionKey(ip, trans)]
	tMutex.RUnlock()
	return t
}

func signReport(req *ReportRequest) []byte {
	mac := hmac.New(sha256.New, config.HMACKey)
	mac.Write([]byte(strconv.FormatInt(req.Time, 36)))
	mac.Write(macSep)
	mac.Write([]byte(req.Name))
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

func getReportBytes(r *ReportRequest) ([]byte, error) {
	d, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	d = append([]byte{byte(Report)}, d...)
	return d, nil
}

func GetResponseBytes(r *ReportResponse) ([]byte, error) {
	d, err := proto.Marshal(r)
	if err != nil {
		return nil, err
	}
	d = append([]byte{byte(Response)}, d...)
	return d, nil
}

func makeReport(now time.Time) *ReportRequest {
	req := &ReportRequest{
		Time:  now.UnixNano(),
		Name:  config.Name,
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
		Trans: trans,
		Seq:   lastSeq + 1,
	}
	resp.Sig = signResponse(resp)
	return resp
}

func InitTransaction(addr *net.UDPAddr, c *net.UDPConn) *Transaction {
	t := &Transaction{
		Addr:         addr,
		LastResponse: time.Now(),
		conn:         c,
		jobCh:        make(chan *Job),
		Name:         addr.IP.String(),
	}
	return t
}

/*

Transactions don't need to have mutexes around their properties since all
reading/writing for each transaction is single-threaded via the job channel

*/
func (t *Transaction) Close() {
	if t.closed {
		return
	}
	t.closed = true
	close(t.jobCh)
	t.Untrack()
}

func (t *Transaction) SetName(name string) {
	t.Name = name
	if t.Name == "" {
		t.Name = t.Addr.IP.String()
	}
}

func (t *Transaction) StoreTripTime(d time.Duration) {
	if t.TripTimes == nil {
		//now create the TripTimes array to include the estimated number of triptimes
		t.TripTimes = make([]time.Duration, 0, config.Iterations+1)
	}
	t.TripTimes = append(t.TripTimes, d)
}

func (t *Transaction) StoreOffset(diff int64) {
	if t.Offsets == nil {
		//now create the TripTimes array to include the estimated number of triptimes
		t.Offsets = make([]int64, 0, config.Iterations+1)
	}
	t.Offsets = append(t.Offsets, diff)
}

func generateTransactionID(ip net.IP) int32 {
	var n int32
	var ok bool
	tMutex.RLock()
	for {
		n = rand.Int31()
		//make sure this ID doesn't already exist
		if _, ok = transactions[getTransactionKey(ip, n)]; !ok {
			break
		}
	}
	tMutex.RUnlock()
	return n
}

func (t *Transaction) GenerateID() {
	t.ID = generateTransactionID(t.Addr.IP)
}

func (t *Transaction) Track() {
	if t.ID == 0 {
		t.GenerateID()
	}
	tMutex.Lock()
	transactions[getTransactionKey(t.Addr.IP, t.ID)] = t
	tMutex.Unlock()
}

func (t *Transaction) Untrack() {
	tMutex.Lock()
	delete(transactions, getTransactionKey(t.Addr.IP, t.ID))
	tMutex.Unlock()
}

func (t *Transaction) NewJob(j *Job) {
	t.jobCh <- j
}

func (t *Transaction) SeqMatches(seq int32) bool {
	return t.seq == seq
}

func (t *Transaction) ReaderSpin() {
	var n int
	var src *net.UDPAddr
	var err error
	var j *Job
	buf := make([]byte, BufferSize)
	for {
		t.conn.SetReadDeadline(time.Now().Add(time.Second * 10))
		n, src, err = t.conn.ReadFromUDP(buf)
		if err != nil {
			if nerr, _ := err.(net.Error); nerr.Timeout() {
				//if we timed out then master/slave must've died or we're done
				break
			} else if nerr.Temporary() {
				//its "temporary" so continue
				continue
			}
			llog.Warn("error reading from UDP connection", llog.KV{"err": err})
			break
		}

		j, err = DecodeMessage(buf[0:n], src)
		if err != nil {
			llog.Warn("error calling DecodeMessage in ReaderSpin", llog.KV{"err": err, "src": src})
			break
		}
		t.NewJob(j)
	}
	t.Close()
}

func (t *Transaction) JobSpin() {
	var j *Job
	var p []byte
	var err error
	for j = range t.jobCh {
		if !t.Addr.IP.Equal(j.SrcAddr.IP) {
			llog.Warn("received job from a mismatched IP", llog.KV{
				"src":      j.SrcAddr,
				"expected": t.Addr,
			})
			continue
		}

		//we have to wrap this particular set in a mutex since removeOldTransactions reads this
		tMutex.Lock()
		t.LastResponse = time.Now()
		tMutex.Unlock()

		if j.MType == Init {
			p, err = getReportBytes(makeReport(time.Now()))
			if err != nil {
				llog.Error("error from GetReportBytes", llog.KV{"err": err})
				break
			}
		} else {
			//increase seq for read
			t.seq++
			p = handleJob(j, t)
			if p == nil {
				//nothing left to write so this transaction is done
				break
			}
		}
		t.write(p)
	}
	t.Close()
}

func (t *Transaction) write(p []byte) {
	var err error
	if t.conn.RemoteAddr() == nil {
		_, err = t.conn.WriteToUDP(p, t.Addr)
	} else {
		_, err = t.conn.Write(p)
	}
	if err != nil {
		llog.Error("write error", llog.KV{"err": err, "src": t.Addr})
		return
	}
	//increase seq for write
	t.seq++
}

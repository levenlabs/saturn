package transaction

import (
	"math"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
	lproto "github.com/levenlabs/saturn/proto"
)

// IncomingMessage takes a message just received from the remote address and
// processes it. It may return a message which should be sent back to the
// remote, or it may return nil
func IncomingMessage(msg *lproto.TxMsg) *lproto.TxMsg {
	im := incomingMsg{
		msg:     msg,
		replyCh: make(chan *lproto.TxMsg),
	}
	incomingMsgCh <- im
	return <-im.replyCh
}

type incomingMsg struct {
	msg     *lproto.TxMsg
	replyCh chan *lproto.TxMsg
}

var incomingMsgCh = make(chan incomingMsg)

func incoming(transactions map[string]*tx, msg *lproto.TxMsg) *lproto.TxMsg {

	kv := llog.KV{
		"msg": msg,
	}
	if !msg.Verify() {
		llog.Warn("received incorrectly signed message", kv)
		return nil
	}

	t := transactions[msg.Id]
	if t != nil && t.expectedSeq != msg.Seq {
		return nil
	}

	retTx := &lproto.TxMsg{
		Id:  msg.Id,
		Seq: msg.Seq + 1,
	}

	var hasInner bool
	if r := msg.GetInitialReport(); r != nil {
		retTx.Inner, hasInner = handleIncomingInitialReport(transactions, msg.Id, r)

	} else if r := msg.GetReport(); r != nil {
		if t == nil {
			llog.Warn("received Report for unknown transaction", kv)
			return nil
		}
		retTx.Inner, hasInner = handleIncomingReport(t, r)

	} else {
		llog.Warn("received unknown proto.Message type", kv)
	}

	if !hasInner {
		return nil
	}

	// We re-get t because the transaction might not have existed when t was
	// created, although it definitely exists now
	t = transactions[msg.Id]
	t.expectedSeq = msg.Seq + 2
	t.lastMessage = time.Now()

	retTx.Sign()
	return retTx
}

// This will only occur on the master's side, the slave will only ever be
// sending an initial report, never receiving one
func handleIncomingInitialReport(transactions map[string]*tx, id string, rep *lproto.InitialReport) (*lproto.TxMsg_Report, bool) {
	t := newTx(transactions, id, rep.Name)

	now := time.Now()
	diff := time.Duration(rep.Time - now.UnixNano())

	//store the first offset and we'll calculate the trip time when we get the
	//first report
	t.offsets = append(t.offsets, diff)

	return &lproto.TxMsg_Report{&lproto.Report{
		Diff: int64(diff),
		Time: now.UnixNano(),
	}}, true
}

func handleIncomingReport(t *tx, rep *lproto.Report) (*lproto.TxMsg_Report, bool) {
	now := time.Now()
	diff := time.Duration(rep.Time - now.UnixNano())

	if config.IsMaster {
		//if this is the 3rd packet then we need to include the RTT for the first offest
		if len(t.offsets) == 1 && len(t.tripTimes) == 0 {
			t.tripTimes = append(t.tripTimes, time.Duration(rep.Diff)-t.offsets[0])
		}
		t.tripTimes = append(t.tripTimes, now.Sub(t.lastMessage))
		t.offsets = append(t.offsets, diff)

		//first trip is free and then each iteration is 2 trips
		//seq starts at 1 so after 1 iteration it'll be at 3
		//only the master can terminate a sequence
		if (t.expectedSeq / 2) >= config.Iterations {
			offset, err := calculateAverageOffset(t.tripTimes, t.offsets)

			kv := t.kv()
			kv["offset"] = offset
			kv["txNumTrips"] = len(t.tripTimes)
			kv["txNumOffsets"] = len(t.offsets)

			if err != nil {
				kv["err"] = err
				llog.Error("error calculating avg offset", kv)
			} else {
				llog.Info("slave offset", kv)
				if config.Threshold < math.Abs(offset) {
					llog.Warn("slave offset is over threshold", kv)
				}
			}
			return nil, false
		}
	}

	return &lproto.TxMsg_Report{&lproto.Report{
		Diff: int64(diff),
		Time: now.UnixNano(),
	}}, true
}

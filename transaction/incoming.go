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
// remote, or it may return nil. It also returns bool signaling to end
// the transaction if true.
func IncomingMessage(msg *lproto.TxMsg) (*lproto.TxMsg, bool) {
	im := incomingMsg{
		msg:     msg,
		replyCh: make(chan incomingReply),
	}
	incomingMsgCh <- im
	r := <-im.replyCh
	return r.nextMsg, r.end
}

type incomingMsg struct {
	msg     *lproto.TxMsg
	replyCh chan incomingReply
}

type incomingReply struct {
	nextMsg *lproto.TxMsg
	end     bool
}

var incomingMsgCh = make(chan incomingMsg)

// returns the next message, if any, and a bool indiciating if the transaction
// should be considered ended or not
func incoming(transactions map[string]*tx, msg *lproto.TxMsg) (*lproto.TxMsg, bool) {

	kv := llog.KV{
		"msg": msg,
	}
	if !msg.Valid() {
		llog.Warn("received invalid message (possibly from a newer version?)", kv)
		return nil, false
	}
	if !msg.Verify() {
		llog.Warn("received incorrectly signed message", kv)
		return nil, false
	}

	t := transactions[msg.Id]
	if t != nil && t.expectedSeq != msg.Seq {
		kv["txExpectedSeq"] = t.expectedSeq
		kv["txReceivedSeq"] = msg.Seq
		llog.Warn("received message with wrong seq", kv)
		// end the transaction since we got out of order
		return nil, true
	}

	nextSeq := msg.Seq + 1
	retTx := &lproto.TxMsg{
		Id:  msg.Id,
		Seq: nextSeq,
	}

	var hasInner bool
	var ended bool
	if r := msg.GetInitialReport(); r != nil {
		retTx.Inner = handleIncomingInitialReport(transactions, msg.Id, r)
		hasInner = true

	} else if r := msg.GetReport(); r != nil {
		if t == nil {
			llog.Warn("received Report for unknown transaction", kv)
			// we don't know about this transaction anymore so end
			return nil, true
		}
		iR, iF := handleIncomingReport(t, r)
		if iR != nil {
			retTx.Inner = iR
		} else {
			retTx.Inner = iF
			// since this is a fin, we can destroy this transaction after
			// writing
			ended = true
			defer func() {
				cleanTx(transactions, t.id)
			}()
		}
		hasInner = true

	} else if f := msg.GetFin(); f != nil {
		if t == nil {
			llog.Warn("received Fin for unknown transaction", kv)
			// we don't know about this transaction anymore so end
			return nil, true
		}
		handleFin(transactions, t, f)
		ended = true

	} else {
		llog.Warn("received unknown proto.Message type", kv)
	}

	if hasInner {
		// We re-get t because the transaction might not have existed when t was
		// created, although it definitely exists now
		t = transactions[msg.Id]
		// Add 2 here since the next seq should be the next incoming one which is
		// one greater than the one we just sent
		t.expectedSeq = nextSeq + 1
		t.lastMessage = time.Now()

		retTx.Sign()
	} else {
		retTx = nil
	}
	return retTx, ended
}

// This will only occur on the master's side, the slave will only ever be
// sending an initial report, never receiving one
func handleIncomingInitialReport(transactions map[string]*tx, id string, rep *lproto.InitialReport) *lproto.TxMsg_Report {
	t := newTx(transactions, id, rep.Name)

	now := time.Now()
	diff := time.Duration(rep.Time - now.UnixNano())
	kv := t.kv()
	llog.Debug("incoming initial report", kv, llog.KV{"isMaster": config.IsMaster})

	//store the first offset and we'll calculate the trip time when we get the
	//first report
	t.offsets = append(t.offsets, diff)

	return &lproto.TxMsg_Report{
		Report: &lproto.Report{
			Diff: int64(diff),
			Time: now.UnixNano(),
		},
	}
}

func handleIncomingReport(t *tx, rep *lproto.Report) (*lproto.TxMsg_Report, *lproto.TxMsg_Fin) {
	now := time.Now()
	diff := time.Duration(rep.Time - now.UnixNano())
	kv := t.kv()
	llog.Debug("incoming report", kv, llog.KV{"isMaster": config.IsMaster})

	if config.IsMaster {
		//if this is the 3rd packet then we need to include the RTT for the first offest
		if len(t.offsets) == 1 && len(t.tripTimes) == 0 {
			t.tripTimes = append(t.tripTimes, time.Duration(rep.Diff)-t.offsets[0])
		}
		t.tripTimes = append(t.tripTimes, now.Sub(t.lastMessage))
		t.offsets = append(t.offsets, diff)
		kv["txNumTrips"] = len(t.tripTimes)

		//first trip is free and then each iteration is 2 trips
		//seq starts at 1 so after 1 iteration it'll be at 3
		//only the master can terminate a sequence
		if (t.expectedSeq / 2) >= config.Iterations {
			offset, err := calculateAverageOffset(t.tripTimes, t.offsets)
			fin := &lproto.Fin{}
			if err != nil {
				fin.Error = err.Error()
				llog.Error("error calculating avg offset", kv.Set("err", err))
			} else {
				fin.Offset = offset
				kv["offset"] = offset
				llog.Info("slave offset", kv)
				if config.Threshold < math.Abs(offset) {
					llog.Warn("slave offset is over threshold", kv)
				}
			}
			llog.Debug("over iterations, ending transaction", kv)
			return nil, &lproto.TxMsg_Fin{
				Fin: fin,
			}
		}
	}

	return &lproto.TxMsg_Report{
		Report: &lproto.Report{
			Diff: int64(diff),
			Time: now.UnixNano(),
		},
	}, nil
}

func handleFin(transactions map[string]*tx, t *tx, fin *lproto.Fin) {
	kv := t.kv()
	kv["offset"] = fin.Offset
	kv["error"] = fin.Error
	llog.Debug("received fin", kv)
	// we received a fin so don't respond with anything and cleanup
	// transaction
	cleanTx(transactions, t.id)
}

// Package transaction deals with the back and forth between the master and
// slave, and keeps track of the different tims and offsets that they give each
// other
//
// A transaction consists of a set of multiple messages between the slave and
// master, each containing the current time and their diff from the time
// delivered previously. Using this data an average offset between the two can
// be determined, taking into account RTT between the two endpoints.
//
// A transaction is started when a slave calls Initiate to set up a new
// transaction and receive the first message it should send to the master. Once
// the master sends back a response, this should be given to IncomingMessage,
// which will return another message to send to the master, and so on.
//
// On the master's side, when it receives a message from any slave it should
// pass that message into IncomingMessage, which will set up the necessary
// transaction data if necessary and return a response message. If it returns
// nil the transaction is completed and nothing needs to be sent.
package transaction

import (
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
)

// To be called in order to start any necessary go-routines needed for this
// package to work
func Init() {
	// This is not automatically called (like init), and we want it that way so
	// we have the option of not starting txSpin during testing
	go txSpin()
}

var cleanupTimeout = 1 * time.Minute

type tx struct {
	id          string
	name        string
	tripTimes   []time.Duration
	offsets     []time.Duration
	lastMessage time.Time
	expectedSeq int32
}

func txSpin() {
	transactions := map[string]*tx{}
	cleanTick := time.Tick(cleanupTimeout / 2)
	for {
		select {
		case im := <-incomingMsgCh:
			im.replyCh <- incoming(transactions, im.msg)
		case it := <-initiateTxCh:
			it.replyCh <- initiate(transactions, it.idStr)
		case <-cleanTick:
			cleanTxs(transactions)
		}
	}
}

// this function is not thread-safe
func cleanTxs(transactions map[string]*tx) {
	for k, t := range transactions {
		if time.Since(t.lastMessage) < cleanupTimeout {
			continue
		}
		delete(transactions, k)
	}
}

// newTx creates a new tx struct for a transaction with the given remote
// address, where the remote address has the given name (optional). The tx will
// be already added to the transactions map when it is returned. Note that you
// should modify expectedSeq as necessary (depending on which side of the
// transaction you're looking at)
func newTx(transactions map[string]*tx, id, name string) *tx {
	t := &tx{
		id:          id,
		name:        name,
		lastMessage: time.Now(),
	}
	if config.IsMaster {
		t.tripTimes = make([]time.Duration, 0, config.Iterations+1)
		t.offsets = make([]time.Duration, 0, config.Iterations+1)
	}
	transactions[id] = t
	return t
}

func (t *tx) kv() llog.KV {
	return llog.KV{
		"txID":          t.id,
		"clientName":        t.name,
	}
}

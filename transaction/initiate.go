package transaction

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/levenlabs/saturn/config"
	lproto "github.com/levenlabs/saturn/proto"
)

// Initiate sets up all the required tracking data for a new transaction and
// returns the first message which should be sent for that transaction. This is
// called by the slave at intervals, it's never called by the master.
//
// idStr should be a string that's semi-unique to the connection the transaction
// is being made over. The local "ip:port" of the connection is recommended
func Initiate(idStr string) *lproto.TxMsg {
	it := initiateTx{
		replyCh: make(chan *lproto.TxMsg),
		idStr:   idStr,
	}
	initiateTxCh <- it
	return <-it.replyCh
}

type initiateTx struct {
	replyCh chan *lproto.TxMsg
	idStr   string
}

var initiateTxCh = make(chan initiateTx)

func newTxID(idStr string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return idStr + "_" + hex.EncodeToString(b)
}

func initiate(transactions map[string]*tx, idStr string) *lproto.TxMsg {
	id := newTxID(idStr)
	t := newTx(transactions, id, config.Name)
	t.expectedSeq = 2

	tx := &lproto.TxMsg{
		Id:  id,
		Seq: 1,
		Inner: &lproto.TxMsg_InitialReport{&lproto.InitialReport{
			Time: time.Now().UnixNano(),
			Name: config.Name,
		}},
	}
	tx.Sign()
	return tx
}

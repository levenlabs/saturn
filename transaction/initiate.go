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
// srcIP should be the ip that we are sending packets from. It is used in place
// of a name if no name was supplied in config.
func Initiate(idStr string, srcIP string) *lproto.TxMsg {
	it := initiateTx{
		replyCh: make(chan *lproto.TxMsg),
		idStr:   idStr,
		srcIP:   srcIP,
	}
	initiateTxCh <- it
	return <-it.replyCh
}

type initiateTx struct {
	replyCh chan *lproto.TxMsg
	idStr   string
	srcIP   string
}

var initiateTxCh = make(chan initiateTx)

func newTxID(idStr string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return idStr + "_" + hex.EncodeToString(b)
}

func initiate(transactions map[string]*tx, idStr string, srcIP string) *lproto.TxMsg {
	id := newTxID(idStr)
	name := config.Name
	if name == "" {
		name = srcIP
	}
	t := newTx(transactions, id, name)
	t.expectedSeq = 2

	tx := &lproto.TxMsg{
		Id:  t.id,
		Seq: 1,
		Inner: &lproto.TxMsg_InitialReport{&lproto.InitialReport{
			Time: time.Now().UnixNano(),
			Name: t.name,
		}},
	}
	tx.Sign()
	return tx
}

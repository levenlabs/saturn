package transaction

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
)

func TestClean(t *T) {
	transactions := map[string]*tx{}

	tx := newTx(transactions, testutil.RandStr(), testutil.RandStr())
	cleanTxs(transactions)
	_, ok := transactions[tx.id]
	assert.True(t, ok)

	tx.lastMessage = time.Now().Add(-5 * time.Minute)
	cleanTxs(transactions)
	_, ok = transactions[tx.id]
	assert.False(t, ok)
}

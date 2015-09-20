package transaction

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/levenlabs/saturn/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitiate(t *T) {
	config.Name = testutil.RandStr()
	transactions := map[string]*tx{}

	msg := initiate(transactions, testutil.RandStr())

	assert.Equal(t, int32(1), msg.Seq)
	assert.Equal(t, config.Name, msg.GetInitialReport().Name)
	assert.True(t, msg.Verify())

	tx, ok := transactions[msg.Id]
	require.True(t, ok)
	assert.Equal(t, msg.Id, tx.id)
	assert.Equal(t, config.Name, tx.name)
	assert.Equal(t, int32(2), tx.expectedSeq)
}

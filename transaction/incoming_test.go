package transaction

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/levenlabs/saturn/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncoming(t *T) {
	config.Iterations = 2

	// So we switch off between the slave's transactions map and the master's
	slaveTxs := map[string]*tx{}
	masterTxs := map[string]*tx{}

	t.Log("initializing tx as slave")
	config.IsMaster = false
	initReport := initiate(slaveTxs, testutil.RandStr(), "")

	t.Log("sending slave's initial report to master")
	config.IsMaster = true
	msg, end := incoming(masterTxs, initReport)
	require.NotNil(t, msg)
	require.False(t, end)

	assert.Equal(t, initReport.Id, msg.Id)
	assert.Equal(t, initReport.Seq+1, msg.Seq)
	assert.True(t, msg.Verify())

	tx, ok := masterTxs[initReport.Id]
	require.True(t, ok)
	assert.Equal(t, tx.expectedSeq, msg.Seq+1)
	assert.Equal(t, 1, len(tx.offsets))
	assert.Equal(t, 0, len(tx.tripTimes))

	t.Log("sending master's first report to slave")
	config.IsMaster = false
	msg2, end := incoming(slaveTxs, msg)
	require.NotNil(t, msg2)
	require.False(t, end)

	assert.Equal(t, msg.Id, msg2.Id)
	assert.Equal(t, msg.Seq+1, msg2.Seq)
	assert.True(t, msg2.Verify())

	t.Log("sending slave's second report to master")
	config.IsMaster = true
	msg3, end := incoming(masterTxs, msg2)
	require.NotNil(t, msg3)
	require.False(t, end)

	assert.Equal(t, msg2.Id, msg3.Id)
	assert.Equal(t, msg2.Seq+1, msg3.Seq)
	assert.True(t, msg3.Verify())

	tx, ok = masterTxs[msg2.Id]
	require.True(t, ok)
	assert.Equal(t, tx.expectedSeq, msg3.Seq+1)
	assert.Equal(t, 2, len(tx.offsets))
	assert.Equal(t, 2, len(tx.tripTimes))

	t.Log("sending master's second report to slave")
	config.IsMaster = false
	msg4, end := incoming(slaveTxs, msg3)
	require.NotNil(t, msg4)
	require.False(t, end)

	assert.Equal(t, msg3.Id, msg4.Id)
	assert.Equal(t, msg3.Seq+1, msg4.Seq)
	assert.True(t, msg4.Verify())

	t.Log("sending slave's final report to master")
	config.IsMaster = true
	msg5, end := incoming(masterTxs, msg4)
	assert.NotNil(t, msg5)
	require.True(t, end)

	assert.Equal(t, msg4.Id, msg5.Id)
	assert.Equal(t, msg4.Seq+1, msg5.Seq)
	assert.True(t, msg5.Verify())
	assert.NotNil(t, msg5.GetFin())

	// make sure the master transaction is cleaned up
	assert.NotContains(t, msg4.Id, masterTxs)

	t.Log("sending master fin to slave")
	config.IsMaster = false
	msg6, end := incoming(slaveTxs, msg5)
	require.Nil(t, msg6)
	require.True(t, end)

	// make sure the slave transaction is cleaned up
	assert.NotContains(t, msg5.Id, slaveTxs)
}

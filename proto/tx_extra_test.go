package proto

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
)

func TestSignVerify(t *T) {
	tx := &TxMsg{
		Id:  testutil.RandStr(),
		Seq: int32(testutil.RandInt64()),
		Inner: &TxMsg_InitialReport{&InitialReport{
			Time: testutil.RandInt64(),
			Name: testutil.RandStr(),
		}},
	}

	assert.False(t, tx.Verify())
	tx.Sig = []byte(testutil.RandStr())
	assert.False(t, tx.Verify())

	tx.Sign()
	assert.NotEmpty(t, tx.Sig)
	assert.True(t, tx.Verify())

	oldSig := tx.Sig

	tx = &TxMsg{
		Id:  testutil.RandStr(),
		Seq: int32(testutil.RandInt64()),
		Inner: &TxMsg_Report{&Report{
			Time: testutil.RandInt64(),
			Diff: testutil.RandInt64(),
		}},
	}

	assert.False(t, tx.Verify())
	tx.Sig = []byte(testutil.RandStr())
	assert.False(t, tx.Verify())

	tx.Sign()
	assert.NotEmpty(t, tx.Sig)
	assert.True(t, tx.Verify())

	assert.NotEqual(t, oldSig, tx.Sig)
}

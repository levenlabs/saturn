package proto

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/saturn/config"
)

//go:generate sh -c "protoc --go_out=. *.proto"

func (m *TxMsg) getSig() []byte {
	mac := hmac.New(sha256.New, config.HMACKey)
	fmt.Fprint(mac, m.Id)
	fmt.Fprint(mac, m.Seq)
	switch r := m.GetInner().(type) {
	case *TxMsg_InitialReport:
		fmt.Fprint(mac, r.InitialReport.Time)
		fmt.Fprint(mac, r.InitialReport.Name)
	case *TxMsg_Report:
		fmt.Fprint(mac, r.Report.Diff)
		fmt.Fprint(mac, r.Report.Time)
	case *TxMsg_Fin:
		fmt.Fprint(mac, r.Fin.Offset)
	default:
		llog.Error("received invalid report inner")
	}
	return mac.Sum(nil)
}

// Sign signs this message
func (m *TxMsg) Sign() {
	m.Sig = m.getSig()
}

// Valid returns if the TxMsg is valid and has a known Inner
// this is used to ignore messages from newer/older versions
func (m *TxMsg) Valid() bool {
	if m.Id == "" {
		return false
	}
	var known bool
	switch m.GetInner().(type) {
	case *TxMsg_InitialReport:
		known = true
	case *TxMsg_Report:
		known = true
	case *TxMsg_Fin:
		known = true
	}
	return known
}

// Verify makes sure that the signature matches what we calculate it to be
func (m *TxMsg) Verify() bool {
	return hmac.Equal(m.getSig(), m.Sig)
}

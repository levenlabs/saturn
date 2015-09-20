package proto

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"

	"github.com/levenlabs/saturn/config"
)

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
	}
	return mac.Sum(nil)
}

func (m *TxMsg) Sign() {
	m.Sig = m.getSig()
}

func (m *TxMsg) Verify() bool {
	return hmac.Equal(m.getSig(), m.Sig)
}

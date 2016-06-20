package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/go-srvclient"
	"github.com/levenlabs/saturn/config"
	lproto "github.com/levenlabs/saturn/proto"
	"github.com/levenlabs/saturn/transaction"
	"github.com/mediocregopher/skyapi/client"
)

func main() {

	transaction.Init()

	if config.IsMaster {
		llog.Info("starting as master")
		kv := llog.KV{"addr": config.ListenAddr}
		llog.Info("listening on udp", kv)
		lAddr, err := net.ResolveUDPAddr("udp", config.ListenAddr)
		if err != nil {
			llog.Fatal("error resolving UDP addr", kv.Set("err", err))
		}

		go advertise()
		listenSpin(lAddr, nil)
	} else {
		llog.Info("starting as slave", llog.KV{"master": config.MasterAddr})
		reportSpin()
	}
}

func lookupMaster() *net.UDPAddr {
	//see if they passed a hostname without a port and then do a srv lookup
	masterAddr := srvclient.MaybeSRV(config.MasterAddr)
	serverAddr, err := net.ResolveUDPAddr("udp", masterAddr)
	if err != nil {
		llog.Fatal("error resolving UDP addr", llog.KV{"err": err, "addr": masterAddr})
	}
	return serverAddr
}

func advertise() {
	if config.SkyAPIAddr == "" {
		return
	}
	kv := llog.KV{"skyapiAddr": config.SkyAPIAddr, "listenAddr": config.ListenAddr}
	llog.Info("connecting to skyapi", kv)

	kv["err"] = client.ProvideOpts(client.Opts{
		SkyAPIAddr:        config.SkyAPIAddr,
		Service:           "saturn",
		ThisAddr:          config.ListenAddr,
		ReconnectAttempts: -1,
	})
	llog.Fatal("skyapi giving up reconnecting", kv)
}

func marshalAndWrite(msg proto.Message, c *net.UDPConn, dst *net.UDPAddr, kv llog.KV) bool {
	b, err := proto.Marshal(msg)
	if err != nil {
		llog.Error("error marshaling msg", kv.Set("err", err))
		return false
	}

	_, err = c.WriteToUDP(b, dst)
	if err != nil {
		llog.Error("error writing msg", kv.Set("err", err))
		return false
	}
	return true
}

func readAndUnmarshal(c *net.UDPConn, kv llog.KV) (*lproto.TxMsg, *net.UDPAddr, bool) {
	b := make([]byte, 1024)
	n, addr, err := c.ReadFromUDP(b)
	if err != nil {
		fn := llog.Error
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			fn = llog.Warn
		}
		fn("error reading from udp socket", kv.Set("err", err))
		return nil, nil, false
	}

	var msg lproto.TxMsg
	if err := proto.Unmarshal(b[:n], &msg); err != nil {
		llog.Error("error unmarshaling proto msg", kv.Set("err", err))
		// even though we had an error unmarshalling, it could just be bogus
		// traffic
		return nil, nil, true
	}
	return &msg, addr, true
}

func reportSpin() {
	lookupAndSlaveReport()

	tick := time.Tick(time.Duration(config.Interval) * time.Second)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	for {
		select {
		case <-sig:
			lookupAndSlaveReport()
		case <-tick:
			lookupAndSlaveReport()
		}
	}
}

func lookupAndSlaveReport() {
	masterAddr := lookupMaster()
	doSlaveReport(masterAddr)
}

func doSlaveReport(masterAddr *net.UDPAddr) bool {
	kv := llog.KV{"addr": masterAddr}
	llog.Info("beginning transaction", kv)
	defer llog.Info("ended transaction", kv)

	// it doesn't really matter what port we listen for
	lAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		llog.Fatal("error resolving report listen UDP addr", kv.Set("err", err))
		return false
	}

	c, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		llog.Error("error connecting to master", kv.Set("err", err))
		return false
	}
	defer c.Close()

	// We're using the localAddr as the prefix for the transaction ID
	addrStr := c.LocalAddr().String()
	ipStr, _, _ := net.SplitHostPort(addrStr)
	firstMsg := transaction.Initiate(addrStr, ipStr)
	kv["txID"] = firstMsg.Id
	if !marshalAndWrite(firstMsg, c, masterAddr, kv) {
		// logging happened in marshalAndWrite
		return false
	}

	for {
		// Set a ReadDeadline so we don't wait forever if the master is dead
		c.SetReadDeadline(time.Now().Add(time.Second * 5))
		msg, _, ok := readAndUnmarshal(c, kv)
		if !ok {
			// logging happned in readAndUnmarshal
			return false
		}
		// maybe we received some bogus traffic that wasn't unmarshalable
		if msg == nil {
			continue
		}

		nextMsg, end := transaction.IncomingMessage(msg)
		if nextMsg == nil {
			if end {
				llog.Debug("no nextMsg, closing udp", kv)
				// success so break
				break
			}
			continue
		}
		if !marshalAndWrite(nextMsg, c, masterAddr, kv) {
			// logging happened in marshalAndWrite
			return false
		}
	}
	return true
}

func listenSpin(lAddr *net.UDPAddr, stopCh chan interface{}) {
	kv := llog.KV{"addr": lAddr.String()}
	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		llog.Fatal("error listening to UDP port", kv.Set("err", err))
	}
	for {
		select {
		case <-stopCh:
			return
		default:
			doMasterInner(conn)
		}
	}
}

func doMasterInner(c *net.UDPConn) {
	kv := llog.KV{"addr": config.ListenAddr}
	//don't set a ReadDeadline since we want it to block until a message
	msg, remoteAddr, ok := readAndUnmarshal(c, kv)
	if !ok {
		llog.Fatal("couldn't read from udp listen socket", kv)
	}
	// maybe we received some bogus traffic that wasn't unmarshalable
	if msg == nil {
		return
	}

	kv["txID"] = msg.Id
	kv["remoteAddr"] = remoteAddr

	llog.Debug("handling master incoming message", kv)

	nextMsg, _ := transaction.IncomingMessage(msg)
	if nextMsg == nil {
		return
	}

	llog.Debug("writing back out master nextMsg", kv)

	marshalAndWrite(nextMsg, c, remoteAddr, kv)
}

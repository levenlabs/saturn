package main

import (
	"time"

	"net"
	"strings"

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
		go advertise()
		listenSpin()
	} else {
		llog.Info("starting as slave", llog.KV{"master": config.MasterAddr})
		reportSpin()
	}
}

func lookupMaster() *net.UDPAddr {
	//see if they passed a hostname without a port and then do a srv lookup
	masterAddr := config.MasterAddr
	_, _, err := net.SplitHostPort(config.MasterAddr)
	if err != nil && strings.Contains(err.Error(), "missing port") {
		masterAddr, err = srvclient.SRV(config.MasterAddr)
		if err != nil {
			llog.Fatal("error resolving master-addr using srv", llog.KV{"err": err, "addr": config.MasterAddr})
			return nil
		}
	}

	serverAddr, err := net.ResolveUDPAddr("udp", masterAddr)
	if err != nil {
		llog.Fatal("error resolving UDP addr", llog.KV{"err": err, "addr": masterAddr})
	}
	return serverAddr
}

func advertise() {
	if config.SkyAPIAddr != "" {
		skyapiAddr, err := srvclient.SRV(config.SkyAPIAddr)
		if err != nil {
			llog.Fatal("error resolving skyapi-addr", llog.KV{"err": err, "addr": config.SkyAPIAddr})
		}

		llog.Info("connecting to skyapi", llog.KV{"resolvedAddr": skyapiAddr, "thisAddr": config.ListenAddr})

		err = client.ProvideOpts(client.Opts{
			SkyAPIAddr:        skyapiAddr,
			Service:           "saturn",
			ThisAddr:          config.ListenAddr,
			ReconnectAttempts: 3,
		})
		llog.Fatal("skyapi giving up reconnecting", llog.KV{
			"addr":         config.SkyAPIAddr,
			"resolvedAddr": skyapiAddr,
			"err":          err,
		})
	}
}

func marshalAndWrite(msg proto.Message, c *net.UDPConn, dst *net.UDPAddr, kv llog.KV) bool {
	b, err := proto.Marshal(msg)
	if err != nil {
		kv["err"] = err
		llog.Error("error marshaling msg", kv)
		return false
	}

	if dst == nil {
		_, err = c.Write(b)
	} else {
		_, err = c.WriteToUDP(b, dst)
	}

	if err != nil {
		kv["err"] = err
		llog.Error("error writing msg", kv)
		return false
	}
	return true
}

func readAndUnmarshal(c *net.UDPConn, kv llog.KV) (*lproto.TxMsg, *net.UDPAddr, bool) {
	b := make([]byte, 1024)
	n, addr, err := c.ReadFromUDP(b)
	if err != nil {
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			// right now only the slave sets a ReadDeadline, but that's
			// expected so just debug
			llog.Debug("timed out reading from udp socket", kv)
		} else {
			kv["err"] = err
			llog.Error("error reading from udp socket", kv)
		}
		return nil, nil, false
	}

	var msg lproto.TxMsg
	if err := proto.Unmarshal(b[:n], &msg); err != nil {
		kv["err"] = err
		llog.Error("error unmarshaling proto msg", kv)
		return nil, nil, false
	}
	return &msg, addr, true
}

func reportSpin() {
	doSlaveReport()
	for _ = range time.Tick(time.Duration(config.Interval) * time.Second) {
		doSlaveReport()
	}
}

func doSlaveReport() {
	masterAddr := lookupMaster()
	kv := llog.KV{"addr": masterAddr}
	llog.Info("beginning transaction", kv)
	defer llog.Info("ended transaction", kv)

	c, err := net.DialUDP("udp", nil, masterAddr)
	if err != nil {
		kv["err"] = err
		llog.Error("error connecting to master", kv)
		return
	}
	defer c.Close()

	// We're using the localAddr as the prefix for the transaction ID
	addrStr := c.LocalAddr().String()
	ipStr, _, _ := net.SplitHostPort(addrStr)
	firstMsg := transaction.Initiate(addrStr, ipStr)
	kv["txID"] = firstMsg.Id
	if !marshalAndWrite(firstMsg, c, nil, kv) {
		return
	}

	// Todo: instead of relying on read timing out to know when the transaction
	// is done, we should be having the master send a "end" packet
	for {
		// Set a ReadDeadline so we don't wait forever if the master is dead or
		// ended the transaction
		c.SetReadDeadline(time.Now().Add(time.Second * 5))
		msg, _, ok := readAndUnmarshal(c, kv)
		if !ok {
			return
		}

		nextMsg := transaction.IncomingMessage(msg)
		if nextMsg == nil {
			return
		}
		if !marshalAndWrite(nextMsg, c, nil, kv) {
			return
		}
	}
}

func listenSpin() {
	kv := llog.KV{"addr": config.ListenAddr}
	llog.Info("listening on udp", kv)
	lAddr, err := net.ResolveUDPAddr("udp", config.ListenAddr)
	if err != nil {
		kv["err"] = err
		llog.Fatal("error resolving UDP addr", kv)
	}
	conn, err := net.ListenUDP("udp", lAddr)
	if err != nil {
		kv["err"] = err
		llog.Fatal("error listening to UDP port", kv)
	}

	for {
		doMasterInner(conn)
	}
}

func doMasterInner(c *net.UDPConn) {
	kv := llog.KV{"addr": config.ListenAddr}
	//don't set a ReadDeadline since we want it to block until a message
	msg, remoteAddr, ok := readAndUnmarshal(c, kv)
	if !ok {
		llog.Fatal("couldn't read from udp listen socket", kv)
	}

	kv["txID"] = msg.Id
	kv["remoteAddr"] = remoteAddr

	nextMsg := transaction.IncomingMessage(msg)
	if nextMsg == nil {
		return
	}

	marshalAndWrite(nextMsg, c, remoteAddr, kv)
}

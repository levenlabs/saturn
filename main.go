package main

import (
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/go-srvclient"
	"github.com/levenlabs/saturn/config"
	"github.com/levenlabs/saturn/sync"
	"github.com/mediocregopher/skyapi/client"
	"net"
	"strings"
)

func main() {
	llog.SetLevelFromString(config.LogLevel)
	if config.IsMaster {
		llog.Info("starting as master")
		go advertise()
	} else {
		masterAddr := lookupMaster()
		llog.Info("starting as slave", llog.KV{"master": config.MasterAddr, "addr": masterAddr})
		go reportSpin()
	}
	listenSpin()
}

func lookupMaster() string {
	//see if they passed a hostname without a port and then do a srv lookup
	masterAddr := config.MasterAddr
	_, _, err := net.SplitHostPort(config.MasterAddr)
	if err != nil && strings.Contains(err.Error(), "missing port") {
		masterAddr, err = srvclient.SRV(config.MasterAddr)
		if err != nil {
			llog.Fatal("error resolving master-addr using srv", llog.KV{"err": err, "addr": config.MasterAddr})
			return ""
		}
	}
	return masterAddr
}

func advertise() {
	if config.SkyAPIAddr != "" {
		skyapiAddr, err := srvclient.SRV(config.SkyAPIAddr)
		if err != nil {
			llog.Fatal("error resolving skyapi-addr", llog.KV{"err": err, "addr": config.SkyAPIAddr})
		}

		llog.Info("connecting to skyapi", llog.KV{"resolvedAddr": skyapiAddr, "thisAddr": config.ListenAddr})

		go func() {
			err := client.ProvideOpts(client.Opts{
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
		}()
	}
}

func reportSpin() {
	var masterAddr string
	var serverAddr *net.UDPAddr
	var err error
	for range time.Tick(10 * time.Second) {
		masterAddr = lookupMaster()
		serverAddr, err = net.ResolveUDPAddr("udp", masterAddr)
		if err != nil {
			llog.Fatal("error resolving UDP addr", llog.KV{"err": err, "addr": masterAddr})
		}
		llog.Info("sending report", llog.KV{"addr": serverAddr})
		sync.SendReport(serverAddr)
	}
}

func listenSpin() {
	listenAddr, err := net.ResolveUDPAddr("udp", config.ListenAddr)
	if err != nil {
		llog.Fatal("error resolving UDP addr", llog.KV{"err": err, "addr": config.ListenAddr})
	}
	llog.Info("listening on udp", llog.KV{"addr": config.ListenAddr})
	conn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		llog.Fatal("error listening to UDP port", llog.KV{"err": err, "addr": config.ListenAddr})
	}

	//todo: currently the average size of udp mesages is ~80 bytes so should we make this smaller?
	buf := make([]byte, 160)
	var n int
	var addr *net.UDPAddr
	for {
		n, addr, err = conn.ReadFromUDP(buf)
		sync.HandleMessage(buf[0:n], addr)
	}
}

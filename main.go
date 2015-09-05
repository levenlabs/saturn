package main

import (
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/go-srvclient"
	"github.com/levenlabs/saturn/config"
	"github.com/levenlabs/saturn/sync"
	"github.com/mediocregopher/skyapi/client"
	"net"
)

func main() {
	if config.IsMaster {
		llog.Info("starting as master")
		go advertise()
	} else {
		llog.Info("starting as slave", llog.KV{"master": config.MasterAddr})
		go reportSpin()
	}
	listenSpin()
}

func advertise() {
	if config.SkyAPIAddr != "" {
		skyapiAddr, err := srvclient.SRV(config.SkyAPIAddr)
		if err != nil {
			llog.Fatal("error resolving skyapi-addr", llog.KV{"err": err, "addr": config.SkyAPIAddr})
		}
		err = client.Provide(
			skyapiAddr, "saturn", config.ListenAddr, 1, 100,
			-1, 15*time.Second,
		)
		llog.Fatal("error providing to skyapi", llog.KV{"err": err})
	}
}

func reportSpin() {
	serverAddr, err := net.ResolveUDPAddr("udp", config.MasterAddr)
	if err != nil {
		llog.Fatal("error resolving UDP addr", llog.KV{"err": err, "addr": config.MasterAddr})
	}
	for {
		sync.SendReport(serverAddr)
		time.Sleep(time.Second * time.Duration(5))
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

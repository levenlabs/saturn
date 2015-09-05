package main

import (
	"time"

	"github.com/levenlabs/go-srvclient"
	"github.com/mediocregopher/skyapi/client"
	"github.com/levenlabs/saturn/config"
	"net"
	"github.com/levenlabs/go-llog"
	. "github.com/levenlabs/saturn/proto"
	"github.com/golang/protobuf/proto"
	"github.com/levenlabs/saturn/sync"
)

func main() {
	var msg proto.Message
	var fn func(proto.Message, *net.UDPAddr)
	if config.MasterAddr == "" {
		llog.Info("starting as master")
		go advertise()
		msg = &ReportRequest{}
		fn = sync.HandleReport
	} else {
		llog.Info("starting as slave", llog.KV{"master": config.MasterAddr})
		go reportSpin()
		msg = &ReportResponse{}
		fn = sync.HandleResponse
	}
	listenSpin(msg, fn)
}

func advertise() {
	if config.SkyAPIAddr != "" {
		skyapiAddr, err := srvclient.SRV(config.SkyAPIAddr)
		if err != nil {
			llog.Fatal("error resolving skyapi-addr", llog.KV{"err": err, "addr": config.SkyAPIAddr})
		}
		err = client.Provide(
			skyapiAddr, "saturn", config.ListenAddr, 1, 100,
			-1, 15 * time.Second,
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

func listenSpin(msg proto.Message, fn func(proto.Message, *net.UDPAddr)) {
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
	buf := make([]byte, 256)
	var n int
	var addr *net.UDPAddr
	for {
		n, addr, err = conn.ReadFromUDP(buf)
		err = proto.Unmarshal(buf[0:n], msg)
		if err != nil {
			llog.Fatal("unmarshaling error: ", llog.KV{"err": err, "n": n, "src": addr})
			continue
		}
		llog.Info("read udp", llog.KV{"n": n})
		fn(msg, addr)
	}
}

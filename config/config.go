// Package config parses command-line/environment/config file arguments and puts
// together the configuration of this instance, which is made available to other
// packages.
package config

import (
	"github.com/levenlabs/go-llog"
	"github.com/mediocregopher/lever"
)

// Configurable variables which are made available
var (
	ListenAddr string
	SkyAPIAddr string
	MasterAddr string
	Name       string
	Interval   int32
	HMACKey    []byte
	Iterations int32
	IsMaster   bool
	Threshold  float64
	LogLevel   string
)

func init() {
	l := lever.New("saturn", nil)
	l.Add(lever.Param{
		Name:        "--listen-addr",
		Description: "Address to listen on for requests and responses",
		Default:     ":4123",
	})
	l.Add(lever.Param{
		Name:        "--skyapi-addr",
		Description: "Hostname of skyapi, to be looked up via a SRV request. Unset means don't register with skyapi",
	})
	l.Add(lever.Param{
		Name:        "--master-addr",
		Description: "The master to connect to. If empty, then this node assumes its the master. If just a hostname, it will try to do a SRV request.",
	})
	l.Add(lever.Param{
		Name:        "--name",
		Description: "The name to report to the master. If none is sent, the master uses IP.",
	})
	l.Add(lever.Param{
		Name:        "--hmac-key",
		Description: "The hmac key for each report request. Must be the same across all nodes!",
		Default:     "secret",
	})
	l.Add(lever.Param{
		Name:        "--rounds",
		Description: "The number of offsets used to calculate the slave's actual offset.",
		Default:     "5",
	})
	l.Add(lever.Param{
		Name:        "--interval",
		Description: "The number of seconds to wait between each report. Should be at least 10 seconds.",
		Default:     "180",
	})
	l.Add(lever.Param{
		Name:        "--threshold",
		Description: "The threshold in milliseconds for reporting a server",
		Default:     "5000",
	})
	l.Add(lever.Param{
		Name:        "--log-level",
		Description: "Adjust the log level. Valid options are: error, warn, info, debug",
		Default:     "warn",
	})
	l.Parse()

	ListenAddr, _ = l.ParamStr("--listen-addr")
	SkyAPIAddr, _ = l.ParamStr("--skyapi-addr")
	MasterAddr, _ = l.ParamStr("--master-addr")
	if MasterAddr == "" {
		IsMaster = true
	}
	Name, _ = l.ParamStr("--name")
	k, _ := l.ParamStr("--hmac-key")
	HMACKey = []byte(k)
	i, _ := l.ParamInt("--rounds")
	Iterations = int32(i)
	i, _ = l.ParamInt("--interval")
	Interval = int32(i)
	i, _ = l.ParamInt("--threshold")
	Threshold = float64(i)
	LogLevel, _ = l.ParamStr("--log-level")

	llog.SetLevelFromString(LogLevel)
}

package conf

import (
	// "flag"

	"flag"

	"github.com/BurntSushi/toml"
)

type Server struct {
	Username string
	Passwd   string
	IP       string
	Proto    string
	SQL      string
}

type Taos struct {
	Database   string
	Table      string
	Stable     string
	Condition  string
	FileName   string
	MarketType string
	Symbol     string
}

type Config struct {
	Server *Server
	Taos   *Taos
}

var (
	confPath string
	Conf     *Config
)

func init() {
	flag.StringVar(&confPath, "conf", "../repairData.toml", "config path")
}

func Init() (err error) {
	// flag.StringVar(&confPath, "conf", "repairData.toml", "config path")
	// confPath = "../repairData.toml"
	_, err = toml.DecodeFile(confPath, &Conf)
	return
}

package conf

import (
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
	Database string
	Table    string
	Stable   string
	Word     string
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
	flag.StringVar(&confPath, "conf", "push.toml", "config path")
}

func Init() (err error) {
	_, err = toml.DecodeFile(confPath, &Conf)
	return
}

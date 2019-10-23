package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strconv"
	"time"

	_ "bailun.com/CT4_quote_server/lib/taosSql"

	"bailun.com/CT4_quote_server/DataRepairTool/conf"

	"bailun.com/CT4_quote_server/DataRepairTool/Server"

	"github.com/astaxie/beego/logs"
)

var row, nrow []string
var err error

func int2string(num int) string {
	str := strconv.Itoa(num)
	return str
}

func string2int(str string) int {
	num, err := strconv.Atoi(str)
	if err != nil {
		logs.Error(str)
		logs.Error(err)
	}
	return num
}

func int64Tostring(num int64) string {
	str := strconv.FormatInt(num, 10)
	return str
}

func use_database(db *sql.DB, demodb string) {
	st := time.Now().Nanosecond()
	// use database
	res, err := db.Exec("use " + demodb) // notes: must no quote to db name
	if err != nil {
		logs.Error(demodb)
		logs.Error(err)
		return
	}

	affectd, err := res.RowsAffected()
	if err != nil {
		logs.Error(err)
	}

	et := time.Now().Nanosecond()
	logs.Debug("use database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func main() {
	logs.EnableFuncCallDepth(true)
	logs.SetLogFuncCallDepth(3)

	flag.Parse()
	if err = conf.Init(); err != nil {
		logs.Error(err)
		return
	}

	dns := fmt.Sprintf("%s:%s%s(%s)/", conf.Conf.Server.Username, conf.Conf.Server.Passwd, conf.Conf.Server.Proto, conf.Conf.Server.IP)
	// logs.Debug(dns)
	// db, err := sql.Open("taosSql", "root:taosdata@/tcp(192.168.1.119:0)/")
	db, err := sql.Open(conf.Conf.Server.SQL, dns)
	if err != nil {
		logs.Error("Open database error: %s\n", err)
		return
	} else {
		logs.Debug("connect successful")
	}
	defer db.Close()

	// demotStmt := "globaldb"

	demotStmt := conf.Conf.Taos.Database
	table := conf.Conf.Taos.Table
	fileName := conf.Conf.Taos.FileName

	Server.Use_database(db, demotStmt)
	Server.Select_data(db, table, fileName)

}

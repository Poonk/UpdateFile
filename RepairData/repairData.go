package main

import (
	"DataRepairTool/Server"
	"DataRepairTool/conf"
	"database/sql"
	"fmt"
	_ "taosSql"

	"github.com/astaxie/beego/logs"
)

func main() {
	logs.EnableFuncCallDepth(true)
	logs.SetLogFuncCallDepth(3)

	if err := conf.Init(); err != nil {
		logs.Error(err)
		return
	}

	dns := fmt.Sprintf("%s:%s%s(%s)/", conf.Conf.Server.Username, conf.Conf.Server.Passwd, conf.Conf.Server.Proto, conf.Conf.Server.IP)
	// logs.Debug(dns)
	// db, err := sql.Open("taosSql", "root:taosdata@/tcp(192.168.1.119:0)/")
	db, err := sql.Open(conf.Conf.Server.SQL, dns)
	if err != nil {
		logs.Error(conf.Conf.Server.SQL, "\t", dns)
		logs.Error("Open database error: %s\n", err)
		return
	} else {
		logs.Debug("connect successful")
	}
	defer db.Close()

	// demotStmt := "globaldb"

	demotStmt := conf.Conf.Taos.Database
	table := conf.Conf.Taos.Table
	stable := conf.Conf.Taos.Stable
	fileName := conf.Conf.Taos.FileName

	// table := "stock_30m"
	// stable := "h4_5103_spx"
	// stable := "h4_5102_c20"
	// stable := "h4_5101_xin9"
	// stable := "h4_5101_xin0"
	// stable := "m1_5103_ndx"
	// stable := "h4_5101_hsi"

	Server.Use_database(db, demotStmt)
	// Server.Select_data_stmt(db, table)
	Server.Select_data(db, table, "")
	Server.Drop_table_stmt(db, table)
	Server.UpdateFile(table, fileName)
	Server.Insert_data_stmt(db, table, stable, conf.Conf.Taos.MarketType, conf.Conf.Taos.Symbol)

}

package main

import (
	"DataRepairTool/conf"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	_ "taosSql"
	"time"

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
		logs.Error(err)
	}

	affectd, err := res.RowsAffected()
	if err != nil {
		logs.Error(err)
	}

	et := time.Now().Nanosecond()
	logs.Debug("use database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

//将子表拉到本地
func select_data_stmt(db *sql.DB, demot string) {
	st := time.Now().Nanosecond()

	oldFileName := demot + ".csv" //"oldFile.csv"
	// wfs, err := os.OpenFile(oldFileName, os.O_RDWR|os.O_CREATE, 0666)
	wfs, err := os.Create(oldFileName)
	if err != nil {
		logs.Error("can not create file, err is %+v", err)
	}
	defer wfs.Close()

	wfs.Seek(0, io.SeekEnd)
	w := csv.NewWriter(wfs)
	w.Comma = ','
	w.UseCRLF = true

	rows, err := db.Query("select * from ? ?", demot, conf.Conf.Taos.Word) // go text mode
	if err != nil {
		logs.Error(err)
	}

	// logs.Debug("%10s%s%8s %5s %9s%s %s %8s%s %7s%s %8s%s %11s%s %14s%s\n", " ", "ts", " ", "date", " ", "time", " ", "open", " ", "high", " ", "low", " ", "close", " ", "vol"," ", "dv")
	var affectd int
	for rows.Next() {
		var oldContent []string
		var ts string
		var date int
		var time int
		var open int64
		var high int64
		var low int64
		var close int64
		var vol int64
		var turnover int64
		var pre_close int64
		// var market_type string
		// var symbol string

		err = rows.Scan(&ts, &date, &time, &open, &high, &low, &close, &vol, &turnover, &pre_close) //, &market_type, &symbol)
		oldContent = append(oldContent, ts)
		// w.Write(oldContent)
		oldContent = append(oldContent, int2string(date))
		// w.Write(oldContent)
		oldContent = append(oldContent, int2string(time))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(open))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(high))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(low))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(close))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(vol))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(turnover))
		// w.Write(oldContent)
		oldContent = append(oldContent, int64Tostring(pre_close))
		// w.Write(oldContent)
		//fmt.Println("start scan fields from row.rs, &fv:", &fv)
		//err = rows.Scan(&fv)
		if err != nil {
			logs.Error(err)
		}

		// logs.Debug(ts, "\t", date, "\t", time, "\t", open, "\t", high, "\t", low, "\t", close, "\t", vol, "\t", turnover, "\t", pre_close) //, "\t", market_type, "\t", symbol)
		logs.Debug(oldContent)
		w.Write(oldContent)
		w.Flush()
		affectd++

	}
	// logs.Debug(oldContent)
	// w.Write(oldContent)

	et := time.Now().Nanosecond()
	logs.Debug("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func main() {
	logs.EnableFuncCallDepth(true)
	logs.SetLogFuncCallDepth(3)

	if err = conf.Init(); err != nil {
		logs.Error(err)
	}

	dns := fmt.Sprintf("%s:%s%s(%s)/", conf.Conf.Server.Username, conf.Conf.Server.Passwd, conf.Conf.Server.Proto, conf.Conf.Server.IP)
	// logs.Debug(dns)
	// db, err := sql.Open("taosSql", "root:taosdata@/tcp(192.168.1.119:0)/")
	db, err := sql.Open(conf.Conf.Server.SQL, dns)
	if err != nil {
		logs.Error("Open database error: %s\n", err)
	} else {
		logs.Debug("connect successful")
	}
	defer db.Close()

	// demotStmt := "globaldb"

	demotStmt := conf.Conf.Taos.Database
	table := conf.Conf.Taos.Table

	// table := "stock_30m"
	// stable := "h4_5103_spx"
	// stable := "h4_5102_c20"
	// stable := "h4_5101_xin9"
	// stable := "h4_5101_xin0"
	// stable := "m1_5103_ndx"
	// stable := "h4_5101_hsi"

	use_database(db, demotStmt)
	select_data_stmt(db, table)

}

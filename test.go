package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	_ "taosSql"
	"testCsv/conf"
	"time"

	"github.com/astaxie/beego/logs"
)

var a string
var old, new int
var row, nrow []string
var newContent [][]string
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

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func updateFile(stable string) {
	// st := time.Now().Nanosecond()

	fileName := stable + ".csv" //"oldFile.csv"
	nfileName := "newFile.csv"

	// fs := openFile(fileName)
	// nfs := openFile(nfileName)

	fs, err := os.Open(fileName)
	if err != nil {
		logs.Error("can not open the file, err is %+v", err)
	}
	defer fs.Close()

	nfs, err := os.Open(nfileName)
	if err != nil {
		logs.Error("can not open the file, err is %+v", err)
	}
	defer nfs.Close()

	r := csv.NewReader(fs)
	rr := csv.NewReader(nfs)

	row, err := r.Read()
	if err != nil && err != io.EOF {
		logs.Error("can not read, err is %+v", err)
	}
	if err == io.EOF {
		logs.Error(err)
	}
	// a := row[0]
	// old, err := strconv.Atoi(a)

	// old = getNew(r)
	// new := getNew(rr)

	nrow, err := rr.Read()
	if err != nil && err != io.EOF {
		logs.Error("can not read, err is %+v", err)
	}
	if err == io.EOF {
		logs.Error(err)
	}
	// b := nrow[0]
	// new, err := strconv.Atoi(b)

	logs.Debug(row[0], "\t", nrow[0])
	// logs.Debug(old, "\t", new)

	// if a > b {
	// 	logs.Debug("a")
	// } else {
	// 	logs.Debug("b")
	// }

	newFileName := "resultFile.csv"
	wfs, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		logs.Error("can not create file, err is %+v", err)
	}
	defer wfs.Close()

	wfs.Seek(0, io.SeekEnd)
	w := csv.NewWriter(wfs)
	w.Comma = ','
	w.UseCRLF = true

	flag, nflag := false, false

	//针对大文件，一行一行的读取文件
	for {
		logs.Debug(row[0], "\t", nrow[0])
		if !flag && !nflag {
			if row[0] > nrow[0] {
				newContent = append(newContent, nrow)
				logs.Debug(nrow[0])
				t := nrow
				nrow, err = rr.Read()
				if err != nil && err != io.EOF {
					logs.Error("can not read, err is %+v", err)
				}
				if err == io.EOF {
					nrow = t
					nflag = true
					logs.Error(err)
				}
				// b := nrow[0]
				// new, err = strconv.Atoi(b)
				// new = getNew(rr)
				// logs.Debug(new, "\t", old)

			} else if row[0] < nrow[0] {
				newContent = append(newContent, row)
				logs.Debug(row[0])
				tmp := row
				row, err = r.Read()
				if err != nil && err != io.EOF {
					logs.Error("can not read, err is %+v", err)
				}
				if err == io.EOF {
					row = tmp
					flag = true
					logs.Error(err)
				}
				// a := row[0]
				// old, err = strconv.Atoi(a)
				// old = getNew(r)
			} else if row[0] == nrow[0] {
				// for i := 0; i < min(len(row), len(nrow)); i++ {
				// 	logs.Debug(i, "\t", row[i], "\t", nrow[i])
				// }
				newContent = append(newContent, nrow)
				logs.Debug(nrow[0])
				t := nrow
				nrow, err = rr.Read()
				if err != nil && err != io.EOF {
					logs.Error("can not read, err is %+v", err)
				}
				if err == io.EOF {
					nrow = t
					nflag = true
					logs.Error(err)
				}
				tmp := row
				row, err = r.Read()
				if err != nil && err != io.EOF {
					logs.Error("can not read, err is %+v", err)
				}
				if err == io.EOF {
					row = tmp
					flag = true
					logs.Error(err)
				}
			}
		} else if flag && nflag {
			break
		} else if flag == true {
			newContent = append(newContent, nrow)
			logs.Debug(nrow[0])
			t := nrow
			nrow, err = rr.Read()
			if err != nil && err != io.EOF {
				logs.Error("can not read, err is %+v", err)
			}
			if err == io.EOF {
				nrow = t
				nflag = true
				logs.Error(err)
			}
		} else if nflag == true {
			newContent = append(newContent, row)
			logs.Debug(row[0])
			tmp := row
			row, err = r.Read()
			if err != nil && err != io.EOF {
				logs.Error("can not read, err is %+v", err)
			}
			if err == io.EOF {
				row = tmp
				flag = true
				logs.Error(err)
			}
		}
	}
	w.WriteAll(newContent)
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
	wfs, err := os.OpenFile(oldFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		logs.Error("can not create file, err is %+v", err)
	}
	defer wfs.Close()

	wfs.Seek(0, io.SeekEnd)
	w := csv.NewWriter(wfs)
	w.Comma = ','
	w.UseCRLF = true

	rows, err := db.Query("select * from ?", demot) // go text mode
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

		// logs.Debug("%s\t", ts)
		// logs.Debug("%d\t", id)
		// logs.Debug("%10s\t", name)
		// logs.Debug("%d\t", len)
		// logs.Debug("%t\t", flag)
		// logs.Debug("%s\t", notes)
		// logs.Debug("%06.3f\t", fv)
		// logs.Debug("%09.6f\n", dv)
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

func drop_table_stmt(db *sql.DB, demodb string) {
	st := time.Now().Nanosecond()
	// drop test db
	stmt, err := db.Prepare("drop table ?")
	if err != nil {
		logs.Error(err)
	}
	defer stmt.Close()

	res, err := stmt.Exec(demodb)
	if err != nil {
		logs.Error(err)
	}

	affectd, err := res.RowsAffected()
	if err != nil {
		logs.Error(err)
	}

	et := time.Now().Nanosecond()
	fmt.Printf("drop table result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func insert_data_stmt(db *sql.DB, demot string, tableName string) {
	st := time.Now().Nanosecond()

	fileName := "resultFile.csv"
	fs, err := os.Open(fileName)
	if err != nil {
		logs.Error("can not open the file, err is %+v", err)
	}
	defer fs.Close()

	r := csv.NewReader(fs)

	// stmt, err := db.Prepare("insert into ? using stock_4h tags (?,?) values ('?', ?, ?, ?, ?, ?, ?, ?, ?, ?")
	// // insert into ? using stock_4h tags(`5102`,'fchi') values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	// if err != nil {
	// 	logs.Error(err)
	// }

	var res sql.Result
	var t []string
	flag := false
	// insert data into table
	for {
		if !flag {
			row, err = r.Read()
			if err != nil && err != io.EOF {
				logs.Error("can not read, err is %+v", err)
			}
			if err == io.EOF {
				flag = true
				row = t
				// logs.Error(err)
			}
			logs.Debug(row)
			t = row
			// for j := 0; j < (len(row))-1; j++ {
			// 	logs.Debug(j, "\t", row[j])
			// }

			tmp := fmt.Sprintf("insert into %s using %s tags ('%s','%s') values ('%s', %d, %d, %d, %d, %d, %d, %d, %d, %d)",
				demot, tableName, "5102", "fchi", row[0], string2int(row[1]), string2int(row[2]), string2int(row[3]), string2int(row[4]), string2int(row[5]),
				string2int(row[6]), string2int(row[7]), string2int(row[8]), string2int(row[9]))
			logs.Debug(tmp)

			// res, err = stmt.Exec(demot, "5102", "fchi", row[0], string2int(row[1]), string2int(row[2]),
			// 	string2int(row[3]), string2int(row[4]), string2int(row[5]), string2int(row[6]), string2int(row[7]),
			// 	string2int(row[8]), string2int(row[9]))
			res, err = db.Exec(tmp)
			if err != nil {
				logs.Error(err)
			}
		} else {
			break
		}
	}

	affectd, err := res.RowsAffected()
	if err != nil {
		logs.Error(err)
	}

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
	var demotStmt string
	var stable string
	var table string
	logs.Debug("Databases:")
	fmt.Scanln(&demotStmt)

	logs.Debug("Stable:")
	fmt.Scanln(&stable)

	logs.Debug("Table")
	fmt.Scanln(&table)

	logs.Debug("databases: ", demotStmt, "\t", "stable: ", stable, "\t", "table: ", table)

	// table := "stock_30m"
	// stable := "h4_5103_spx"
	// stable := "h4_5102_c20"
	// stable := "h4_5101_xin9"
	// stable := "h4_5101_xin0"
	// stable := "m1_5103_ndx"
	// stable := "h4_5101_hsi"

	use_database(db, demotStmt)
	select_data_stmt(db, stable)
	drop_table_stmt(db, stable)
	updateFile(stable)
	insert_data_stmt(db, stable, tableName)

}

//32 + 11

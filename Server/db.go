package Server

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"bailun.com/CT4_quote_server/DataRepairTool/conf"

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
		logs.Error(str)
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

func Use_database(db *sql.DB, demodb string) {
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
		return
	}

	et := time.Now().Nanosecond()
	logs.Debug("use database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

//将子表拉到本地
func Select_data(db *sql.DB, demot string, fileName string) {
	st := time.Now().Nanosecond()
	var oldFileName string
	var rows *sql.Rows
	if fileName == "" {
		oldFileName = demot + ".csv" //"oldFile.csv"
	} else {
		oldFileName = fileName + ".csv" //"oldFile.csv"
	}
	// wfs, err := os.OpenFile(oldFileName, os.O_RDWR|os.O_CREATE, 0666)
	wfs, err := os.Create("../file/" + oldFileName)
	if err != nil {
		logs.Error("can not create file, err is %+v", err)
		return
	}
	defer wfs.Close()

	wfs.Seek(0, io.SeekEnd)
	w := csv.NewWriter(wfs)
	w.Comma = ','
	w.UseCRLF = true
	if fileName == "" {
		rows, err = db.Query("select * from ?", demot) // go text mode
	} else {
		rows, err = db.Query("select * from ? ?", demot, conf.Conf.Taos.Condition) // go text mode
	}
	if err != nil {
		logs.Error(err)
		return
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
			return
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

func Drop_table_stmt(db *sql.DB, demodb string) {
	st := time.Now().Nanosecond()
	// drop test db
	stmt, err := db.Prepare("drop table ?")
	if err != nil {

		logs.Error(err)
		return
	}
	defer stmt.Close()

	res, err := stmt.Exec(demodb)
	if err != nil {
		logs.Error(demodb)
		logs.Error(err)
		return
	}

	affectd, err := res.RowsAffected()
	if err != nil {
		logs.Error(err)
		return
	}

	et := time.Now().Nanosecond()
	fmt.Printf("drop table result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func Insert_data_stmt(db *sql.DB, demot string, tableName string, marketType string, symbol string) {
	st := time.Now().Nanosecond()

	fileName := "resultFile.csv"
	fs, err := os.Open("../file/" + fileName)
	if err != nil {
		logs.Error(fileName)
		logs.Error("can not open the file, err is %+v", err)
		return
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
				return
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
				demot, tableName, marketType, symbol, row[0], string2int(row[1]), string2int(row[2]), string2int(row[3]), string2int(row[4]), string2int(row[5]),
				string2int(row[6]), string2int(row[7]), string2int(row[8]), string2int(row[9]))
			logs.Debug(tmp)

			// res, err = stmt.Exec(demot, "5102", "fchi", row[0], string2int(row[1]), string2int(row[2]),
			// 	string2int(row[3]), string2int(row[4]), string2int(row[5]), string2int(row[6]), string2int(row[7]),
			// 	string2int(row[8]), string2int(row[9]))
			res, err = db.Exec(tmp)
			if err != nil {
				logs.Error(err)
				return
			}
		} else {
			break
		}
	}

	affectd, err := res.RowsAffected()
	if err != nil {
		logs.Error(err)
		return
	}

	et := time.Now().Nanosecond()
	logs.Debug("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1E9)
}

func UpdateFile(stable string, file string) {
	// st := time.Now().Nanosecond()

	fileName := stable + ".csv" //"oldFile.csv"
	nfileName := file + ".csv"

	// fs := openFile(fileName)
	// nfs := openFile(nfileName)

	fs, err := os.Open("../file/" + fileName)
	if err != nil {
		logs.Error("can not open the file, err is %+v", err)
		return
	}
	defer fs.Close()

	nfs, err := os.Open("../file/" + nfileName)
	if err != nil {
		logs.Error("can not open the file, err is %+v", err)
		return
	}
	defer nfs.Close()

	r := csv.NewReader(fs)
	rr := csv.NewReader(nfs)

	row, err := r.Read()
	if err != nil && err != io.EOF {
		logs.Error("can not read, err is %+v", err)
		return
	}
	if err == io.EOF {
		logs.Error(err)
		return
	}

	nrow, err := rr.Read()
	if err != nil && err != io.EOF {
		logs.Error("can not read, err is %+v", err)
		return
	}
	if err == io.EOF {
		logs.Error(err)
		return
	}

	logs.Debug(row[0], "\t", nrow[0])

	newFileName := "resultFile.csv"
	// wfs, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE, 0666)
	wfs, err := os.Create("../file/" + newFileName)
	if err != nil {
		logs.Error("can not create file, err is %+v", err)
		return
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
					return
				}
				if err == io.EOF {
					nrow = t
					nflag = true
					logs.Error(err)
				}

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
					return
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

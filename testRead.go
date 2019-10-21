package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/astaxie/beego/logs"
)

// func openFile(fileName string) *os.File {
// 	fs, err := os.Open(fileName)
// 	if err != nil {
// 		logs.Error("can not open the file, err is %+v", err)
// 	}
// 	defer fs.Close()
// 	return fs
// }

// func getNew(r io.Reader) int {
// 	row, err := r.Read()
// 	if err != nil && err != io.EOF {
// 		logs.Error("can not read, err is %+v", err)
// 	}
// 	if err == io.EOF {
// 		logs.Error(err)
// 	}
// 	a := row[0]
// 	var number int
// 	number, err = strconv.Atoi(a)
// 	return number
// }

var newContent [][]string

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func updateFile() {
	// st := time.Now().Nanosecond()

	fileName := "oldFile.csv"
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

func main() {
	logs.EnableFuncCallDepth(true)
	logs.SetLogFuncCallDepth(3)

	// fileName := "oldFile.csv"
	// fs, err := os.Open(fileName)
	// if err != nil {
	// 	logs.Error("can not open the file, err is %+v", err)
	// }
	// defer fs.Close()
	// r := csv.NewReader(fs)

	// for i := 0; i <= 10; i++ {
	// 	row, err := r.Read()
	// 	if err != nil && err != io.EOF {
	// 		logs.Error("can not read, err is %+v", err)
	// 	}
	// 	if err == io.EOF {
	// 		logs.Error(err)
	// 	}
	// 	logs.Debug(row)
	// 	for i := 0; i < (len(row)); i++ {
	// 		logs.Debug(i, "\t", row[i])
	// 	}
	// }

	// updateFile()
	for {
		var a, b int
		fmt.Scanln(&a, &b)
		logs.Debug(a, " + ", b, " = ", a+b)
	}
}

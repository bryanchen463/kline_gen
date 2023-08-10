package main

import (
	"archive/zip"
	"bufio"
	"flag"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

var date = flag.String("date", time.Now().Format(DateLayout), "which day's kline date to generator")
var dir = flag.String("dir", "/share", "save direction")
var aggTradeDir = flag.String("aggTradeBaseDir", "/share/agg_database/bn", "agg trade diretion")

func parseZipFile(filename string, kline *Kline) {
	var fullPath string
	if kline.Type == SPOT {
		fullPath = path.Join(*aggTradeDir, "spot", *date, filename)
	} else if kline.Type == Future {
		fullPath = path.Join(*aggTradeDir, "um", *date, filename)
	}

	zipFile, err := zip.OpenReader(fullPath)
	if err != nil {
		log.Fatal(err)
	}
	defer zipFile.Close()

	symbol := strings.Split(filename, "-aggTrades-")[0]
	if len(zipFile.File) == 1 {
		file := zipFile.File[0]
		log.Println("文件名：", file.Name)

		// 打开zip文件中的文件
		fileReader, err := file.Open()
		if err != nil {
			log.Fatal(err)
		}
		defer fileReader.Close()

		scanner := bufio.NewScanner(fileReader)
		var content string
		scanner.Scan()
		for scanner.Scan() {
			content = scanner.Text()
			trade := parseTrade(content)
			kline.AddTrade(symbol, trade)
		}
	} else {
		log.Fatal("unexpect zip file count")
	}
}

func run(kline Kline) {
	var aggDir string
	if kline.Type == SPOT {
		aggDir = path.Join(*aggTradeDir, "spot", *date)
	} else if kline.Type == Future {
		aggDir = path.Join(*aggTradeDir, "um", *date)
	}

	log.Println(aggDir)
	files, err := os.ReadDir(aggDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			parseZipFile(file.Name(), &kline)
		}
	}
	kline.Save()
}

func main() {
	flag.Parse()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		kline := NewKline(*date, SPOT)
		run(*kline)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		kline := NewKline(*date, Future)
		run(*kline)
		wg.Done()
	}()
	wg.Wait()
}

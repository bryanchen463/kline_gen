package main

import (
	"archive/zip"
	"bufio"
	"flag"
	"log"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v2"
)

var date = flag.String("date", "2023-07-01", "which day's kline date to generator")
var dir = flag.String("dir", "/share", "save direction")
var aggTradeDir = flag.String("aggTradeBaseDir", "/share/agg_database", "agg trade diretion")
var kline *Kline

func parseZipFile(filename string) {
	var fullPath string
	if config.Type == SPOT {
		fullPath = path.Join(*aggTradeDir, "spot", *date, filename)
	} else if config.Type == Future {
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

func run() {
	var aggDir string
	if config.Type == SPOT {
		aggDir = path.Join(*aggTradeDir, "spot", *date)
	} else if config.Type == Future {
		aggDir = path.Join(*aggTradeDir, "um", *date)
	}

	log.Println(aggDir)
	files, err := os.ReadDir(aggDir)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			parseZipFile(file.Name())
		}
	}
	kline.Save()
}

func parseConfig() {
	file, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(file, &config)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	flag.Parse()
	parseConfig()
	kline = NewKline(*date)
	run()
}

package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Category int

func (c *Category) string() string {
	if *c == SPOT {
		return "spot"
	} else if *c == Future {
		return "um"
	}
	return ""
}

const (
	SPOT Category = iota
	Future
)

const (
	AggTradeBaseDir = `C:\Users\dell\AppData\Roaming\MobaXterm\slash\RemoteFiles\66940_3_0`
	DateLayout      = "2006-01-02"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type Trade struct {
	TradeId   uint64
	Price     float64
	Vol       float64
	TimeStamp uint64
	IsSell    bool
}

func parseTrade(content string) Trade {
	fields := strings.Split(content, ",")
	tradeId, err := strconv.ParseUint(fields[0], 10, 64)
	checkErr(err)
	prices, err := strconv.ParseFloat(fields[1], 64)
	checkErr(err)
	vol, err := strconv.ParseFloat(fields[2], 64)
	checkErr(err)
	isSell, err := strconv.ParseBool(fields[6])
	checkErr(err)
	tradeTime, err := strconv.ParseUint(fields[5], 10, 64)
	checkErr(err)
	trade := Trade{
		TradeId:   tradeId,
		Price:     prices,
		Vol:       vol,
		TimeStamp: tradeTime / 1000,
		IsSell:    isSell,
	}
	return trade
}

type KlineData struct {
	Symbol    string
	TimeStamp uint64
	Open      float64
	High      float64
	Low       float64
	Close     float64
	BuyVol    float64
	SellVol   float64
}

func (k *KlineData) Update(trade Trade) {
	if k.TimeStamp == 0 {
		k.High = trade.Price
		k.Low = trade.Price
		k.Open = trade.Price
	} else {
		if k.High < trade.Price {
			k.High = trade.Price
		}
		if k.Low > trade.Price {
			k.Low = trade.Price
		}
	}
	k.Close = trade.Price
	buyVol, sellVol := 0.0, 0.0
	if trade.IsSell {
		sellVol = trade.Vol
	} else {
		buyVol = trade.Vol
	}
	k.TimeStamp = trade.TimeStamp
	k.BuyVol += buyVol
	k.SellVol += sellVol
}

type KlineDatas struct {
	Datas  [1440]KlineData
	Type   Category
	Date   time.Time
	Symbol string
}

func (k *KlineDatas) getPrevDayKlineData() KlineData {
	prevDay := k.Date.Add(-24 * time.Hour)

	filename := fmt.Sprintf("%s-aggTrades-%s.zip", k.Symbol, prevDay.Format(DateLayout))
	if k.Type == SPOT {
		filename = path.Join(AggTradeBaseDir, "spot", prevDay.Format(DateLayout), filename)
	} else if k.Type == Future {
		filename = path.Join(AggTradeBaseDir, "um", prevDay.Format(DateLayout), filename)
	}

	zipFile, err := zip.OpenReader(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer zipFile.Close()

	var klineData KlineData
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
		for scanner.Scan() {
			content = scanner.Text()
		}
		trade := parseTrade(content)
		klineData = KlineData{
			Symbol:    k.Symbol,
			TimeStamp: trade.TimeStamp,
			Open:      trade.Price,
			Close:     trade.Price,
			High:      trade.Price,
			Low:       trade.Price,
		}
	} else {
		log.Fatal("unexpect zip file count")
	}
	return klineData
}

func (k *KlineDatas) getPrevData(minutes int) (KlineData, int) {
	for i := minutes; i >= 0; i-- {
		if k.Datas[i].TimeStamp != 0 {
			return k.Datas[i], i
		}
	}
	return k.getPrevDayKlineData(), -1
}

func (k *KlineDatas) fillPrevData(minutes int) {
	prevData, index := k.getPrevData(minutes)
	for i := index + 1; i < minutes; i++ {
		k.Datas[i] = prevData
	}
}

func (k *KlineDatas) AddTrade(trade Trade) {
	tradeTime := time.Unix(int64(trade.TimeStamp), 0)
	minutes := int(tradeTime.Sub(k.Date).Minutes())
	klineData := k.Datas[minutes]
	klineData.Update(trade)
	k.Datas[minutes] = klineData
	k.fillPrevData(minutes)
}

type Kline struct {
	SymbolData map[string]KlineDatas
	Date       time.Time
}

func NewKline(dt string) *Kline {
	d, _ := time.Parse(DateLayout, dt)
	return &Kline{
		SymbolData: make(map[string]KlineDatas),
		Date:       d,
	}
}

func (k *Kline) AddTrade(symbol string, trade Trade) {
	klineDatas, ok := k.SymbolData[symbol]
	if !ok {
		tradeTime := time.Unix(int64(trade.TimeStamp), 0)
		klineDatas = KlineDatas{
			Type:   config.Type,
			Date:   time.Date(tradeTime.Year(), tradeTime.Month(), tradeTime.Day(), 8, 0, 0, 0, time.Local),
			Symbol: symbol,
		}
	}
	klineDatas.AddTrade(trade)
	k.SymbolData[symbol] = klineDatas
}

func (k *Kline) FillData() {
	for _, klineDatas := range k.SymbolData {
		klineDatas.fillPrevData(1441)
	}
}

func (k *Kline) Save() {
	var kpath string

	if config.Type == SPOT {

		kpath = "spotkdata/"
	} else {

		kpath = "kdata/"
	}

	name := path.Join(kpath, "open/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_Open.csv")
	WriteToFile(name, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.Open, 'f', -1, 64)
	})

	name1 := path.Join(kpath, "high/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_High.csv")
	WriteToFile(name1, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.High, 'f', -1, 64)
	})

	name2 := path.Join(kpath, "low/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_Low.csv")
	WriteToFile(name2, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.Low, 'f', -1, 64)
	})

	name3 := path.Join(kpath, "close/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_Close.csv")
	WriteToFile(name3, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.Close, 'f', -1, 64)
	})

	name4 := path.Join(kpath, "buyvol/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_BuyVol.csv")
	WriteToFile(name4, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.BuyVol, 'f', -1, 64)
	})

	name5 := path.Join(kpath, "sellvol/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_SellVol.csv")
	WriteToFile(name5, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.SellVol, 'f', -1, 64)
	})
}

func WriteToFile(filename string, data Kline, valueFunc func(KlineData) string) {
	dir := path.Dir(filename)
	os.MkdirAll(dir, os.ModeDir)
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Extract symbols
	symbols := make([]string, 0, len(data.SymbolData))
	for symbol := range data.SymbolData {
		symbols = append(symbols, symbol)
	}

	// Write header
	writer.Write(append([]string{"Timestamp"}, symbols...))

	fmt.Println("长度为=", len(data.SymbolData))
	for i, v := range data.SymbolData[symbols[0]].Datas {
		row := make([]string, len(symbols)+1)
		row[0] = strconv.FormatFloat(float64(v.TimeStamp), 'f', 0, 64)
		for j, symbol := range symbols {
			row[j+1] = valueFunc(data.SymbolData[symbol].Datas[i])
		}
		writer.Write(row)
	}
}

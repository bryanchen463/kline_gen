package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
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
	DateLayout = "2006-01-02"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type Trade struct {
	TradeId   uint64
	Price     decimal.Decimal
	Vol       decimal.Decimal
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
		Price:     decimal.NewFromFloat(prices),
		Vol:       decimal.NewFromFloat(vol),
		TimeStamp: tradeTime / 1000,
		IsSell:    isSell,
	}
	return trade
}

type KlineData struct {
	Symbol    string
	TimeStamp uint64
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal
	Price     decimal.Decimal
	BuyVol    decimal.Decimal
	SellVol   decimal.Decimal
	Turnover  decimal.Decimal
}

func (k *KlineData) Update(trade Trade) {
	price := trade.Price
	if k.TimeStamp == 0 {
		k.High = price
		k.Low = price
		k.Open = price
	} else {
		if k.High.LessThan(price) {
			k.High = price
		}
		if price.LessThan(k.Low) {
			k.Low = price
		}
	}
	k.Close = price
	buyVol, sellVol := 0.0, 0.0
	if trade.IsSell {
		sellVol = trade.Vol.InexactFloat64()
	} else {
		buyVol = trade.Vol.InexactFloat64()
	}
	k.TimeStamp = trade.TimeStamp
	k.Turnover = trade.Price.Mul(trade.Vol)
	k.BuyVol = k.BuyVol.Add(decimal.NewFromFloat(buyVol))
	k.SellVol = k.BuyVol.Add(decimal.NewFromFloat(sellVol))
	k.Price = trade.Price
}

func (k *KlineData) Record() []string {
	ts := fmt.Sprintf("%d", k.TimeStamp)
	return []string{
		ts, k.Open.String(), k.High.String(), k.Low.String(), k.Close.String(), k.BuyVol.String(), k.SellVol.String(), k.Turnover.String(),
	}
}

type KlineDatas struct {
	Datas  [1440]KlineData
	Type   Category
	Date   time.Time
	Symbol string
}

func (k *KlineDatas) FixHighAndLow() {
	dotmap := make(map[int]int)
	for _, data := range k.Datas {
		dotmap[data.Price.NumDigits()]++
	}
	maxDig := 0
	freqDig := 0
	freqCount := 0
	for dig, count := range dotmap {
		if maxDig < dig {
			maxDig = dig
		}
		if count > freqCount {
			freqCount = count
			freqDig = dig
		}
	}
	if maxDig != freqDig {
		log.Printf("digit err %d != %d", maxDig, freqDig)
	}
	minTick := decimal.NewFromFloat(math.Pow(10, float64(freqDig)))
	for _, data := range k.Datas {
		data.High.Sub(minTick)
		data.Low.Add(minTick)
	}
}

func (k *KlineDatas) getPrevDayKlineData() KlineData {
	prevDay := k.Date.Add(-24 * time.Hour)

	filename := fmt.Sprintf("%s-aggTrades-%s.zip", k.Symbol, prevDay.Format(DateLayout))
	if k.Type == SPOT {
		filename = path.Join(*aggTradeDir, "spot", prevDay.Format(DateLayout), filename)
	} else if k.Type == Future {
		filename = path.Join(*aggTradeDir, "um", prevDay.Format(DateLayout), filename)
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
		price := trade.Price
		klineData = KlineData{
			Symbol:    k.Symbol,
			TimeStamp: trade.TimeStamp,
			Open:      price,
			Close:     price,
			High:      price,
			Low:       price,
		}
	} else {
		log.Fatal("unexpect zip file count")
	}
	return klineData
}

func (k *KlineDatas) getPrevData(minutes int) (KlineData, int) {
	for i := minutes - 1; i >= 0; i-- {
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
	if minutes > 0 {
		k.fillPrevData(minutes)
	}
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

		kpath = path.Join(*dir, "spotkdata/")
	} else {

		kpath = path.Join(*dir, "kdata/")
	}

	name := path.Join(kpath, "open/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_Open.csv")
	WriteToFile(name, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.Open.InexactFloat64(), 'f', -1, 64)
	})

	name1 := path.Join(kpath, "high/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_High.csv")
	WriteToFile(name1, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.High.InexactFloat64(), 'f', -1, 64)
	})

	name2 := path.Join(kpath, "low/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_Low.csv")
	WriteToFile(name2, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.Low.InexactFloat64(), 'f', -1, 64)
	})

	name3 := path.Join(kpath, "close/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_Close.csv")
	WriteToFile(name3, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.Close.InexactFloat64(), 'f', -1, 64)
	})

	name4 := path.Join(kpath, "buyvol/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_BuyVol.csv")
	WriteToFile(name4, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.BuyVol.InexactFloat64(), 'f', -1, 64)
	})

	name5 := path.Join(kpath, "sellvol/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_SellVol.csv")
	WriteToFile(name5, *k, func(kline KlineData) string {
		return strconv.FormatFloat(kline.SellVol.InexactFloat64(), 'f', -1, 64)
	})

	name6 := path.Join(kpath, "turnover/"+k.Date.Format(DateLayout)+"_"+config.Type.string()+"_TurnOver.csv")
	WriteToFile(name6, *k, func(kline KlineData) string {
		turnOver, _ := kline.Turnover.Float64()
		return strconv.FormatFloat(turnOver, 'f', 2, 64)
	})

	writeKlineToFile(kpath, *k)
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
	for i := range data.SymbolData[symbols[0]].Datas {
		row := make([]string, len(symbols)+1)
		row[0] = strconv.FormatFloat(float64(data.Date.UnixMilli()+int64(i*60000)), 'f', 0, 64)
		for j, symbol := range symbols {
			row[j+1] = valueFunc(data.SymbolData[symbol].Datas[i])
		}
		writer.Write(row)
	}
}

func writeKlineToFile(baseDir string, data Kline) {
	dir := path.Dir(baseDir)
	os.MkdirAll(dir, os.ModeDir)

	for symbol, kline := range data.SymbolData {
		filename := path.Join(baseDir, kline.Date.Format(DateLayout), symbol+"_"+config.Type.string()+".csv")
		os.MkdirAll(path.Dir(filename), os.ModeDir)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModeAppend)
		if err != nil {
			log.Println(err)
			continue
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		defer writer.Flush()
		writer.Write([]string{"timestamp", "open", "high", "low", "close", "buyvol", "sellvol", "turnover"})
		for _, k := range kline.Datas {
			writer.Write(k.Record())
		}
	}
}

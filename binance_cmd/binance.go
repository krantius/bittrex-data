package main

import (
	"fmt"

	"encoding/json"
	"io/ioutil"

	"context"

	"time"

	"github.com/adshao/go-binance"
	"github.com/olivere/elastic"
	"strconv"
	"github.com/krantius/bittrex-data/bittrex"
)

type PrettyKline struct {
	OpenTime                 int64     `json:"openTime"`
	Open                     float64    `json:"open"`
	High                     float64    `json:"high"`
	Low                      float64    `json:"low"`
	Close                    float64    `json:"close"`
	Volume                   float64    `json:"volume"`
	CloseTime                int64     `json:"closeTime"`
	QuoteAssetVolume         float64    `json:"quoteAssetVolume"`
	TradeNum                 int64     `json:"tradeNum"`
	TakerBuyBaseAssetVolume  float64    `json:"takerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume float64    `json:"takerBuyQuoteAssetVolume"`
	Market                   string    `json:"market"`
	Time                     time.Time `json:"time"`
}

func GetCandles(market string) []*PrettyKline {
	client := binance.NewClient("", "")

	klines, err := client.NewKlinesService().Symbol(market).Interval("1m").Do(context.Background())

	if err != nil {
		fmt.Printf("error getting klines for market %s: %v\n", market, err)
		return []*PrettyKline{}
	}

	all := []*PrettyKline{}
	for _, candle := range klines {
		open, _ := strconv.ParseFloat(candle.Open, 64)
		high, _ := strconv.ParseFloat(candle.High, 64)
		low, _ := strconv.ParseFloat(candle.Low, 64)
		close, _ := strconv.ParseFloat(candle.Close, 64)
		volume, _ := strconv.ParseFloat(candle.Volume, 64)
		quoteAssetVolume, _ := strconv.ParseFloat(candle.QuoteAssetVolume, 64)
		takerBuyBaseAssetVolume, _ := strconv.ParseFloat(candle.TakerBuyBaseAssetVolume, 64)
		takerBuyQuoteAssetVolume, _ := strconv.ParseFloat(candle.TakerBuyQuoteAssetVolume, 64)

		tmp := &PrettyKline{
			OpenTime:                 candle.OpenTime,
			Open:                     open * bittrex.Satoshi,
			High:                     high * bittrex.Satoshi,
			Low:                      low * bittrex.Satoshi ,
			Close:                    close * bittrex.Satoshi,
			Volume:                   volume,
			CloseTime:                candle.CloseTime,
			QuoteAssetVolume:         quoteAssetVolume,
			TradeNum:                 candle.TradeNum,
			TakerBuyBaseAssetVolume:  takerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: takerBuyQuoteAssetVolume,
			Market: market,
			Time:   time.Unix(candle.CloseTime/1000, 0),
		}

		all = append(all, tmp)
	}

	return all
}

func ListenBinance() {
	wsAggTradeHandler := func(event *binance.WsAggTradeEvent) {
		fmt.Printf("%+v\n", event)
	}
	errHandler := func(err error) {
		fmt.Println(err)
	}
	doneC, _, err := binance.WsAggTradeServe("LTCBTC", wsAggTradeHandler, errHandler)
	if err != nil {
		fmt.Println(err)
		return
	}
	<-doneC
}

func LoadMarkets(name string) []string {
	b, err := ioutil.ReadFile("markets.json")
	if err != nil {
		fmt.Printf("failed to read markets file: %v", err)
		return nil
	}

	markets := []string{}
	err = json.Unmarshal(b, &markets)
	if err != nil {
		fmt.Printf("failed to load markets: %v", err)
		return nil
	}

	return markets
}

func main() {
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://192.168.1.125:9200"))
	if err != nil {
		panic(err)
	}

	markets := LoadMarkets("./markets.json")

	UpdateMarketData(markets, client)
}

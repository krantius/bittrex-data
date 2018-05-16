package main

import (
	"fmt"

	"encoding/json"
	"io/ioutil"

	"context"

	"time"

	"github.com/adshao/go-binance"
	"github.com/olivere/elastic"
)

type PrettyKline struct {
	OpenTime                 int64     `json:"openTime"`
	Open                     string    `json:"open"`
	High                     string    `json:"high"`
	Low                      string    `json:"low"`
	Close                    string    `json:"close"`
	Volume                   string    `json:"volume"`
	CloseTime                int64     `json:"closeTime"`
	QuoteAssetVolume         string    `json:"quoteAssetVolume"`
	TradeNum                 int64     `json:"tradeNum"`
	TakerBuyBaseAssetVolume  string    `json:"takerBuyBaseAssetVolume"`
	TakerBuyQuoteAssetVolume string    `json:"takerBuyQuoteAssetVolume"`
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
		tmp := &PrettyKline{
			OpenTime:                 candle.OpenTime,
			Open:                     candle.Open,
			High:                     candle.High,
			Low:                      candle.Low,
			Close:                    candle.Close,
			Volume:                   candle.Volume,
			CloseTime:                candle.CloseTime,
			QuoteAssetVolume:         candle.QuoteAssetVolume,
			TradeNum:                 candle.TradeNum,
			TakerBuyBaseAssetVolume:  candle.TakerBuyBaseAssetVolume,
			TakerBuyQuoteAssetVolume: candle.TakerBuyQuoteAssetVolume,
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

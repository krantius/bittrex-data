package bittrex

import (
	"net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"strings"
	"time"
)

type Base struct {
	Message string `json:"message"`
	Success bool `json:"success"`
}

type MarketResponse struct {
	Base
	Result []Summary `json:"result"`
}

type CandleResponse struct {
	Base
	Result []Candle `json:"result"`
}

type Summary struct {
	Market Market `json:"Market"`
}

type Market struct {
	MarketName string `json:"MarketName"`
}

type Candle struct {
	Open float32 `json:"O"`
	High float32 `json:"H"`
	Low float32 `json:"L"`
	Close float32 `json:"C"`
	Volume float32 `json:"V"`
	BaseVolume float32 `json:"BV"`
	Timestamp candleTime `json:"T"`
}

type PrettyCandle struct {
	Open float32 `json:"open"`
	High float32 `json:"high"`
	Low float32 `json:"low"`
	Close float32 `json:"close"`
	Volume float32 `json:"volume"`
	BaseVolume float32 `json:"baseVolume"`
	Timestamp candleTime `json:"time"`
	Market string `json:"market"`
	Interval int `json:"interval"`
}

type candleTime time.Time

func ConvertCandle(candle Candle, market string) PrettyCandle {
	return PrettyCandle{
		Open: candle.Open * 100000000,
		High: candle.High * 100000000,
		Low: candle.Low * 100000000,
		Close: candle.Close * 100000000,
		Volume: candle.Volume,
		BaseVolume: candle.BaseVolume,
		Timestamp: candle.Timestamp,
		Market: market,
		Interval: 5,
	}
}

func GetCandles(market string) []Candle {
	resp, err := http.Get(fmt.Sprintf("https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=%s&tickInterval=fiveMin", market))
	if err != nil {
		fmt.Printf("failed to get ticks for market %v\n", market)
	}

	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	var cr CandleResponse
	err = json.Unmarshal(body, &cr)
	if err != nil {
		panic(err)
	}

	return cr.Result
}

func GetMarkets() {
	url := "https://bittrex.com/api/v2.0/pub/Markets/GetMarketSummaries"

	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()

	var mr MarketResponse
	err = json.Unmarshal(body, &mr)
	if err != nil {
		panic(err)
	}

	marketArr := []string{}
	for _, summary := range mr.Result {
		name := summary.Market.MarketName

		if strings.HasPrefix(name, "BTC-") {
			marketArr = append(marketArr, name)
		}
	}
}

func (t *candleTime) UnmarshalJSON(b []byte) error {
	if len(b) < 2 {
		return fmt.Errorf("could not parse time %s", string(b))
	}
	// trim enclosing ""
	result, err := time.Parse("2006-01-02T15:04:05", string(b[1:len(b)-1]))
	if err != nil {
		return fmt.Errorf("could not parse time: %v", err)
	}
	*t = candleTime(result)
	return nil
}

func (t candleTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02T15:04:05"))
	return []byte(stamp), nil
}

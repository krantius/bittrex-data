package main

import (
	"encoding/json"
	"fmt"

	"io/ioutil"
)

func main() {
	markets := LoadMarkets("./markets.json")

	/*
		client, err := elastic.NewSimpleClient(elastic.SetURL("http://192.168.1.125:9200"))

		if err != nil {
			panic(err)
		}


		candleStats := []*stats.CandleStats{}
		for i, market := range markets {
			c, err := stats.GetStats(market, client)
			if err != nil {
				log.Printf("error getting stats: %v", err)
				continue
			}
			candleStats = append(candleStats, c)
			fmt.Printf("Did %d out of %d\n", i+1, len(markets))*/
	/*candles := GetCandles(market)

		for _, c := range candles {
			pc := ConvertCandle(c, market)
			StoreInElastic(pc, client)
		}

		fmt.Printf("Did %d out of %d\n", i+1, len(markets))
	}*/

	//stats.OutputStats(candleStats)
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

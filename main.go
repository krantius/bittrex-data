package main

import (
	"encoding/json"
	"fmt"

	"io/ioutil"
	"github.com/olivere/elastic"
	"github.com/krantius/bittrex-data/stats"
)

func main() {
	markets := LoadMarkets("./markets.json")


	client, err := elastic.NewSimpleClient()

	if err != nil {
		panic(err)
	}

	// Update our elasticsearch data with the most up to date candles
	UpdateMarketData(markets, client)

	// Output the market stats to ./stats.txt
	stats.OutputStats(markets, client)
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

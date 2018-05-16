package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/olivere/elastic"
)

func UpdateMarketData(markets []string, client *elastic.Client) {
	for i, market := range markets {
		/*	q := elastic.NewMatchPhraseQuery("market", market)
			searchResult, err := client.Search().Index("binance").Type("kline").Query(q).From(0).Size(1).Sort("time", false).Do(context.Background())
			if err != nil {
				log.Printf("Got error getting latest candle from es for market %v: %v", market, err)
				continue
			}

			if len(searchResult.Hits.Hits) == 0 {
				log.Print("Expecting 1 es hit, but found none for %v", market)
				continue
			}

			c := &PrettyKline{}
			err = json.Unmarshal(*searchResult.Hits.Hits[0].Source, c)
			if err != nil {
				log.Printf("Got error unmarshaling %v from es: %v", market, err)
				continue
			}

			oldT := time.Time(c.Time)*/

		candles := GetCandles(market)
		for _, c := range candles {
			/*	newT := time.Time(c.Time)

				if !newT.After(oldT) {
					continue
				}
			*/
			StoreInElastic(c, client)
		}

		fmt.Printf("Did %d out of %d\n", i+1, len(markets))
	}
}

func StoreInElastic(candle *PrettyKline, client *elastic.Client) {
	ctx := context.Background()

	json, _ := json.Marshal(candle)

	_, err := client.Index().Index("binance").Type("kline").BodyString(string(json)).Do(ctx)
	if err != nil {
		fmt.Printf("error inserting: %v\n", err)
		return
	}
}

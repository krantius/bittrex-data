package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/krantius/bittrex-data/bittrex"
	"github.com/olivere/elastic"
)

func UpdateMarketData(markets []string, client *elastic.Client) {
	for i, market := range markets {

		q := elastic.NewMatchPhraseQuery("market", market)
		searchResult, err := client.Search().Index("bittrex").Type("candle").Query(q).From(0).Size(1).Sort("time", false).Do(context.Background())
		if err != nil {
			log.Printf("Got error getting latest candle from es for market %v: %v", market, err)
			continue
		}

		if len(searchResult.Hits.Hits) == 0 {
			log.Print("Expecting 1 es hit, but found none for %v", market)
			continue
		}

		c := &bittrex.PrettyCandle{}
		err = json.Unmarshal(*searchResult.Hits.Hits[0].Source, c)
		if err != nil {
			log.Printf("Got error unmarshaling %v from es: %v", market, err)
			continue
		}

		oldT := time.Time(c.Timestamp)

		candles := bittrex.GetCandles(market)
		for _, c := range candles {
			pc := bittrex.ConvertCandle(c, market)

			newT := time.Time(pc.Timestamp)

			if !newT.After(oldT) {
				continue
			}

			StoreInElastic(pc, client)
		}

		fmt.Printf("Did %d out of %d\n", i+1, len(markets))
	}
}

func StoreInElastic(candle bittrex.PrettyCandle, client *elastic.Client) {
	ctx := context.Background()

	json, _ := json.Marshal(candle)

	_, err := client.Index().Index("bittrex").Type("candle").BodyString(string(json)).Do(ctx)
	if err != nil {
		fmt.Printf("error inserting: %v\n", err)
		return
	}
}

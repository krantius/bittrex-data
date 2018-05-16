package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"log"
	"os"

	"github.com/krantius/bittrex-data/bittrex"
	"github.com/olivere/elastic"
)

type CandleStats struct {
	Interval int
	Market   string
	Avg      float32
	Med      float32
	High     float32
	Low      float32
	Sum      float32
}

// Outputs all market stats found in elastic search to stats.txt
func OutputStats(markets []string, client *elastic.Client) {
	cs := []*CandleStats{}
	for i, m := range markets {
		s, err := getStats(m, client)
		if err != nil {
			log.Printf("error getting stats: %v", err)
			continue
		}

		fmt.Printf("Did %d out of %d\n", i+1, len(markets))
		cs = append(cs, s)
	}

	// Guarantees a fresh file
	f, err := os.Create("./stats.txt")
	if err != nil {
		fmt.Printf("failed to create stats file: %v\n", err)
		return
	}

	d, err := json.Marshal(cs)
	if err != nil {
		log.Printf("failed to marshal candles: %v", err)
	}

	f.Write(d)
}

// Gets candles from elastic search and calculates the CandleStats
func getStats(market string, client *elastic.Client) (*CandleStats, error) {
	q := elastic.NewMatchPhraseQuery("market", market)
	searchResult, err := client.Search().Index("bittrex").Type("candle").Query(q).From(0).Size(10000).Do(context.Background())
	if err != nil {
		return nil, err
	}

	candles := []*bittrex.PrettyCandle{}
	for _, hit := range searchResult.Hits.Hits {
		c := &bittrex.PrettyCandle{}
		err := json.Unmarshal(*hit.Source, c)
		if err != nil {
			panic(err)
		}

		candles = append(candles, c)
	}

	fmt.Printf("%v\n", len(candles))

	return calcCandleStats(candles), nil
}

// Calculates the candle stats from the slice of candles
func calcCandleStats(candles []*bittrex.PrettyCandle) *CandleStats {
	fmt.Printf("%v\n", len(candles))
	cs := &CandleStats{}

	if len(candles) == 0 {
		return nil
	}
	cs.Interval = candles[0].Interval
	cs.Market = candles[0].Market

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].BaseVolume < candles[j].BaseVolume
	})

	var sum float32 = 0.0
	for _, c := range candles {
		sum += c.BaseVolume
	}

	cs.Avg = sum / float32(len(candles))
	cs.Med = candles[len(candles)/2].BaseVolume
	cs.High = candles[len(candles)-1].BaseVolume
	cs.Low = candles[0].BaseVolume
	cs.Sum = sum

	fmt.Printf("%+v", cs)
	return cs
}

package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strings"
	"fmt"

	"time"
	"github.com/olivere/elastic"
	"context"
	"sort"
	"log"
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

func main() {
	markets := []string {
		"BTC-2GIVE",
		"BTC-ABY",
		"BTC-ADA",
		"BTC-ADT",
		"BTC-ADX",
		"BTC-AEON",
		"BTC-AMP",
		"BTC-ANT",
		"BTC-ARDR",
		"BTC-ARK",
		"BTC-AUR",
		"BTC-BAT",
		"BTC-BAY",
		"BTC-BCC",
		"BTC-BCPT",
		"BTC-BCY",
		"BTC-BITB",
		"BTC-BLITZ",
		"BTC-BLK",
		"BTC-BLOCK",
		"BTC-BNT",
		"BTC-BRK",
		"BTC-BRX",
		"BTC-BSD",
		"BTC-BTG",
		"BTC-BURST",
		"BTC-BYC",
		"BTC-CANN",
		"BTC-CFI",
		"BTC-CLAM",
		"BTC-CLOAK",
		"BTC-COVAL",
		"BTC-CRB",
		"BTC-CRW",
		"BTC-CURE",
		"BTC-CVC",
		"BTC-DASH",
		"BTC-DCR",
		"BTC-DCT",
		"BTC-DGB",
		"BTC-DMD",
		"BTC-DMT",
		"BTC-DNT",
		"BTC-DOGE",
		"BTC-DOPE",
		"BTC-DTB",
		"BTC-DYN",
		"BTC-EBST",
		"BTC-EDG",
		"BTC-EFL",
		"BTC-EGC",
		"BTC-EMC",
		"BTC-EMC2",
		"BTC-ENG",
		"BTC-ENRG",
		"BTC-ERC",
		"BTC-ETC",
		"BTC-ETH",
		"BTC-EXCL",
		"BTC-EXP",
		"BTC-FCT",
		"BTC-FLDC",
		"BTC-FLO",
		"BTC-FTC",
		"BTC-GAM",
		"BTC-GAME",
		"BTC-GBG",
		"BTC-GBYTE",
		"BTC-GEO",
		"BTC-GLD",
		"BTC-GNO",
		"BTC-GNT",
		"BTC-GOLOS",
		"BTC-GRC",
		"BTC-GRS",
		"BTC-GUP",
		"BTC-HMQ",
		"BTC-IGNIS",
		"BTC-INCNT",
		"BTC-IOC",
		"BTC-ION",
		"BTC-IOP",
		"BTC-KMD",
		"BTC-KORE",
		"BTC-LBC",
		"BTC-LGD",
		"BTC-LMC",
		"BTC-LRC",
		"BTC-LSK",
		"BTC-LTC",
		"BTC-LUN",
		"BTC-MANA",
		"BTC-MCO",
		"BTC-MEME",
		"BTC-MER",
		"BTC-MLN",
		"BTC-MONA",
		"BTC-MUE",
		"BTC-MUSIC",
		"BTC-NAV",
		"BTC-NBT",
		"BTC-NEO",
		"BTC-NEOS",
		"BTC-NLG",
		"BTC-NMR",
		"BTC-NXC",
		"BTC-NXS",
		"BTC-NXT",
		"BTC-OK",
		"BTC-OMG",
		"BTC-OMNI",
		"BTC-PART",
		"BTC-PAY",
		"BTC-PINK",
		"BTC-PIVX",
		"BTC-POLY",
		"BTC-POT",
		"BTC-POWR",
		"BTC-PPC",
		"BTC-PTC",
		"BTC-PTOY",
		"BTC-QRL",
		"BTC-QTUM",
		"BTC-QWARK",
		"BTC-RADS",
		"BTC-RBY",
		"BTC-RCN",
		"BTC-RDD",
		"BTC-REP",
		"BTC-RLC",
		"BTC-RVR",
		"BTC-SALT",
		"BTC-SBD",
		"BTC-SC",
		"BTC-SEQ",
		"BTC-SHIFT",
		"BTC-SIB",
		"BTC-SLR",
		"BTC-SLS",
		"BTC-SNRG",
		"BTC-SNT",
		"BTC-SPHR",
		"BTC-SPR",
		"BTC-SRN",
		"BTC-STEEM",
		"BTC-STORJ",
		"BTC-STRAT",
		"BTC-SWIFT",
		"BTC-SWT",
		"BTC-SYNX",
		"BTC-SYS",
		"BTC-THC",
		"BTC-TIX",
		"BTC-TKS",
		"BTC-TRST",
		"BTC-TRUST",
		"BTC-TRX",
		"BTC-TUSD",
		"BTC-TX",
		"BTC-UBQ",
		"BTC-UKG",
		"BTC-UNB",
		"BTC-UP",
		"BTC-VEE",
		"BTC-VIA",
		"BTC-VIB",
		"BTC-VRC",
		"BTC-VRM",
		"BTC-VTC",
		"BTC-VTR",
		"BTC-WAVES",
		"BTC-WAX",
		"BTC-WINGS",
		"BTC-XCP",
		"BTC-XDN",
		"BTC-XEL",
		"BTC-XEM",
		"BTC-XLM",
		"BTC-XMG",
		"BTC-XMR",
		"BTC-XMY",
		"BTC-XRP",
		"BTC-XST",
		"BTC-XVC",
		"BTC-XVG",
		"BTC-XWC",
		"BTC-XZC",
		"BTC-ZCL",
		"BTC-ZEC",
		"BTC-ZEN",
		"BTC-ZRX",
	}

	client, err := elastic.NewSimpleClient()
	if err != nil {
		panic(err)
	}


	UpdateMarketData(markets, client)
	/*
	for i, market := range markets {
		candles := GetCandles(market)

		for _, c := range candles {
			pc := ConvertCandle(c, market)
			StoreInElastic(pc, client)
		}

		fmt.Printf("Did %d out of %d\n", i+1, len(markets))
	}*/
}

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

		c := &PrettyCandle{}
		err = json.Unmarshal(*searchResult.Hits.Hits[0].Source, c)
		if err != nil {
			log.Printf("Got error unmarshaling %v from es: %v", market, err)
			continue
		}

		oldT := time.Time(c.Timestamp)

		candles := GetCandles(market)
		for _, c := range candles {
			pc := ConvertCandle(c, market)

			newT := time.Time(pc.Timestamp)

			if !newT.After(oldT) {
				continue
			}

			StoreInElastic(pc, client)
		}

		fmt.Printf("Did %d out of %d\n", i+1, len(markets))
	}
}

func StoreInElastic(candle PrettyCandle, client *elastic.Client) {
	ctx := context.Background()

	json, _ := json.Marshal(candle)

	_, err := client.Index().Index("bittrex").Type("candle").BodyString(string(json)).Do(ctx)
	if err != nil {
		fmt.Printf("error inserting: %v\n", err)
		return
	}
}

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

func GetStats(market string, client *elastic.Client) error {
	q := elastic.NewMatchPhraseQuery("market", market)
	searchResult, err := client.Search().Index("bittrex").Type("candle").Query(q).From(0).Size(10000).Do(context.Background())
	if err != nil {
		return err
	}

	candles := []*PrettyCandle{}
	for _, hit := range searchResult.Hits.Hits {
		c := &PrettyCandle{}
		err := json.Unmarshal(*hit.Source, c)
		if err != nil {
			panic(err)
		}

		candles = append(candles, c)
	}

	fmt.Printf("%v\n", len(candles))

	calcCandleStats(candles)

	return nil
}

type CandleStats struct {
	Interval int
	Market string
	Avg float32
	Med float32
	High float32
	Low float32
	Sum float32
}

func calcCandleStats(candles []*PrettyCandle) {
	fmt.Printf("%v\n", len(candles))
	cs := &CandleStats{}
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


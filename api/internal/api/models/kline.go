package models

// Kline mirrors the wire shape Hyperliquid uses on its candleSnapshot REST and
// candle WS channels. All numeric price/volume fields are kept as strings to
// preserve precision through the JSON round-trip; epoch fields are unix
// milliseconds.
type Kline struct {
	TimeOpen  int64  `json:"t"`
	TimeClose int64  `json:"T"`
	Symbol    string `json:"s"`
	Interval  string `json:"i"`
	Open      string `json:"o"`
	High      string `json:"h"`
	Low       string `json:"l"`
	Close     string `json:"c"`
	Volume    string `json:"v"`
	Trades    int    `json:"n"`
}

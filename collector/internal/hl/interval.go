package hl

// HL's wire format uses uppercase "1M" for monthly candles to disambiguate
// from the lowercase "1m" minute interval. Internally we standardise on the
// safer "1mo" (Postgres folds unquoted identifiers to lowercase, so a
// "klines_1M" table would collide with "klines_1m"). All translation between
// the two namespaces happens at the HL adapter boundary — REST request
// payloads and WS subscribe params go through toWireInterval; candle frames
// coming back through the SDK callback get fromWireInterval applied.

// toWireInterval converts our internal interval to what HL accepts on the wire.
func toWireInterval(interval string) string {
	if interval == "1mo" {
		return "1M"
	}
	return interval
}

// fromWireInterval converts HL's wire interval back to our internal name.
func fromWireInterval(interval string) string {
	if interval == "1M" {
		return "1mo"
	}
	return interval
}

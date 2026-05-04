package models

// UserFill mirrors HL's userFills wire shape (one row per maker/taker match).
// Numeric fields are strings to preserve precision; time is unix ms.
type UserFill struct {
	Tid           int64  `json:"tid"`
	Time          int64  `json:"time"`
	Coin          string `json:"coin"`
	Side          string `json:"side"`           // "B" or "A"
	Px            string `json:"px"`
	Sz            string `json:"sz"`
	Fee           string `json:"fee"`
	ClosedPnl     string `json:"closedPnl"`
	StartPosition string `json:"startPosition,omitempty"`
	Dir           string `json:"dir,omitempty"`
	Oid           int64  `json:"oid"`
	Hash          string `json:"hash,omitempty"`
	Crossed       bool   `json:"crossed"`
}

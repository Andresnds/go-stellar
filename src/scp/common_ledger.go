package scp

const (
	OK                    = "OK"
	ErrNoAccount          = "ErrNoAccount"
	ErrInsuficientBalance = "ErrInsuficientBalance"
	ErrInvalidTranscation = "ErrInvalidTranscation"
	ErrSignatureForged    = "ErrSignatureForged"
)

type Err string

type TransactionArgs struct {
	From      string
	To        string
	Value     float32
	XID       int64
	RSign     string
	SSign     string
}

type TransactionReply struct {
	Err Err
}

type GetBalanceArgs struct {
	Account string
	XID     int64
}

type GetBalanceReply struct {
	Err     Err
	Balance float32
}

type InsertCoinsArgs struct {
	Account string
	Value   float32
	XID     int64
}

type InsertCoinsReply struct {
	Err Err
}

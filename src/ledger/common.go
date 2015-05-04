package ledger

const (
	OK                    = "OK"
	ErrNoAccount          = "ErrNoAccount"
	ErrInsuficientBalance = "ErrInsuficientBalance"
	ErrInvalidTranscation = "ErrInvalidTranscation"
)

type Err string

type TransactiondArgs struct {
	From      string
	To        string
	Value     float
	XID       int64
	Signature string
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
	Balance float
}

type InsertCoinsArgs struct {
	Account string
	Value   float
	XID     int64
}

type InsertCoinsReply struct {
	Err Err
}

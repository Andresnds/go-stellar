package ledger

const (
	OK                    = "OK"
	ErrNoAccount          = "ErrNoAccount"
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

type TransactiondReply struct {
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

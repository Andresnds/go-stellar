package ledger

import "net/rpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "time"

type Clerk struct {
	servers []string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current balance for an account.
// returns false if the account does not exist.
//
func (ck *Clerk) GetBalance(account string) (float32, bool) {
	XID := nrand()
	args := GetBalanceArgs{account, XID}
	var reply GetBalanceReply

	to := 10 * time.Millisecond
	ok := false
	for !ok {
		for i := 0; !ok && i < len(ck.servers); i++ {
			ok = call(ck.servers[i], "Ledger.GetBalance", args, &reply)
			if reply.Err != OK {
				ok = false
			}
			if reply.Err == ErrNoAccount {
				return 0, false
			}
		}
		if !ok {
			time.Sleep(to)
			to *= 2
		}
	}
	return reply.Balance, true
}

// sends a Transction to be executed on the system
// return false if the transaction is invalid
func (ck *Clerk) Transaction(from string, to string, value float32, signature string) bool {
	XID := nrand()
	args := TransactionArgs{from, to, value, XID, signature}
	var reply TransactionReply

	t := 10 * time.Millisecond
	ok := false
	for !ok {
		for i := 0; !ok && i < len(ck.servers); i++ {
			ok = call(ck.servers[i], "Ledger.Transaction", args, &reply)
			if reply.Err != OK {
				ok = false
			}
			if reply.Err == ErrInvalidTranscation {
				return false
			}
		}

        if reply.Err == ErrInsuficientBalance {
            return false
        }

		if !ok {
			time.Sleep(t)
			t *= 2
		}
	}
	return true
}

func (ck *Clerk) InsertCoins(account string, value float32) {
	XID := nrand()
	args := InsertCoinsArgs{account, value, XID}
	var reply InsertCoinsReply

	to := 10 * time.Millisecond
	ok := false
	for !ok {
		for i := 0; !ok && i < len(ck.servers); i++ {
			ok = call(ck.servers[i], "Ledger.InsertCoins", args, &reply)
			if reply.Err != OK {
				ok = false
			}
		}
		if !ok {
			time.Sleep(to)
			to *= 2
		}
	}
}

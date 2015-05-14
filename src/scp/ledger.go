package scp

import "net"
import "fmt"
import "net/rpc"
import "log"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "math/big"
import "crypto/dsa"
import "time"

import "bytes"

type Op struct {
	From      string
	To        string
	Value     float32
	RSign     string
	SSign     string
	XID       int64
	Op        string
}

type Ledger struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	scp        *ScpNode

	balances map[string]float32
	seq      int
	done     int
}

const LedgerDebug = 0

func (lg *Ledger) DPrintf(format string, a ...interface{}) (n int, err error) {
	if LedgerDebug > 0 {
		tag := fmt.Sprintf("Ledger %d: ", lg.me)
		log.Printf(tag+format+"\n", a...)
	}
	return
}

func (lg *Ledger) catchUp() {
	for decided, _ := lg.scp.Status(lg.seq); decided || lg.seq < lg.done; decided, _ = lg.scp.Status(lg.seq) {
		lg.seq++
	}
}

func (lg *Ledger) replicate(op Op) bool {
	lg.scp.Start(lg.seq, op)
	to := 10 * time.Millisecond
	for {
		decided, value := lg.scp.Status(lg.seq)
		if decided {
			if value == op {
				return true
			} else {
				return false
			}
		} else {
			time.Sleep(to)
			to *= 2
		}
	}
}

func (lg *Ledger) replicateUntilDecide(op Op) error {
	lg.catchUp()
	lg.scp.Start(lg.seq, op)
	to := 10 * time.Millisecond
	for {
		decided, value := lg.scp.Status(lg.seq)
		if decided {
			if value == op {
				return nil
			}
			lg.catchUp()
			lg.scp.Start(lg.seq, op)
			to = 10 * time.Millisecond
		} else {
			time.Sleep(to)
			to *= 2
		}
	}
}

func (lg *Ledger) applyOperations(XID int64) error {
	to := 10 * time.Millisecond
	var opXID int64
	for XID != opXID {
		if decided, op := lg.scp.Status(lg.done); decided {
			opXID = op.XID
			switch op.Op {
			case "Transaction":
				lg.balances[op.From] -= op.Value
				lg.balances[op.To] += op.Value
			case "InsertCoins":
				lg.balances[op.To] += op.Value
			}
			lg.done++
			to = 10 * time.Millisecond
		} else {
			time.Sleep(to)
			to *= 2
		}
	}
	return nil
}

func (lg *Ledger) GetBalance(args *GetBalanceArgs, reply *GetBalanceReply) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	op := Op{
		From: args.Account,
		To:   args.Account,
		XID:  args.XID,
		Op:   "GetBalance",
	}

	lg.replicateUntilDecide(op)
	lg.applyOperations(args.XID)

	reply.Balance = lg.balances[args.Account]
	reply.Err = OK

	return nil
}

func (lg *Ledger) Transaction(args *TransactionArgs, reply *TransactionReply) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	
	if !validSignature(args) {
		reply.Err = ErrSignatureForged
		return nil
	}

    if args.Value <= 0 {
        reply.Err = ErrInvalidTranscation
        return nil
    }

	op := Op{
		From:      args.From,
		To:        args.To,
		Value:     args.Value,
		XID:       args.XID,
		RSign:     args.RSign,
		SSign:     args.SSign,
		Op:        "Transaction",
	}

	for ok := false; !ok; ok = lg.replicate(op) {
		dumbID := nrand()
		lg.replicateUntilDecide(Op{Op: "Dumb", XID: dumbID})
		lg.applyOperations(dumbID)

		if lg.balances[args.From] < args.Value {
			reply.Err = ErrInsuficientBalance
			return nil
		}

		lg.seq++
	}

	reply.Err = OK
	return nil
}

func (lg *Ledger) InsertCoins(args *InsertCoinsArgs, reply *InsertCoinsReply) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	// TODO: verify signature?

    if args.Value < 0 {
        reply.Err = ErrInvalidTranscation
        return nil
    }

	op := Op{
		To:    args.Account,
		Value: args.Value,
		XID:   args.XID,
		Op:    "InsertCoins",
	}

	lg.replicateUntilDecide(op)
	lg.applyOperations(args.XID)

	reply.Err = OK
	return nil
}

func validSignature(args *TransactionArgs) bool {
	pCache := new(bytes.Buffer)
	decCache := gob.NewDecoder(pCache)

	pCache.WriteString(args.From)
	var pk1 dsa.PublicKey
	decCache.Decode(&pk1)

	pCache.Reset()
	pCache.WriteString(args.To)
	var pk2 dsa.PublicKey
	decCache.Decode(&pk2)

	transaction := getTransaction(&pk1, &pk2, args.Value)

	pCache.Reset()
	pCache.WriteString(args.RSign)
	var rSign big.Int
	decCache.Decode(&rSign)

	pCache.Reset()
	pCache.WriteString(args.SSign)
	var sSign big.Int
	decCache.Decode(&sSign)

	return dsa.Verify(&pk1, transaction, &rSign, &sSign)
}

// tell the server to shut itself down.
func (lg *Ledger) kill() {
	lg.DPrintf("Kill(%d): die\n", lg.me)
	atomic.StoreInt32(&lg.dead, 1)
	lg.l.Close()
	lg.scp.Kill()
}

func (lg *Ledger) isdead() bool {
	return atomic.LoadInt32(&lg.dead) != 0
}

func (lg *Ledger) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&lg.unreliable, 1)
	} else {
		atomic.StoreInt32(&lg.unreliable, 0)
	}
}

func (lg *Ledger) isunreliable() bool {
	return atomic.LoadInt32(&lg.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via SCP to
// form the fault-tolerant Account/value service.
// me is the index of the current server in servers[].
//
func Make(servers []string, me int, peerSlices map[int][][]int) *Ledger {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	lg := new(Ledger)
	lg.me = me

	lg.balances = make(map[string]float32)

	rpcs := rpc.NewServer()
	rpcs.Register(lg)

	lg.scp = StartServer(servers, me, peerSlices)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	lg.l = l

	go func() {
		for lg.isdead() == false {
			conn, err := lg.l.Accept()
			if err == nil && lg.isdead() == false {
				if lg.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if lg.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && lg.isdead() == false {
				fmt.Printf("Ledger(%v) accept: %v\n", me, err.Error())
				lg.kill()
			}
		}
	}()

	return lg
}

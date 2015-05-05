package ledger

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	From      string
	To        string
	Value     float32
	Signature string
	XID       int64
	Op        string
}

type Ledger struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	balances    map[string]float32
	seen        map[int64]bool
	replyBuffer map[int64]float32
	done        int
	seq         int
}

func (lg *Ledger) catchUp() {
	for dec, _ := lg.px.Status(lg.seq); dec == paxos.Decided || lg.seq < lg.done; dec, _ = lg.px.Status(lg.seq) {
		lg.seq++
	}
}

func (lg *Ledger) replicate(op Op) bool {
	lg.px.Start(lg.seq, op)
	to := 10 * time.Millisecond
	for {
		status, value := lg.px.Status(lg.seq)
		switch status {
		case paxos.Decided:
			if value == op {
				return true
			} else {
				return false
			}
		default:
			time.Sleep(to)
			to *= 2
		}
	}
}

func (lg *Ledger) replicateUntilDecide(op Op) error {
	lg.catchUp()
	lg.px.Start(lg.seq, op)
	to := 10 * time.Millisecond
	for {
		status, value := lg.px.Status(lg.seq)
		switch status {
		case paxos.Decided:
			if value == op {
				return nil
			}
			lg.catchUp()
			lg.px.Start(lg.seq, op)
			to = 10 * time.Millisecond
		default:
			time.Sleep(to)
			to *= 2
		}
	}
}

func (lg *Ledger) applyOperations(XID int64) error {
	to := 10 * time.Millisecond
	for !lg.seen[XID] {
		switch status, value := lg.px.Status(lg.done); status {
		case paxos.Decided:
			op := value.(Op)
			if !lg.seen[op.XID] {
				switch op.Op {
				case "Transaction":
					lg.balances[op.From] -= op.Value
					lg.balances[op.To] += op.Value
				case "InsertCoins":
					lg.balances[op.To] += op.Value
				}
				lg.seen[op.XID] = true
			}
			lg.px.Done(lg.done)
			lg.done++
			to = 10 * time.Millisecond
		default:
			time.Sleep(to)
			to *= 2
		}
	}
	return nil
}

func (lg *Ledger) GetBalance(args *GetBalanceArgs, reply *GetBalanceReply) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if value, ok := lg.replyBuffer[args.XID]; ok {
		reply.Balance = value
		reply.Err = OK
		return nil
	}

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

	lg.replyBuffer[args.XID] = reply.Balance
	return nil
}

func (lg *Ledger) Transaction(args *TransactionArgs, reply *TransactionReply) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if _, ok := lg.replyBuffer[args.XID]; ok {
		reply.Err = OK
		return nil
	}

	// TODO: verify signature
    if args.Value <= 0 {
        reply.Err = ErrInvalidTranscation
        return nil
    }

	op := Op{
		From:      args.From,
		To:        args.To,
		Value:     args.Value,
		XID:       args.XID,
		Signature: args.Signature,
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
	lg.replyBuffer[args.XID] = 0.0
	return nil
}

func (lg *Ledger) InsertCoins(args *InsertCoinsArgs, reply *InsertCoinsReply) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if _, ok := lg.replyBuffer[args.XID]; ok {
		reply.Err = OK
		return nil
	}

	// TODO: some way to verify it
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
	lg.replyBuffer[args.XID] = 0.0
	return nil
}

// tell the server to shut itself down.
func (lg *Ledger) kill() {
	DPrintf("Kill(%d): die\n", lg.me)
	atomic.StoreInt32(&lg.dead, 1)
	lg.l.Close()
	lg.px.Kill()
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
// servers that will cooperate via Paxos to
// form the fault-tolerant Account/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *Ledger {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	lg := new(Ledger)
	lg.me = me

	lg.balances = make(map[string]float32)
	lg.seen = make(map[int64]bool)
	lg.replyBuffer = make(map[int64]float32)

	rpcs := rpc.NewServer()
	rpcs.Register(lg)

	lg.px = paxos.Make(servers, me, rpcs)

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

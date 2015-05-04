package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	min            int
	done           map[int]int
	doneMutex      sync.Mutex
	Instances      map[int]*InstanceInfo
	instancesMutex sync.Mutex
	seqMutexesMu   sync.RWMutex
	seqMutexes     map[int]*sync.Mutex
}

type InstanceInfo struct {
	Proposer *Proposer
	Acceptor *Acceptor
}

type Proposer struct {
	Num        int
	HighestNum int
	Value      interface{}
	Prepared   map[string]bool
	Accepted   map[string]bool
	Decided    bool
	Active     bool

	mutex sync.Mutex
}

type Acceptor struct {
	HighestNum    int
	AcceptedNum   int
	AcceptedValue interface{}

	mutex    sync.Mutex
	// lastHear time.Time
}

func (px *Paxos) UpdateDone(done int, me int) {
	px.doneMutex.Lock()
	defer px.doneMutex.Unlock()

	if px.done[me] < done {
		px.done[me] = done
	}

	min := px.done[px.me]
	for _, d := range px.done {
		if d < min {
			min = d
		}
	}

	if min > px.min {
		px.instancesMutex.Lock()
		px.min = min
		for seq := range px.Instances {
			if seq <= min {
				px.seqMutexesMu.RLock()
				mu := px.seqMutexes[seq]
				px.seqMutexesMu.RUnlock()
				mu.Lock()
				delete(px.Instances, seq)
				mu.Unlock()
				// log.Println("Deleted",seq)
			}
		}
		px.instancesMutex.Unlock()
	}
}

// ------------------------ Prepare ----------------------------------

func (px *Paxos) SendPrepareRequests(seq int) {
	px.InitializeInstance(seq)

	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if seq <= px.min {
		mu.Unlock()
		return
	}
	proposer := px.Instances[seq].Proposer
	mu.Unlock()

	proposer.Prepared = make(map[string]bool)

	// log.Println("Seq",seq,"->",px.me,"sending Prepare on",proposer.Num,"value",proposer.Value)

	for _, peer := range px.peers {
		if proposer.Decided {
			break
		}
		ok := true

		args := PrepareArgs{proposer.Num, seq, px.me, px.done[px.me]}
		reply := PrepareReply{}

		if px.peers[px.me] == peer {
			px.Prepare(args, &reply) // Not use RPC on internal calls
		} else {
			ok = false
			for i := 0; i<10 && !ok; i++ {
				ok = call(peer, "Paxos.Prepare", args, &reply)
			}
		}

		if ok {
			px.UpdateDone(reply.Done, reply.Me)
			if reply.Ok {
				proposer.Prepared[peer] = true
				if proposer.HighestNum < reply.AcceptedNum && reply.Value != nil {
					proposer.HighestNum = reply.AcceptedNum
					proposer.Value = reply.Value
				}
			} else {
				break
			}

		}

		if len(proposer.Prepared) > len(px.peers)/2 {
			// log.Println("Seq",seq,"->",px.me,"got majority Prepare on",proposer.Num,"value",proposer.Value)
			break
		}
	}

}

func (px *Paxos) Prepare(args PrepareArgs, reply *PrepareReply) error {
	// log.Println("\t\t",px.me,"received Prepare from",args.Me,"on",args.Num)
	px.InitializeInstance(args.Seq)
	px.UpdateDone(args.Done, args.Me)
	reply.Done = px.done[px.me]
	reply.Me = px.me

	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[args.Seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if args.Seq <= px.min {
		mu.Unlock()
		return nil
	}
	acceptor := px.Instances[args.Seq].Acceptor
	mu.Unlock()

	acceptor.mutex.Lock()
	if acceptor.HighestNum > args.Num {
		reply.Ok = false
		// log.Println("\t\t",px.me,"DENIED Prepare from",args.Me,"on",args.Num)
	} else {
		acceptor.HighestNum = args.Num

		reply.Ok = true
		reply.Value = acceptor.AcceptedValue
		reply.AcceptedNum = acceptor.AcceptedNum
		// log.Println("\t\t",px.me,"Prepare from",args.Me,"on",args.Num)
	}
	acceptor.mutex.Unlock()

	return nil
}

// ------------------------ Accept ----------------------------------

func (px *Paxos) SendAcceptRequests(seq int) {
	px.InitializeInstance(seq)
	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if seq <= px.min {
		mu.Unlock()
		return
	}
	proposer := px.Instances[seq].Proposer
	mu.Unlock()

	proposer.Accepted = make(map[string]bool)

	// log.Println("Seq",seq,"->",px.me,"sending Accept on",proposer.Num,"value",proposer.Value)

	for peer := range proposer.Prepared {
		if proposer.Decided {
			break
		}

		ok := true

		args := AcceptArgs{proposer.Num, proposer.Value, seq, px.me, px.done[px.me]}
		reply := AcceptReply{}

		if px.peers[px.me] == peer {
			px.Accept(args, &reply)
		} else {
			ok = false
			for i := 0; i<10 && !ok; i++ {
				ok = call(peer, "Paxos.Accept", args, &reply)
			}
		}
		if ok {
			px.UpdateDone(reply.Done, reply.Me)
			if reply.Ok {
				proposer.Accepted[peer] = true
			} else {
				break
			}
		}

		if len(proposer.Accepted) > len(px.peers)/2 {
			// log.Println("Seq",seq,"->",px.me,"got majority Accept on",proposer.Num,"value",proposer.Value)
			break
		}
	}

}

func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error {
	// log.Println("\t\t",px.me,"received Accept from",args.Me,"on",args.Num,"value",args.Value)
	px.InitializeInstance(args.Seq)
	px.UpdateDone(args.Done, args.Me)
	reply.Done = px.done[px.me]
	reply.Me = px.me

	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[args.Seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if args.Seq <= px.min {
		mu.Unlock()
		return nil
	}
	acceptor := px.Instances[args.Seq].Acceptor
	mu.Unlock()

	acceptor.mutex.Lock()
	if acceptor.HighestNum > args.Num {
		reply.Ok = false
		// log.Println("\t\t",px.me,"DENIED Accept from",args.Me,"on",args.Num,"value",args.Value)
	} else {
		acceptor.HighestNum = args.Num
		acceptor.AcceptedNum = args.Num
		acceptor.AcceptedValue = args.Value

		reply.Ok = true
		// log.Println("\t\t",px.me,"Accept from",args.Me,"on",args.Num,"value",args.Value)
	}
	acceptor.mutex.Unlock()

	return nil
}

// ------------------------ Decide ----------------------------------

func (px *Paxos) SendDecideRequests(seq int) {
	px.InitializeInstance(seq)
	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if seq <= px.min {
		mu.Unlock()
		return
	}
	proposer := px.Instances[seq].Proposer
	mu.Unlock()

	undecided := make(map[string]bool)

	for _, peer := range px.peers {
		undecided[peer] = true
	}

	// log.Println("Seq",seq,"->",px.me,"sending Decide on",proposer.Num,"value",proposer.Value)

	for len(undecided) > 0 && !px.isdead() {
		for peer := range undecided {
			ok := true

			args := DecideArgs{proposer.Num, proposer.Value, seq, px.me, px.done[px.me]}
			reply := DecideReply{}
			if px.peers[px.me] == peer {
				px.Decide(args, &reply)
			} else {
				ok = false
				for i := 0; i<10 && !ok; i++ {
					ok = call(peer, "Paxos.Decide", args, &reply)
				}
			}

			if ok {
				delete(undecided, peer)
				px.UpdateDone(reply.Done, reply.Me)
			}
		}
		time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
	}

	// log.Println("Seq",seq,"->",px.me,"done with Decide on",proposer.Num,"value",proposer.Value)
}

func (px *Paxos) Decide(args DecideArgs, reply *DecideReply) error {
	// log.Println("\t\t",px.me,"received Decide from",args.Me,"value",args.Value)
	px.InitializeInstance(args.Seq)
	px.UpdateDone(args.Done, args.Me)
	reply.Done = px.done[px.me]
	reply.Me = px.me

	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[args.Seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if args.Seq <= px.min {
		mu.Unlock()
		return nil
	}
	acceptor := px.Instances[args.Seq].Acceptor
	mu.Unlock()

	acceptor.mutex.Lock()
	acceptor.HighestNum = args.Num
	acceptor.AcceptedNum = args.Num
	acceptor.AcceptedValue = args.Value
	acceptor.mutex.Unlock()

	mu.Lock()
	if args.Seq <= px.min {
		mu.Unlock()
		return nil
	}
	proposer := px.Instances[args.Seq].Proposer
	mu.Unlock()

	proposer.mutex.Lock()
	// if !proposer.Active {
	// log.Println("\t\t",px.me,"Decide from",args.Me,"value",args.Value)
	proposer.Active = true
	proposer.Value = args.Value
	proposer.Num = args.Num
	proposer.Decided = true
	// }
	proposer.mutex.Unlock()

	return nil
}

// ------------------------ Routine ----------------------------------

func (px *Paxos) ProposerRoutine(seq int) {
	px.InitializeInstance(seq)
	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if seq <= px.min {
		mu.Unlock()
		return
	}
	proposer := px.Instances[seq].Proposer
	mu.Unlock()

	for !proposer.Decided && !px.isdead() && seq > px.min {

		time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))

		// Update Proposer Num -> Unique and higher than any so far
		proposer.Num = px.me + (1+proposer.Num/len(px.peers))*len(px.peers)

		px.SendPrepareRequests(seq)

		// Majority prepared
		if len(proposer.Prepared) > len(px.peers)/2 {
			px.SendAcceptRequests(seq)

			// Majority accepted
			if len(proposer.Accepted) > len(px.peers)/2 {
				proposer.Decided = true
			}
		}
	}

	px.SendDecideRequests(seq)
}

func (px *Paxos) InitializeInstance(seq int) {
	px.instancesMutex.Lock()
	defer px.instancesMutex.Unlock()

	px.seqMutexesMu.Lock()
	if _, ok := px.seqMutexes[seq]; !ok {
		px.seqMutexes[seq] = &sync.Mutex{}
	}
	px.seqMutexesMu.Unlock()

	if seq <= px.min {
		return
	}

	if _, ok := px.Instances[seq]; !ok {
		// log.Println("initialize",seq)
 		px.Instances[seq] = &InstanceInfo{&Proposer{}, &Acceptor{}}
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.InitializeInstance(seq)
	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if seq <= px.min {
		mu.Unlock()
		return
	}
	proposer := px.Instances[seq].Proposer
	mu.Unlock()

	proposer.mutex.Lock()
	if !proposer.Active {
		proposer.Active = true
		proposer.Value = v
		go px.ProposerRoutine(seq)
	}
	proposer.mutex.Unlock()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.InitializeInstance(seq)
	px.UpdateDone(seq, px.me)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := 0
	for seq := range px.Instances {
		if seq > max {
			max = seq
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.doneMutex.Lock()
	defer px.doneMutex.Unlock()

	return px.min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.InitializeInstance(seq)
	px.seqMutexesMu.RLock()
	mu := px.seqMutexes[seq]
	px.seqMutexesMu.RUnlock()
	mu.Lock()
	if seq <= px.min {
		mu.Unlock()
		return Forgotten, nil
	}
	proposer := px.Instances[seq].Proposer
	mu.Unlock()

	if proposer.Decided {
		return Decided, proposer.Value
	}

	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.Instances = make(map[int]*InstanceInfo)
	px.done = make(map[int]int)
	for peer := range px.peers {
		px.done[peer] = -1
	}
	px.min = -1
	px.seqMutexes = make(map[int]*sync.Mutex)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

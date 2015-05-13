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
import "time"

/* -------- Debugging -------- */

const Debug = 1

func (scp *ScpNode) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		tag := fmt.Sprintf("ScpNode %d: ", scp.me)
		log.Printf(tag+format+"\n", a...)
	}
	return
}

/* -------- Node -------- */

type ScpNode struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing

	me           int // This node's id
	quorumSlices []QuorumSlice

	peers      map[int]string // All the peers discovered so far
	peerSlices map[int][]QuorumSlice

	slots   map[int]*Slot // SCP runs in a Slot
	quorums []Quorum
}

/* -------- Phase -------- */

type Phase int

const (
	PREPARE Phase = iota + 1
	FINISH
	EXTERNALIZE
)

/* -------- Slices -------- */

type QuorumSlice []int // nodes ids
type Quorum []int      // nodes ids

/* -------- Slot -------- */

// Holds every nodes' state, including itself
type Slot struct {
	states map[int]*State
}

/* -------- State -------- */

type State struct {
	B    Ballot
	P    Ballot
	POld Ballot
	C    Ballot
	Phi  Phase
}

func (state State) getCopy() State {
	copyState := state
	return copyState
}

func (state State) isInitialized() bool {
	return state.B.N != 0
}

func (state State) String() string {
	return fmt.Sprintf("B.n=%d, P.n=%d, POld.n=%d, C.n=%d, Phi=%d", state.B.N, state.P.N, state.POld.N, state.C.N, state.Phi)
}

/* -------- Ballot -------- */

type Ballot struct {
	N int
	V Op
}

// Returns two bools: b1 > b2 and b1 ~ b2
func compareBallots(b1, b2 Ballot) (greater, compatible bool) {
	if b1.N == 0 && b2.N == 0 {
		greater = false
		compatible = true
		return
	}

	if b1.N == 0 {
		greater = false
		compatible = true
		return
	}

	if b2.N == 0 {
		greater = true
		compatible = true
		return
	}

	greater = (b1.N > b2.N)
	compatible = (b1.V == b2.V)
	return
}

func areBallotsEqual(b1, b2 Ballot) bool {
	sameN := (b1.N == b2.N)
	_, compatible := compareBallots(b1, b2)
	return (sameN && compatible)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

/* -------- API -------- */

func (scp *ScpNode) Start(seq int, v Op) {
	// Locked
	scp.mu.Lock()
	defer scp.mu.Unlock()

	scp.DPrintf("Start(seq=%d, v=%+v)", seq, v)

	// Create initial state
	// Allocate this slot if needed
	slot := scp.getSlot(seq)
	state := slot.states[scp.me]
	state.B = Ballot{1, v}

	// Propose every timeout. Maybe 100ms is too much.
	// TODO: Check if a randomized interval is necessary
	// TODO: Tick until externalized, not dead
	go func() {
		for !scp.isdead() {
			scp.propose(seq, v)
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (scp *ScpNode) propose(seq int, v Op) {
	// Locked
	scp.mu.Lock()

	scp.DPrintf("Timeout! Propose(seq=%d, v=%+v)", seq, v)

	// Timeout! Update b if we are still looking for consensus
	myState := scp.slots[seq].states[scp.me]
	if myState.Phi != EXTERNALIZE {
		scp.DPrintf("Proposing new ballot")
		myState.B.N += 1
		myStateCopy := myState.getCopy()
		scp.mu.Unlock()
		scp.broadcastMessage(seq, myStateCopy)
		return
	}
	scp.mu.Unlock()
}

// Returns: true if a consensus was reached on seq, with it's value
// false otherwise
func (scp *ScpNode) Status(seq int) (bool, Op) {
	slot := scp.getSlot(seq)
	myState := slot.states[scp.me]

	scp.DPrintf("Status(seq=%d): state=%v", seq, myState)

	if myState.Phi == EXTERNALIZE {
		return true, myState.B.V
	}
	return false, Op{}
}

// ------- RPC HANDLERS ------- //

// Reads and process the message from other scp nodes
func (scp *ScpNode) ProcessMessage(args *ProcessMessageArgs, reply *ProcessMessageReply) error {
	// Locked
	scp.mu.Lock()

	scp.DPrintf("RPC handling ProcessMessage: args=%+v", args)

	// Allocate this slot if needed
	slot := scp.getSlot(args.Seq)

	// Update peer's state
	// TODO: Check if this is the most recent message
	slot.states[args.ID] = &args.State


	// Run the SCP steps to update this slot's state
	updated := scp.tryToUpdateState(args.Seq)

	// If i'm finished and he is not, just send him my state
	if slot.states[scp.me].Phi == EXTERNALIZE && args.State.Phi != EXTERNALIZE {
		stateCopy := slot.states[scp.me].getCopy()
		scp.mu.Unlock()
		scp.replyMessage(args.Seq, stateCopy, args.ID)
		return nil
	}

	// Broadcast message to peers if needed
	if updated {
		stateCopy := slot.states[scp.me].getCopy()
		scp.mu.Unlock()
		scp.broadcastMessage(args.Seq, stateCopy)
		return nil
	}
	scp.mu.Unlock()
	return nil
}

func (scp *ScpNode) ExchangeQSlices(args *ExchangeQSlicesArgs, reply *ExchangeQSlicesReply) {
	// TODO: :P
	scp.DPrintf("RPC handling ExchangeQSlices: args=%+v", args)
}

// Broadcast message to other scp nodes
func (scp *ScpNode) broadcastMessage(seq int, state State) {
	scp.DPrintf("broadcastMessage(seq=%d), state=%+v", seq, state)

	// Assumes scp.peers is immutable! If not, create a copy before unlocking.
	for peerId, peer := range scp.peers {
		// Don't send it to yourself
		if peerId == scp.me {
			continue
		}

		args := &ProcessMessageArgs{seq, scp.me, state}
		var reply ProcessMessageReply

		scp.DPrintf("Sending message to peer=%s", peer)
		ok := call(peer, "ScpNode.ProcessMessage", args, &reply)
		if !ok {
			scp.DPrintf("Sending message to peer=%s failed!", peer)
		}
	}
}

// Used when we are externalized, so we just reply whoever sent us a message
func (scp *ScpNode) replyMessage(seq int, state State, peerId int) {
	scp.DPrintf("replyMessage(seq=%d) to %d, state=%+v", seq, peerId, state)

	peer := scp.peers[peerId]

	args := &ProcessMessageArgs{seq, scp.me, state}
	var reply ProcessMessageReply

	scp.DPrintf("Sending message to peer=%s", peer)
	ok := call(peer, "ScpNode.ProcessMessage", args, &reply)
	if !ok {
		scp.DPrintf("Sending message to peer=%s failed!", peer)
	}
}


// -------- State update -------- //

// Tries to update it's state, given the information in M
// Returns: bool stating if updated or not
func (scp *ScpNode) tryToUpdateState(seq int) bool {
	// State before
	stateBefore := scp.slots[seq].states[scp.me].getCopy()

	scp.DPrintf("tryToUpdateState(seq=%d), state before=%+v", seq, stateBefore)

	// Apply the SCP steps
	scp.step0(seq)
	scp.step1(seq)
	scp.step2(seq)
	scp.step3(seq)
	scp.step4(seq)

	// Check if there was an update
	stateAfter := scp.slots[seq].states[scp.me].getCopy()
	scp.DPrintf("tryToUpdateState(seq=%d), state after=%+v", seq, stateAfter)
	if stateBefore != stateAfter {
		return true
	}
	return false
}

// Try to update b
// TODO: Not change when phi = EXTERNALIZE?
func (scp *ScpNode) step0(seq int) {
	slot := scp.slots[seq]
	myState := slot.states[scp.me]


	scp.DPrintf("step0(seq=%d), state=%+v", seq, myState)

	if myState.Phi == EXTERNALIZE {
		scp.DPrintf("Doesn't meet step 0 requirements")
		return
	}

	for _, state := range slot.states {
		greaterThanB, _ := compareBallots(state.B, myState.B)
		_, compatibleWithC := compareBallots(state.B, myState.C)

		if greaterThanB && compatibleWithC {
			myState.B = state.B
		}
	}
}

// Try to update p
func (scp *ScpNode) step1(seq int) {
	myState := scp.slots[seq].states[scp.me]

	scp.DPrintf("step1(seq=%d), state=%+v", seq, myState)

	// Should not be recently uninitilized
	if !myState.isInitialized() {
		scp.DPrintf("Doesn't meet step 1 requirements: uninitialized")
		return
	}

	// Conditions: phi = PREPARE and b > p >= c
	if myState.Phi != PREPARE {
		scp.DPrintf("Doesn't meet step 1 requirements")
		return
	}

	if greater, _ := compareBallots(myState.B, myState.P); !greater {
		scp.DPrintf("Doesn't meet step 1 requirements")
		return
	}

	// No need to check p >= c, just for asserting

	// To accept prepare b, we need a quorum voting/accepting prepare b
	// Hence we need a quorum with b = myState.B or p = myState.B or pOld = myState.B
	isValidB := func(peerState State) bool {
		return areBallotsEqual(peerState.B, myState.B)
	}

	isValidP := func(peerState State) bool {
		return areBallotsEqual(peerState.P, myState.B)
	}

	isValidPOld := func(peerState State) bool {
		return areBallotsEqual(peerState.POld, myState.B)
	}

	if scp.hasQuorum(seq, isValidB) || scp.hasQuorum(seq, isValidP) || scp.hasQuorum(seq, isValidPOld) {
		scp.DPrintf("step1(seq=%d): updating p! Found quorum voting/accepting prepare b", seq)
		scp.updatePs(seq, myState.B)
		return
	}

	// Or we need a v-blocking accepting prepare b
	// Hence we need a v-blocking with p = myState.B or pOld = myState.B
	if scp.hasVBlocking(seq, isValidP) || scp.hasVBlocking(seq, isValidPOld) {
		scp.DPrintf("step1(seq=%d): updating p! Found vblocking accepting prepare b", seq)
		scp.updatePs(seq, myState.B)
		return
	}
}

func (scp *ScpNode) updatePs(seq int, newP Ballot) {
	myState := scp.slots[seq].states[scp.me]

	// Only update pOld if necessary
	if _, compatible := compareBallots(newP, myState.P); compatible {
		myState.POld = myState.P
	}
	myState.P = newP

	// Ensure the invariant p ~ c
	if _, compatible := compareBallots(myState.P, myState.C); !compatible {
		// This means that we went from voting to commit c to accept (abort c)
		// by setting c = 0, which is valid in FBA voting
		myState.C = Ballot{}
	}
}

// Try to update c
func (scp *ScpNode) step2(seq int) {
	myState := scp.slots[seq].states[scp.me]

	scp.DPrintf("step2(seq=%d), state=%+v", seq, myState)

	// Should not be uninitilized
	if !myState.isInitialized() {
		scp.DPrintf("Doesn't meet step 2 requirements: uninitialized")
		return
	}

	// Conditions: phi = PREPARE, b = p, b != c
	if myState.Phi != PREPARE {
		scp.DPrintf("Doesn't meet step 2 requirements")
		return
	}

	if !areBallotsEqual(myState.B, myState.P) {
		scp.DPrintf("Doesn't meet step 2 requirements")
		return
	}

	if areBallotsEqual(myState.B, myState.C) {
		scp.DPrintf("Doesn't meet step 2 requirements")
		return
	}

	// To vote on commit c, we need to confirm prepare b,
	// so we need a quorum accepting prepare b
	// Hence we need a quorum with p = myState.B(= myState.P) or pOld = myState.B(= myState.P)
	isValidP := func(peerState State) bool {
		return areBallotsEqual(peerState.P, myState.B)
	}

	isValidPOld := func(peerState State) bool {
		return areBallotsEqual(peerState.POld, myState.B)
	}

	if scp.hasQuorum(seq, isValidP) || scp.hasQuorum(seq, isValidPOld) {
		scp.DPrintf("step2(seq=%d): updating c! Found quorum of accepting prepare b(= p)", seq)
		myState.C = myState.B
		return
	}
}

// Try to update phi: PREPARE -> FINISH
func (scp *ScpNode) step3(seq int) {
	myState := scp.slots[seq].states[scp.me]

	scp.DPrintf("step3(seq=%d), state=%+v", seq, myState)

	// Should not be uninitilized
	if !myState.isInitialized() {
		scp.DPrintf("Doesn't meet step 3 requirements: uninitialized")
		return
	}

	// Conditions: phi=PREPARE and b = p = c
	if myState.Phi != PREPARE {
		scp.DPrintf("Doesn't meet step 3 requirements")
		return
	}

	if !(areBallotsEqual(myState.B, myState.P) && areBallotsEqual(myState.P, myState.C)) {
		scp.DPrintf("Doesn't meet step 3 requirements")
		return
	}

	// To accept commit c, we need a quorum voting/accepting commit c
	// Hence we need a quorum with our c
	isValid := func(peerState State) bool {
		return areBallotsEqual(peerState.C, myState.C)
	}

	if scp.hasQuorum(seq, isValid) {
		myState.Phi = FINISH
		scp.DPrintf("step3(seq=%d): setting phi=FINISH! Found quorum voting/accepting commit c(= b = p)", seq)
		return
	}

	// Or we need a v-blocking accepting commit c
	// Hence we need a v-blocking with our c and phi = FINISH
	isValid = func(peerState State) bool {
		return areBallotsEqual(peerState.C, myState.C) && (peerState.Phi == FINISH)
	}

	if scp.hasVBlocking(seq, isValid) {
		myState.Phi = FINISH
		scp.DPrintf("step3(seq=%d): setting phi=FINISH! Found vblocking accepting commit c(= b = p)", seq)
		return
	}
}

// Try to update phi: FINISH -> EXTERNALIZE
func (scp *ScpNode) step4(seq int) {
	myState := scp.slots[seq].states[scp.me]

	scp.DPrintf("step4(seq=%d), state=%+v", seq, myState)

	// Should not be uninitilized
	if !myState.isInitialized() {
		scp.DPrintf("Doesn't meet step 4 requirements: uninitialized")
		return
	}

	// Conditions : phi = FINISH
	if myState.Phi != FINISH {
		scp.DPrintf("Doesn't meet step 4 requirements")
		return
	}

	// To confirm commit c, we need a quorum accepting commit c
	// Hence we need a quorum with our c and phi = FINISH
	isValid := func(peerState State) bool {
		return areBallotsEqual(peerState.C, myState.C) && (peerState.Phi == FINISH)
	}

	if scp.hasQuorum(seq, isValid) {
		myState.Phi = EXTERNALIZE
		scp.DPrintf("step3(seq=%d): setting phi=EXTERNALIZE! Found quorum accepting commit c(= b = p)", seq)
		return
	}
}

// ---- Helper Functions ---- //

// Returns true if there is a quorum that satisfies the validator isValid
func (scp *ScpNode) hasQuorum(seq int, isValid func(State) bool) bool {
	// Printings for debugging
	// scp.DPrintf("quorums=%v", scp.quorums)
	// s := ""
	// for peerId, _ := range scp.peers {
	// 	s = s + fmt.Sprintf("[peer %d state=%v] ", peerId, scp.slots[seq].states[peerId])
	// }
	// scp.DPrintf("peers state=%s", s)

	for _, quorum := range scp.quorums {
		if scp.isQuorumValid(seq, quorum, isValid) {
			return true
		}
	}
	return false
}

// Checks if all quorum members satisfy the validator isValid
func (scp *ScpNode) isQuorumValid(seq int, quorum Quorum, isValid func(State) bool) bool {
	for _, nodeId := range quorum {
		state := *(scp.slots[seq].states[nodeId])
		if !state.isInitialized() || !isValid(state) {
			return false
		}
	}
	return true
}

// Returns true if there all of it's slices are blocked
func (scp *ScpNode) hasVBlocking(seq int, isValid func(State) bool) bool {
	for _, slice := range scp.peerSlices[scp.me] {
		if !scp.isSliceBlocked(seq, slice, isValid) {
			return false
		}
	}
	return true
}

// A slice is blocked if any of it's elements satisfies the validator
func (scp *ScpNode) isSliceBlocked(seq int, slice QuorumSlice, isValid func(State) bool) bool {
	for _, nodeId := range slice {
		state := *(scp.slots[seq].states[nodeId])
		if state.isInitialized() && isValid(state) {
			return true
		}
	}
	return false
}

/* -------- Make -------- */

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant Account/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int, peerSlices map[int][][]int) *ScpNode {
	gob.Register(Op{})
	gob.Register(State{})

	scp := new(ScpNode)
	scp.init(me, servers, peerSlices)

	rpcs := rpc.NewServer()
	rpcs.Register(scp)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	scp.l = l

	go func() {
		for scp.isdead() == false {
			conn, err := scp.l.Accept()
			if err == nil && scp.isdead() == false {
				if scp.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if scp.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && scp.isdead() == false {
				scp.DPrintf("accept: %v", err.Error())
				scp.Kill()
			}
		}
	}()

	return scp
}

func (scp *ScpNode) init(id int, servers []string, peerSlices map[int][][]int) {
	scp.me = id

	scp.peers = make(map[int]string)
	for nodeId, server := range servers {
		scp.peers[nodeId] = server
	}

	scp.peerSlices = make(map[int][]QuorumSlice)
	for nodeId, nodeSlices := range peerSlices {
		scp.peerSlices[nodeId] = make([]QuorumSlice, len(nodeSlices))
		for i, nodeSlice := range nodeSlices {
			scp.peerSlices[nodeId][i] = QuorumSlice(nodeSlice)
		}
	}

	scp.slots = make(map[int]*Slot)

	scp.quorums = findQuorums(scp.peerSlices)
	// Store only the quorums which contain myself
	for i, quorum := range scp.quorums {

		contained := false
		for _, v := range quorum {
			if v == scp.me {
				contained = true
				break
			}
		}

		if !contained {
			scp.quorums = append(scp.quorums[:i], scp.quorums[i+1:]...) // removing i'th element
		}
	}
}

func (scp *ScpNode) getSlot(seq int) (slot *Slot) {
	if _, ok := scp.slots[seq]; !ok {
		scp.DPrintf("getSlot(seq=%d): allocating new slot %d", seq, seq)
		newSlot := Slot{}
		scp.slots[seq] = &newSlot

		// Allocate a State for each peer
		newSlot.states = make(map[int]*State)
		for peerId, _ := range scp.peers {
			newSlot.states[peerId] = &State{}
		}
		newSlot.states[scp.me] = &State{Phi: PREPARE}
	}
	return scp.slots[seq]
}

/* ----- Util ----- */

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
			fmt.Printf("scp Dial() failed: %v\n", err1)
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

// tell the server to shut itself down.
func (scp *ScpNode) Kill() {
	// DPrintf("Kill(%d): die\n", scp.me)
	atomic.StoreInt32(&scp.dead, 1)
	scp.l.Close()
}

func (scp *ScpNode) isdead() bool {
	return atomic.LoadInt32(&scp.dead) != 0
}

func (scp *ScpNode) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&scp.unreliable, 1)
	} else {
		atomic.StoreInt32(&scp.unreliable, 0)
	}
}

func (scp *ScpNode) isunreliable() bool {
	return atomic.LoadInt32(&scp.unreliable) != 0
}

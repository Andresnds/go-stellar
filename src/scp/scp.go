package scp

/* Imports */

type Phase int
const (
	PREPARE Phase = iota + 1
	FINISH
	EXTERNALIZE
)

type QuorumSlice []int // nodes ids
type Quorum []int // nodes ids

type ScpNode struct {
	mu    sync.Mutex
	l     net.Listener

	me int // This node's id
	quorumSlices []QuorumSlice

	peers map[int]string // All the peers discovered so far
	peersSlices map[int][]QuorumSlice

	slots map[int]*Slot // SCP runs in a Slot
	quorums []Quorum
}

/* -------- State -------- */

// Holds every nodes' state, including itself
type Slot struct {
	states map[int]*State 
}

/* -------- State -------- */

type State struct {
	b    Ballot
	p    Ballot
	pOld Ballot
	c    Ballot
	phi  Phase
}

/* -------- Ballot -------- */

type Ballot struct {
	n int
	v ledger.Op
}

// Returns two bools: b1 > b2 and b1 ~ b2
func compareBallots(b1, b2 Ballot) (greater, compatible bool) {
	if b1.n == 0 && b2.n == 0 {
		greater = false
		compatible = true
		return
	}

	if b1.n == 0 {
		greater = false
		compatible = true
		return
	}

	if b2.n == 0 {
		greater = true
		compatible = true
		return
	}

	greater = (b1.n > b2.n)
	compatible = (ledger.Compare(b1.v, b2.v) == 0)
	return
}

func areBallotsEqual(b1, b2 Ballot) bool {
	sameN := (b2.n == b2.n)
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

/* -------- Initialization -------- */

func (scp *ScpNode) Init(id int, peers map[int]string, peerSlices map[int][]QuorumSlice) {
	scp.id = id

	scp.peers := make(map[int]string)
	for nodeID, add := range peers {
		scp.peers[nodeID] = add
	}

	scp.peerSlices := make(map[int][]QuorumSlice)
	for nodeID, nodeSlices := range peerSlices {
		scp.peerSlices[nodeID] = make([]QuorumSlice, len(nodeSlices)) 
		for i, nodeSlice := range nodeSlices {
			scp.peerSlices[nodeID][i] = nodeSlice
		}
	}

	scp.slots = make(map[int]*Slot)

	scp.quorums = findQuorums(scp.peerSlices)
	// Store only the quorums which contain myself
	for i, quorum := range scp.quorums {

		contained := false
		for _, v := quorum {
			if v == scp.me {
				contained = true
				break
			}
		}

		if !contained {
			scp.quorums = append(scp.quorums[:i], scp.quorums[i+1:]...) // removing i'th element
		}
	}

	// TODO: Initialize l and listen to network
}

func (scp *ScpNode) Start(seq int, v Ledger.Op) {
	// Locked
	scp.mu.Lock()
	defer scp.mu.Unlock()

	// Propose every timeout
	// Maybe 100ms is too much
	go func() {
		scp.propose(v)
		time.Sleep(100 * time.Millisecond)
	}()
}

func (scp *ScpNode) propose(seq int, v Ledger.Op) {
	// Locked
	scp.mu.Lock()
	defer scp.mu.Unlock()

	// Timeout! Update b if we are still looking for consensus
	myState := scp.slot.states[scp.me]
	if myState.b != 0 myState.phi != EXTERNALIZE {
		myState.b.n += 1
		scp.broadcastMessage(seq)
	}
}

func (scp *ScpNode) Status(seq int) (..) {
	// TODO!
}

// ------- RPC HANDLERS ------- //

// Reads and process the message from other scp nodes
func (scp *ScpNode) ProcessMessage(args *ProcessMessageArgs, reply *ProcessMessageReply) error {
	// Locked
	scp.mu.Lock()
	defer scp.mu.Unlock()

	// Allocate this slot if needed
	scp.checkSlotAllocation(args.Seq)
	slot := scp.slots[args.Seq]

	// Update peer's state
	// TODO: Check if this is the most recent message
	slot.states[args.ID] = &args.State

	// Run the SCP steps to update this slot's state
	updated := scp.tryToUpdateState(args.Seq)

	// Broadcast message to peers if needed
	if updated {
		scp.broadcastMessage(args.Seq)
	}
}

func (scp *ScpNode) ExchangeQSlices(args *ExchangeQSlicesArgs, reply *ExchangeQSlicesReply) {
	// TODO: :P
}

func (scp *ScpNode) checkSlotAllocation(seq int) {
	if _, ok := scp.slots[seq]; !ok {
		newSlot := Slot{}
		newSlot.states = make(map[int]*State)
		scp.slots[seq] = &newSlot
	}
}

// Broadcast message to other scp nodes
func (scp *ScpNode) broadcastMessage(seq int) {
	state := scp.slots[args.Seq].state[scp.me]

	for id, s := range scp.peers {
		args := &ProcessMessageArgs{seq, scp.me, state.getCopy()}
		var reply ProcessMessageReply
		call(s, "ScpNode.ProcessMessage", args, &reply)
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

// -------- State update -------- //

// Tries to update it's state, given the information in M
// Returns: bool stating if updated or not
func (scp *ScpNode) tryToUpdateState(seq int) bool {
	// State before
	// TODO: implement func (state State) getCopy() {}
	stateBefore := scp.slots[seq].states[scp.me].getCopy() // Copy it!!

	// Apply the SCP steps
	step0(seq)
	step1(seq)
	step2(seq)
	step3(seq)
	step4(seq)

	// Check if there was an update
	// TODO: Can't easily compare this kind of struct. Create method to compare?
	stateAfter := scp.slots[seq].states[scp.me].getCopy()
	if stateBefore.b.n != stateAfter.b.n ||
		stateBefore.p.n != stateAfter.p.n ||
		stateBefore.pOld.n != stateAfter.pOld.n ||
		stateBefore.c.n != stateAfter.c.n ||
		stateBefore.phi != stateAfter.phi {
		return true
	}
	return false
}

// Try to update b
// TODO: Not change when phi = EXTERNALIZE?
func (scp *ScpNode) step0(seq int) {
	myState := scp.slots[seq].states[scp.me]

	if myState.phi == EXTERNALIZE {
		return
	}

	for _, state := range slot.states {
		greaterThanB, _ := compareBallots(state.b, myState.b)
		_, compatibleWithC := compareBallots(state.b, myState.c)

		if greaterThanB && compatibleWithC {
			myState.b = state.b
		}
	}
}

// Try to update p
func (scp *ScpNode) step1(seq int) {
	myState := scp.slots[seq].states[scp.me]

	// Conditions: phi = PREPARE and b > p > c
	if myState.phi != PREPARE {
		return
	}

	if greater, _ := compareBallots(myState.b, myState.p); !greater {
		return
	}

	if greater, _ := compareBallots(myState.p, myState.c); !greater {
		return
	}

	// To accept prepare b, we need a quorum voting/accepting prepare b
	// Hence we need a quorum with b = myState.b or p = myState.b or pOld = myState.b
	isValidB := func (peerState State) bool {
		return areBallotsEqual(peerState.b, myState.b)
	}

	isValidP := func (peerState State) bool {
		return areBallotsEqual(peerState.p, myState.b)
	}

	isValidPOld := func (peerState State) bool {
		return areBallotsEqual(peerState.pOld, myState.b)
	}

	if scp.hasQuorum(isValidB) || scp.hasQuorum(isValidP) || scp.hasQuorum(isValidPOld) {
		scp.updatePs(seq, myState.b)
		return
	}

	// Or we need a v-blocking accepting prepare b
	// Hence we need a v-blocking with p = myState.b or pOld = myState.b
	if scp.hasVBlocking(isValidP) || scp.hasVBlocking(isValidPOld) {
		scp.updatePs(seq, myState.b)
		return
	}
}

func (scp *ScpNode) updatePs(seq int, newP Ballot) {
	myState:= scp.slots[seq].states[scp.me]

	// Only update pOld if necessary
	if _, compatible := compareBallots(pOldMin, myState.p); compatible {
		myState.pOld = myState.p
	}
	myState.p = newP

	// Ensure the invariant p ~ c
	if _, compatible := compareBallots(myState.p, myState.c); !compatible {
		// This means that we went from voting to commit c to accept (abort c)
		// by setting c = 0, which is valid in FBA voting
		myState.c = Ballot{}
	}
}

// Try to update c
func (scp *ScpNode) step2(seq int) {
	myState:= scp.slots[seq].states[scp.me]

	// Conditions: phi = PREPARE, b = p, b != c
	if myState.phi != PREPARE {
		return
	}

	if !areBallotsEqual(myState.b, myState.p) {
		return
	}

	if areBallotsEqual(myState.b, myState.c) {
		return
	}

	// To vote on commit c, we need to confirm prepare b,
	// so we need a quorum accepting prepare b
	// Hence we need a quorum with p = myState.b(= myState.p) or pOld = myState.b(= myState.p)
	isValidP := func (peerState State) bool {
		return areBallotsEqual(peerState.p, myState.b)
	}

	isValidPOld := func (peerState State) bool {
		return areBallotsEqual(peerState.pOld, myState.b)
	}

	if scp.hasQuorum(isValidP) || scp.hasQuorum(isValidPOld) {
		myState.c = myState.b
		return
	}
}

// Try to update phi: PREPARE -> FINISH
func (scp *ScpNode) step3(seq int) {
	myState:= scp.slot[seq].states[scp.me]

	// Conditions: b = p = c
	if !(areBallotsEqual(myState.b, myState.p) && areBallotsEqual(myState.p, myState.c)) {
		return
	}

	// To accept commit c, we need a quorum voting/accepting commit c
	// Hence we need a quorum with our c
	isValid := func (peerState State) bool {
		return areBallotsEqual(peerState.c, myState.c)
	}

	if scp.hasQuorum(isValid) {
		myState.phi = FINISH
		return
	}

	// Or we need a v-blocking accepting commit c
	// Hence we need a v-blocking with our c and phi = FINISH
	isValid = func (peerState State) bool {
		return areBallotsEqual(peerState.c, myState.c) && (peerState.phi == FINISH)
	}

	if scp.hasVBlocking(isValid) {
		myState.phi = FINISH
		return
	}
}

// Try to update phi: FINISH -> EXTERNALIZE
func (scp *ScpNode) step4(seq int) {
	myState:= scp.slot[seq].states[scp.me]

	// Conditions : phi = FINISH
	if myState.phi != FINISH {
		return
	}

	// To confirm commit c, we need a quorum accepting commit c
	// Hence we need a quorum with our c and phi = FINISH
	isValid := func (peerState State) bool {
		return areBallotsEqual(peerState.c, myState.c) && (peerState.phi == FINISH)
	}

	if scp.hasQuorum(isValid) {
		myState.phi = EXTERNALIZE
		return
	}
}

// -------- Helper Functions -------- //

// Returns true if there is a quorum that satisfies the validator isValid
func (scp *ScpNode) hasQuorum(isValid func(state State) bool) bool {
	for _, quorum := range scp.quorums {
		if isQuorumValid(quorum, isValid) {
			return true
		}
	}
	return false
}

// Checks if all quorum members satisfy the validator isValid
func isQuorumValid(quorum Quorum, isValid func(state State) bool) bool {
	for _, nodeID := range quorum {
		state := scp.states[nodeId]
		if !isValid(state) {
			return false
		}
	}
	return true
}

// Returns true if there all of it's slices are blocked
func (scp *ScpNode) hasVBlocking(isValid func(state State) bool) bool {
	for _, slice := range scp.peerSlices[scp.me] {
		if !isSliceBlocked(slice, isValid) {
			return false
		}
	}
	return true
}

// A slice is blocked if any of it's elements satisfies the validator
func isSliceBlocked(slice Slice, isValid func(state State) bool) bool {
	for _, nodeID := range slice {
		state = scp.states[nodeID]
		if isValid(state) {
			return true
		}
	}
	return false
}

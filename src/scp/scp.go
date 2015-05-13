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
	id    int

	peers map[int]string // All the peers discovered so far
	peerSlices map[int][]QuorumSlice

	slots map[int]*Slot // SCP runs in a Slot
	quorums []Quorum
}

/* -------- State -------- */

type Slot struct {
	// Slot state
	b        Ballot
	p        Ballot
	pOld     Ballot
	c        Ballot
	phi      Phase

	// Every other node's state
	states map[int]*State 
}

func (slot *Slot) toState() State {
	return State{slot.b, slot.p, slot.pOld, slot.c, slot.phi}
}

/* -------- State -------- */

type State struct {
	b    Ballot
	p    Ballot
	pOld Ballot
	c    Ballot
	phi  Phase
}

// Translates field string into ballot from state
func (state *State) getBallot(field string) {
	switch field {
		case "b": return state.b
		case "p": return state.p
		case "pOld": return state.pOld
		case "c": return state.c
	}
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

/* -------- Init -------- */

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
}

func (scp *ScpNode) ExchangeQSlices(args *ExchangeQSlicesArgs, reply *ExchangeQSlicesReply) {
	// TODO: :P
}

// ------- RPC HANDLERS ------- //

// Reads and process the message from other scp nodes
func (scp *ScpNode) ProcessMessage(args *ProcessMessageArgs, reply *ProcessMessageReply) error {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	slot := scp.slots[args.Seq]
	slot.states[args.ID] = args.State // maybe checking if message is the most updated one... ?

	scp.tryToUpdateState(args.Seq)

	if slot.states[args.ID] != slot {
		scp.broadcastMessage(args.Seq)
	}
}

// Broadcast message to other scp nodes
func (scp *ScpNode) broadcastMessage(seq int) {
	slot := scp.slots[args.Seq]
	args := &Args{seq, scp.me, slot.toState()}

	for id, s := range scp.peers {
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

// -------- State -------- //

// Tries to update it's state, given the information in M
// Returns: bool stating if updated or not
// XXX Why on stellar they broadcast for every step?
func (scp *ScpNode) tryToUpdateState(seq int) bool {
	slot := scp.slots[seq]

	step0(seq)
	step1(seq)
	step2(seq)
	step3(seq)
	step4(seq)
}

func (scp *ScpNode) step0(seq int) {
	slot := scp.slots[seq]

	for _, state := range slot.states {
		greater, _ := compareBallots(state.b, slot.b)
		_, compatible := compareBallots(state.b, slot.c)

		if greater && compatible {
			slot.b = state.b
		}
	}
}

// Update p
func (scp *ScpNode) step1(seq int) {
	slot := scp.slots[seq]

	// Conditions: phi = PREPARE and b > p > c
	if slot.phi != PREPARE {
		return
	}

	if greater, _ := compareBallots(slot.b, slot.p); !greater {
		return
	}

	if greater, _ := compareBallots(slot.p, slot.c); !greater {
		return
	}

	// Quorum all votes/accepts on aborting b's smaller and incompatible with 'beta'
	// then set p = 'beta'
	if candidateB, ok := scp.checkQuorums(seq, "b"); ok {
		if greater, _ := compareBallots(candidateB, slot.p); greater {
			scp.updatePs(seq, candidateB)
			return
		}
	}

	if candidateP, ok := scp.checkQuorums(seq, "p"); ok {
		if greater, _ := compareBallots(candidateP, slot.p); greater {
			scp.updatePs(seq, candidateP)
			return
		}
	}

	if candidatePOld, ok := scp.checkQuorums(seq, "pOld"); ok {
		if greater, _ := compareBallots(candidatePOld, slot.p); greater {
			scp.updatePs(seq, candidatePOld)
			return
		}
	}

	// V-BlockingSet all accepts on aborting b's smaller and incompatible with 'beta'
	// then set p = 'beta'
	if candidateP, ok := scp.checkVblockings(seq, "p"); ok {
		if greater, _ := compareBallots(candidateP, slot.p); greater {
			scp.updatePs(seq, candidateP)
			return
		}
	}

	if candidatePOld, ok := scp.checkVBlockings(seq, "pOld"); ok {
		if greater, _ := compareBallots(candidatePOld, slot.p); greater {
			scp.updatePs(seq, candidatePOld)
			return
		}
	}
}

func (scp *ScpNode) updatePs(seq int, newP Ballot) {
	slot := scp.slots[seq]

	// Only update pOld if necessary
	if _, compatible := compareBallots(pOldMin, slot.p); compatible {
		slot.pOld = slot.p
	}
	slot.p = newP

	// Ensure the invariant p ~ c
	if _, compatible := compareBallots(slot.p, slot.c); !compatible {
		// This means that we went from voting to commit c to accept (abort c)
		// by setting c = 0, which is valid in FBA voting
		slot.c = Ballot{}
	}
}

// Try to update c
// Node confirms b(= p) is prepared: a quorum of accepts that b is prepared
func (scp *ScpNode) step2(seq int) {
	slot := scp.slots[seq]

	// Conditions: phi = PREPARE, b = p, b != c
	if slot.phi != PREPARE {
		return
	}

	if !areBallotsEqual(slot.b, slot.p) {
		return
	}

	if areBallotsEqual(slot.b, slot.c) {
		return
	}

	// Quorum all accepts on aborting b's smallers and incompatible with p
	// then set c = b(= p)
	if candidateP, ok := scp.checkQuorums(seq, "p"); ok {
		if greater, _ := compareBallots(candidate, slot.p); ok {
			// Conservatively update c to b, not to candidate
			// so we keep the invariant c <= p <= b
			slot.c = slot.b
			return
		}
	}

	if candidatePOld, ok := scp.checkQuorums(seq, "pOld"); ok {
		if greater, _ := compareBallots(candidate, slot.p); ok {
			// Conservatively update c to b, not to candidate
			// so we keep the invariant c <= p <= b
			slot.c = slot.b
			return
		}
	}
}

func (scp *ScpNode) step3() {

}

func (scp *ScpNode) step4() {

}

// -------- Helper Functions -------- //


// --- Step 2 helpers --- //

// field: b, p, pOld or c
// Check every quorum for a minCompatible ballot (candidate), and returns the best of these (the max)
// If not found, found = false
func (scp *ScpNode) checkQuorums(seq int, field string) (Ballot, bool) {
	slot := scp.slots[seq]
	bestCandidate := Ballot{} // the max of all the minCompatible ballots
	found := false

	for _, quorum := range scp.quorums {
		if minBallot, ok := scp.minCompatible(quorum, field); ok {
			if greater, compatible := compareBallots(minBallot, bestCandidate); greater {
				bestCandidate = minBallot
				found = true
			}
		}
	}

	return bestCandidate, found
}

// field: p or pOld
// Check every v-blocking for a minCompatible ballot
// If found, return ballot, true
// else, return 0-ballot, false
func (scp *ScpNode) checkVBlockings(seq int, field string) (Ballot, bool) {
	slot := scp.slots[seq]

	// Here I break the abstraction of Ballots having ledger.Op's in order to index ballots by their values.
	// This map is of the form {ledger.Op -> {sliceId -> ballotId}}, or {v: {i: n}}
	invIndex := make(map[ledger.Op]map[int]int)

	for _, slice := range scp.peerSlices[scp.me] {
		for i, nodeID := range slice {
			nodeBallot := scp.states[nodeID].getBallot(field)

			_, ok := invIndex[nodeBallot.v]
			if !ok {
				invIndex[nodeBallot.v] = make(map[int]int)
				invIndex[nodeBallot.v][i] = nodeBallot.n
			} else {
				invIndex[nodeBallot.v][i] = min(nodeBallot.n, invIndex[nodeBallot.v][i])
			}
		}
	}

	// Finding the best candidate, as in scp.checkQuorums
	bestCandidate := Ballot{}
	found := false

	for v, m := range {
		if len(m) != len(scp.peerSlices[scp.me]) {
			continue
		}

		minBallot := Ballot{}
		for i, n := range m {
			if n < minBallot.n || minBallot.n == 0 {
				minBallot.n = n
				minBallot.v = v
			}
		}

		if minBallot.n > bestCandidate.n {
			bestCandidate = minBallot
			found = true
		}
	}

	return bestCandidate, found
}

// If each node on the quorum has a compatible field
// ifreturns the minimum ballot of these
// else returns a 0-ballot and false
// field: b, p, pOld or c
func (scp *ScpNode) minCompatible(seq int, quorum Quorum, field string) (Ballot, bool) {
	slot := scp.slots[seq]
	minBallot := Ballot{}

	for _, nodeID := range quorum {
		nodeBallot := scp.states[nodeID].getBallot(field)
		greater, compatible := compareBallots(minBallot, nodeBallot)
		if !compatible {
			return Ballot{}, false
		}
		if greater {
			minBallot = nodeBallot
		}
	}

	return minBallot, true
}

// --- Step 3 helpers --- //

// Check if every node in a quorum has the same value of c
// If found, return (ballot, true)
// else, return (0-ballot, false)
func (scp *ScpNode) hasQuorum(seq int) bool {
	slot := scp.slots[seq]

	for _, quorum := range scp.quorums {

		allSame := true
		for _, nodeID := range quorum {
			if slot.c != scp.states[nodeID].getBallot("c") {
				allSame = falses
			}
		}

		if allSame == true {
			return true
		}
	}

	return false
}

func (scp *ScpNode) hasVBlocking(seq int) bool {
	slot := scp.slots[seq]

	for _, slice := range scp.peerSlices[scp.me] {

		oneSame := false
		for _, nodeID := range slice {
			if slot.c == scp.states[nodeID].getBallot("c") {
				oneSame = true
				break
			}
		}

		if oneSame == false {
			return false
		}
	}

	return true
}

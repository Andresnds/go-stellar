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
}

func (scp *ScpNode) ExchangeQSlices(args *ExchangeQSlicesArgs, reply *ExchangeQSlicesReply) {
	// TODO: :P
}

// ------- RPC HANDLERS ------- //

// Reads and process the message from other scp nodes
func (scp *ScpNode) ProcessMessage(args *ProcessMessageArgs, reply *ProcessMessageReply) error {
	// Locking

	// Unpack arguments

	// Add to this slot's messages

	// Process this message / try to change state
	scp.tryToUpdateState(...)

	if some condition {
		// Create new message 

		// Broadcast message
		scp.broadcastMessage(message)
	}
}

// -------- Broadcast -------- //

func (scp *ScpNode) broadcastMessage(message) {
	// Create args from this message

	// Send to every peer

}

// -------- Quorum -------- //

// field: b, p, pOld or c
// Check every quorum for a minCompatible ballot
// If found, return ballot, true
// else, return 0-ballot, false
func (scp *ScpNode) checkQuorums(seq int, field string) (Ballot, bool) {
	slot := scp.slots[seq]

	minBallot := Ballot{}
	flag := false

	for _, quorum := range scp.quorums {
		if qBallot, ok := scp.minCompatible(quorum, field); ok {
			if greater, compatible := compareBallots(minBallot, qBallot); greater {
				minBallot = qBallot
				flag = true
			}
		}
	}

	return minBallot, flag
}

// field: p or pOld
// Check every v-blocking for a minCompatible ballot
// If found, return ballot, true
// else, return 0-ballot, false
func (scp *ScpNode) checkVBlockings(seq int, field string) (Ballot, bool) {
	slot := scp.slots[seq]

	minBallot := Ballot{}
	flag := false

	for _, quorum := range scp.quorums {
		if qBallot, ok := scp.minCompatible(quorum, field); ok {
			if greater, compatible := compareBallots(minBallot, qBallot); greater {
				minBallot = qBallot
				flag = true
			}
		}
	}

	return minBallot, flag
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

// -------- State -------- //

// Tries to update it's state, given the information in M
// Returns: bool stating if updated or not
// XXX Why on stellar they broadcast for every step?
func (scp *ScpNode) tryToUpdateState(seq int) bool {
	slot := scp.slots[seq]

	step0(seq)
	step1(seq)
	step2()
	step3()
	step4()
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

func (scp *ScpNode) step1(seq int) {

	slot := scp.slots[seq]

	// Condition for this step: b > p > c
	if greater, _ := compareBallots(slot.b, slot.p); !greater {
		return
	}

	if greater, _ := compareBallots(slot.p, slot.c); !greater {
		return
	}

	// TODO: check if slot.phi is prepared

	// Quorum all votes/accepts on aborting b's smaller and incompatible with 'beta'
	// then set p = 'beta'
	if candidateB, ok := scp.checkQuorums(seq, "b"); ok {
		if greater, _ := compareBallots(candidateB, slot.p); greater {
			scp.updatePs(seq, candidateB)
		}
	}

	if candidateP, ok := scp.checkQuorums(seq, "p"); ok {
		if greater, _ := compareBallots(candidateP, slot.p); greater {
			scp.updatePs(seq, candidateP)
		}
	}

	if candidatePOld, ok := scp.checkQuorums(seq, "pOld"); ok {
		if greater, _ := compareBallots(candidatePOld, slot.p); greater {
			scp.updatePs(seq, candidatePOld)
		}
	}

	// V-BlockingSet all accepts on aborting b's smaller and incompatible with 'beta'
	// then set p = 'beta'
	if candidateP, ok := scp.checkVblockings(seq, "p"); ok {
		if greater, _ := compareBallots(candidateP, slot.p); greater {
			scp.updatePs(seq, candidateP)
		}
	}

	if candidatePOld, ok := scp.checkVBlockings(seq, "pOld"); ok {
		if greater, _ := compareBallots(candidatePOld, slot.p); greater {
			scp.updatePs(seq, candidatePOld)
		}
	}
}

func (scp *ScpNode) updatePs(seq int, newP Ballot) {
	slot := scp.slots[seq]

	// Only update pOld if necessary
	if _, compatible := compareBallots(pOldMin, slot.p); compatible {
		slot.pOld = p
	}
	slot.p = newP
}

func (scp *ScpNode) step2() {

}

func (scp *ScpNode) step3() {

}

func (scp *ScpNode) step4() {

}

package scp

/* Imports */

type Phase int
const (
	PREPARE Phase = iota + 1
	FINISH
	EXTERNALIZE
)

type ScpNode struct {
	mu    sync.Mutex
	l     net.Listener
	id    int
	peers map[int]string // All the peers discovered so far
	slots map[int]*Slot

	quorumSlices []QuorumSlice
	peersSlices  map[int][]QuorumSlice
}
type QuorumSlice []int // nodes ids

type Quorum []int // nodes ids

/* -------- Slot -------- */

type Slot struct {
	b        Ballot
	p        Ballot
	pOld     Ballot
	c        Ballot
	messages map[int]Message // Last message from every node
	phi      Phase
}

func (slot Slot) hasQuorum(

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

func (scp *ScpNode) Init(quorumSlices []QuorumSlice) {
	scp.QuorumSlice = make([]QuorumSlice)
	for _, qs := range quorumSlices {

	}
}


/* -------- Quorum Slices -------- */

func (scp *ScpNode) ExchangeQSlices(args *ExchangeQSlicesArgs, reply *ExchangeQSlicesReply) {

}

/* -------- Message -------- */

type Message struct {
	b    Ballot
	p    Ballot
	pOld Ballot
	c    Ballot
	phi  Phase
	qSlices []QuorumSlices
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

// -------- State -------- //

// Tries to update it's state, given the information in M
// Returns: bool stating if updated or not
// XXX Why on stellar they broadcast for every step?
func (scp *ScpNode) tryToUpdateState(seq int, lastMsg Message) bool {
	slot := scp.slots[seq]

	step0(seq, lastMsg)
	step1()
	step2()
	step3()
	step4()
}

func (scp *ScpNode) step0(seq int, lastMsg Message) {
	slot := scp.slots[seq]

	greater, _ := compareBallots(lastMsg.b, slot.b)
	_, compatible := compareBallots(lastMsg.b, slot.c)

	if greater && compatible {
		slot.b = lastMsg.b
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

	quorums := scp.getQuorums()

	// Quorum all votes/accepts on aborting b's smaller and incompatible with 'beta'
	// then set p = 'beta'
	if bMin, ok := scp.checkQuorums(seq, "b"); ok {
		scp.updatePs(seq, bMin)
	}

	if pMin, ok := scp.checkQuorums(seq, "p"); ok {
		scp.updatePs(seq, pMin)
	}

	if pOldMin, ok := scp.checkQuorums(seq, "pOld"); ok {
		scp.updatePs(seq, pOldMin)
	}

	// V-BlockingSet all accepts on aborting b's smaller and incompatible with 'beta'
	// then set p = 'beta'
	if pMin, ok := scp.checkVblockings(seq, "p"); ok {
		scp.updatePs(seq, pMin)
	}

	if pOldMin, ok := scp.checkVBlockings(seq, "pOld"); ok {
		scp.updatePs(seq, pOldMin)
	}
}

func (scp *ScpNode) updatePs (seq int, newP Ballot) {
	slot := scp.slots[seq]

	// Only update pOld if necessary
	if _, compatible := compareBallots(pOldMin, slot.p); compatible {
		slot.pOld = p
	}
	slot.p = newP
}

func (scp *ScpNode) checkQuorums(field string) (Ballot, bool) {
	slot := scp.slots[seq]

	for quorum := range quorums {
		if minBallot, ok := scp.minCompatible(quorum, field); ok {
			if greater, compatible := compareBallots(minBallot, slot.p); greater {
				return minBallot, true

				// Update p and pOld
				if !compatible {
					slot.pOld = slot.p
				}
				slot.p = minBallot
				return true
			}
		}
	}
	return Ballot{}, false
}

func (scp *ScpNode) getQuorums() []Quorum {
}

// If each node on the quorum has a compatible field, returns the minimum ballot of these
// else returns a 0-ballot and false
// field: b, p, pOld or c
func (scp *ScpNode) minCompatible(quorum Quorum, field string) (Ballot, bool) {
}

func (scp *ScpNode) step2() {

}

func (scp *ScpNode) step3() {

}

func (scp *ScpNode) step4() {

}

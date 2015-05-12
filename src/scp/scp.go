package scp

/* Imports */

// ================================================================= //
// ===== ===== ===== ===== TYPES DEFINITIONS ===== ===== ===== ===== //
// ================================================================= //

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

type Slot struct {
	b        Ballot
	p        Ballot
	pOld     Ballot
	c        Ballot
	messages map[int]Message // Last message from every node
	phi      Phase
}

type QuorumSlice []int

type Quorum [][]int

/* -------- Ballot -------- */

type Ballot struct {
	n int
	v ledger.Op
}

// Returns two bools: b1 > b2 and b1 ~ b2
func (scp *ScpNode)  compare(b1, b2 Ballot) (greater, compatible bool) {
	greater = b1.n > b2.n
	compatible = b1.v == b2.v
	return
}

/* -------- Init -------- */

func (scp *ScpNode) Init(quorumSlices []QuorumSlice) {
	scp.QuorumSlice = make([]QuorumSlice)
	for _, qs := range quorumSlices {

	}
}


/* -------- Quorum Slices -------- */

func (scp *ScpNode) SyncQuorumSlice(args *SyncQSArgs, reply *SyncQSReply) {

}

/* -------- Message -------- */

type Message struct {
	b    Ballot
	p    Ballot
	pOld Ballot
	c    Ballot
	phi  Phase
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
func (scp *ScpNode) tryToUpdateState(seq int) bool {
	slot := scp.slots[seq]
	step0()
	step1()
	step2()
	step3()
	step4()
}

func (scp *ScpNode) step0() {
	if 
}

func (scp *ScpNode) step1() {

}

func (scp *ScpNode) step2() {

}

func (scp *ScpNode) step3() {

}

func (scp *ScpNode) step4() {

}

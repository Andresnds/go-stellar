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

	// What about quorum slices, how to represent them?
}

type Slot struct {
	b Ballot
	p Ballot
	p_other Ballot
	c Ballot
	//messages map[int]Message[] // Every Message from every node: (node id) -> (messages from it)
	messages map[int]Message // Last message from every node
	phase Phase
}

type QuorumSlice int[]

type Quorum int[]

type Ballot struct {
	n int
	v int // could be interface{}
}

func (b Ballot) compareTo(otherB Ballot) {

}

// ============================================================ //
// ===== ===== ===== ===== RPC HANDLERS ===== ===== ===== ===== //
// ============================================================ //

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

// ====================================================== //
// ===== ===== ===== ===== EXTRAS ===== ===== ===== ===== //
// ====================================================== //

func (scp *ScpNode) broadcastMessage(message) {
	// Create args from this message

	// Send to every peer

}

// Tries to update it's state, given the information in M
// Returns: bool stating if updated or not
// XXX Why on stellar they broadcast for every step?
func (scp *ScpNode) tryToUpdateState(seq int) bool {
	slot := scp.slots[seq]

	// Step 1
	if slot.phase == PREPARE && 

	// Step 2

	// Step 3

	// Step 4

}

func (scp *ScpNode) step0() {

}

func (scp *ScpNode) step1() {

}

func (scp *ScpNode) step2() {

}

func (scp *ScpNode) step3() {

}





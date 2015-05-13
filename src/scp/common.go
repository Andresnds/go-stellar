package scp

type ProcessMessageArgs struct {
	Seq int
	ID int
	State State
}

type ProcessMessageReply struct {
	Seq int
	ID int
	State State
}

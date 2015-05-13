package scp

type ProcessMessageArgs struct {
	Seq int
	ID int
	State State
}

// TODO: Add field Err error. Example: ErrAuthentication
type ProcessMessageReply struct {
	/* Empty */
}

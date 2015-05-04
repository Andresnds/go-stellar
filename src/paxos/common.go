package paxos

type PrepareArgs struct {
    Num             int

    Seq             int
    Me              int
    Done            int
}

type PrepareReply struct {
    Ok              bool
    Value           interface{}
    AcceptedNum     int

    Me              int
    Done            int
}

type AcceptArgs struct {
    Num             int
    Value           interface{}

    Seq             int
    Me              int
    Done            int
}

type AcceptReply struct {
    Ok              bool

    Me              int
    Done            int
}

type DecideArgs struct {
    Num             int
    Value           interface{}

    Seq             int
    Me              int
    Done            int
}

type DecideReply struct {

    Me              int
    Done            int
}
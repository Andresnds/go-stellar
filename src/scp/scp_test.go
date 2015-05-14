package scp

import "testing"
import "runtime"
import "strconv"
import "os"
import "time"
import "fmt"
import crand "crypto/rand"
import "encoding/base64"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func port(tag string, host int) string {
	s := "/var/tmp/857-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "scp-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func ndecided(t *testing.T, scp []*ScpNode, seq int) int {
	count := 0
	var v interface{}
	for i := 0; i < len(scp); i++ {
		if scp[i] != nil {
			decided, v1 := scp[i].Status(seq)
			if decided {
				if count > 0 && v != v1 {
					t.Fatalf("decided values do not match; seq=%v i=%v v=%v v1=%v",
						seq, i, v, v1)
				}
				count++
				v = v1
			}
		}
	}
	return count
}

func waitn(t *testing.T, scp []*ScpNode, seq int, wanted int) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if ndecided(t, scp, seq) >= wanted {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	nd := ndecided(t, scp, seq)
	if nd < wanted {
		t.Fatalf("too few decided; seq=%v ndecided=%v wanted=%v", seq, nd, wanted)
	}
}

func cleanup(scp []*ScpNode) {
	for i := 0; i < len(scp); i++ {
		if scp[i] != nil {
			scp[i].Kill()
		}
	}
}

func TestScpBasic(t *testing.T) {
	runtime.GOMAXPROCS(4)

	const nscp = 4
	var scp []*ScpNode = make([]*ScpNode, nscp)
	var scph []string = make([]string, nscp)
	scpm := make(map[int][][]int)
	defer cleanup(scp)

	scpm[0] = [][]int{ []int{0, 1, 2} }
	scpm[1] = [][]int{ []int{1, 2, 3} }
	scpm[2] = [][]int{ []int{1, 2, 3} }
	scpm[3] = [][]int{ []int{1, 2, 3} }

	for i := 0; i < nscp; i++ {
		scph[i] = port("basic", i)
	}
	for i := 0; i < nscp; i++ {
		scp[i] = StartServer(scph, i, scpm)
	}

	fmt.Printf("Test: Single proposer ...\n")

	op := Op{}
	op.XID = int64(1)
	scp[0].Start(0, op)
	waitn(t, scp, 0, 1)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, same value ...\n")

	for i := 0; i < nscp; i++ {
		scp[i].Start(1, Op{XID: int64(10)})
	}
	waitn(t, scp, 1, 1)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, different values ...\n")

	scp[0].Start(2, Op{XID: int64(100)})
	scp[1].Start(2, Op{XID: int64(101)})
	scp[2].Start(2, Op{XID: int64(102)})
	waitn(t, scp, 2, 1)

	fmt.Printf("  ... Passed\n")
}

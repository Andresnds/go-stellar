package scp

import "testing"
// import "runtime"
// import "strconv"
// import "os"
// import "time"
import "fmt"
// import "math/rand"
// import "strings"
// import "sync/atomic"

// func check(t *testing.T, ck *Clerk, account string, value float32) {
// 	v, _ := ck.GetBalance(account)
// 	if v != value {
// 		t.Fatalf("GetBalance(%v) -> %f, expected %f", account, v, value)
// 	}
// }

// func port(tag string, host int) string {
// 	s := "/var/tmp/857-"
// 	s += strconv.Itoa(os.Getuid()) + "/"
// 	os.Mkdir(s, 0777)
// 	s += "ledger-"
// 	s += strconv.Itoa(os.Getpid()) + "-"
// 	s += tag + "-"
// 	s += strconv.Itoa(host)
// 	return s
// }

// func cleanup(lga []*Ledger) {
// 	for i := 0; i < len(lga); i++ {
// 		if lga[i] != nil {
// 			lga[i].kill()
// 		}
// 	}
// }

// func pp(tag string, src int, dst int) string {
// 	s := "/var/tmp/857-"
// 	s += strconv.Itoa(os.Getuid()) + "/"
// 	s += "ledger-" + tag + "-"
// 	s += strconv.Itoa(os.Getpid()) + "-"
// 	s += strconv.Itoa(src) + "-"
// 	s += strconv.Itoa(dst)
// 	return s
// }

// func cleanpp(tag string, n int) {
// 	for i := 0; i < n; i++ {
// 		for j := 0; j < n; j++ {
// 			ij := pp(tag, i, j)
// 			os.Remove(ij)
// 		}
// 	}
// }

func TestBasic(t *testing.T) {
	fmt.Println(":D")

	// runtime.GOMAXPROCS(4)

	// const nservers = 3
	// var lga []*Ledger = make([]*Ledger, nservers)
	// var lgh []string = make([]string, nservers)
	// defer cleanup(lga)

	// for i := 0; i < nservers; i++ {
	// 	lgh[i] = port("basic", i)
	// }
	// for i := 0; i < nservers; i++ {
	// 	lga[i] = StartServer(lgh, i)
	// }

	// ck := MakeClerk(lgh)
	// var cka [nservers]*Clerk
	// for i := 0; i < nservers; i++ {
	// 	cka[i] = MakeClerk([]string{lgh[i]})
	// }

	// fmt.Printf("Test: Basic InsertCoins/Transaction/GetBalance ...\n")

	// ck.InsertCoins("acc1", 100.0)
	// ck.Transaction("acc1", "acc2", 70.0, "sign")
	// check(t, ck, "acc1", 30.0)
	// check(t, ck, "acc2", 70.0)

	// ck.InsertCoins("acc", 10.0)
	// check(t, ck, "acc", 10.0)

	// cka[1].InsertCoins("acc", 15.0)

	// check(t, cka[2], "acc", 25.0)
	// check(t, cka[1], "acc", 25.0)
	// check(t, ck, "acc", 25.0)

	// fmt.Printf("  ... Passed\n")

	// fmt.Printf("Test: Basic Invalid Transaction ...\n")

	// ck.InsertCoins("acc10", 100.0)
	// if ck.Transaction("acc10", "acc20", -70.0, "sign") {
	// 	t.Fatalf("Negative value")
	// }

	// if ck.Transaction("acc10", "acc20", 101.0, "sign") {
	// 	t.Fatalf("Insuficient balance accepted")
	// }

	// fmt.Printf("  ... Passed\n")

	// fmt.Printf("Test: Concurrent clients ...\n")

	// for iters := 0; iters < 20; iters++ {
	// 	const npara = 15
	// 	var ca [npara]chan bool
	// 	for nth := 0; nth < npara; nth++ {
	// 		ca[nth] = make(chan bool)
	// 		go func(me int) {
	// 			defer func() { ca[me] <- true }()
	// 			ci := (rand.Int() % nservers)
	// 			myck := MakeClerk([]string{lgh[ci]})
	// 			if (rand.Int() % 1000) < 300 {
	// 				myck.InsertCoins("b", float32(rand.Int()))
	// 			} else if (rand.Int() % 1000) < 300 {
	// 				value := float32(rand.Int()%10)
	// 				myck.Transaction("b", "a", value, "sign")
	// 				myck.Transaction("a", "b", value, "sign")
	// 			} else {
	// 				myck.GetBalance("b")
	// 			}
	// 		}(nth)
	// 	}
	// 	for nth := 0; nth < npara; nth++ {
	// 		<-ca[nth]
	// 	}
	// 	var va [nservers]float32
	// 	for i := 0; i < nservers; i++ {
	// 		va[i], _ = cka[i].GetBalance("b")
	// 		if va[i] != va[0] {
	// 			t.Fatalf("mismatch")
	// 		}
	// 	}
	// }

	// fmt.Printf("  ... Passed\n")

	// time.Sleep(1 * time.Second)
}

// func part(t *testing.T, tag string, npaxos int, p1 []int, p2 []int, p3 []int) {
// 	cleanpp(tag, npaxos)

// 	pa := [][]int{p1, p2, p3}
// 	for pi := 0; pi < len(pa); pi++ {
// 		p := pa[pi]
// 		for i := 0; i < len(p); i++ {
// 			for j := 0; j < len(p); j++ {
// 				ij := pp(tag, p[i], p[j])
// 				pj := port(tag, p[j])
// 				err := os.Link(pj, ij)
// 				if err != nil {
// 					t.Fatalf("os.Link(%v, %v): %v\n", pj, ij, err)
// 				}
// 			}
// 		}
// 	}
// }

// func TestPartition(t *testing.T) {
// 	runtime.GOMAXPROCS(4)

// 	tag := "partition"
// 	const nservers = 5
// 	var lga []*Ledger = make([]*Ledger, nservers)
// 	defer cleanup(lga)
// 	defer cleanpp(tag, nservers)

// 	for i := 0; i < nservers; i++ {
// 		var lgh []string = make([]string, nservers)
// 		for j := 0; j < nservers; j++ {
// 			if j == i {
// 				lgh[j] = port(tag, i)
// 			} else {
// 				lgh[j] = pp(tag, i, j)
// 			}
// 		}
// 		lga[i] = StartServer(lgh, i)
// 	}
// 	defer part(t, tag, nservers, []int{}, []int{}, []int{})

// 	var cka [nservers]*Clerk
// 	for i := 0; i < nservers; i++ {
// 		cka[i] = MakeClerk([]string{port(tag, i)})
// 	}

// 	fmt.Printf("Test: No partition ...\n")

// 	part(t, tag, nservers, []int{0, 1, 2, 3, 4}, []int{}, []int{})
// 	cka[0].InsertCoins("1", 10)
// 	cka[2].InsertCoins("1", 20)
// 	check(t, cka[3], "1", 30)

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: Progress in majority ...\n")

// 	part(t, tag, nservers, []int{2, 3, 4}, []int{0, 1}, []int{})
// 	cka[2].InsertCoins("1", 20)
// 	check(t, cka[4], "1", 50)

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: No progress in minority ...\n")

// 	done0 := make(chan bool)
// 	done1 := make(chan bool)
// 	go func() {
// 		cka[0].Transaction("1", "2", 10, "sign")
// 		done0 <- true
// 	}()
// 	go func() {
// 		cka[1].GetBalance("1")
// 		done1 <- true
// 	}()

// 	select {
// 	case <-done0:
// 		t.Fatalf("Transaction in minority completed")
// 	case <-done1:
// 		t.Fatalf("GetBalance in minority completed")
// 	case <-time.After(time.Second):
// 	}

// 	check(t, cka[4], "1", 50)
// 	cka[3].Transaction("1", "2", 15, "sign")
// 	check(t, cka[4], "1", 35)

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: Completion after heal ...\n")

// 	part(t, tag, nservers, []int{0, 2, 3, 4}, []int{1}, []int{})

// 	select {
// 	case <-done0:
// 	case <-time.After(30 * 100 * time.Millisecond):
// 		t.Fatalf("Transaction did not complete")
// 	}

// 	select {
// 	case <-done1:
// 		t.Fatalf("GetBalance in minority completed")
// 	default:
// 	}

// 	check(t, cka[4], "1", 25)
// 	check(t, cka[0], "1", 25)
// 	check(t, cka[4], "2", 25)
// 	check(t, cka[0], "2", 25)

// 	part(t, tag, nservers, []int{0, 1, 2}, []int{3, 4}, []int{})

// 	select {
// 	case <-done1:
// 	case <-time.After(100 * 100 * time.Millisecond):
// 		t.Fatalf("GetBalance did not complete")
// 	}

// 	// check(t, cka[1], "1", "15")

// 	fmt.Printf("  ... Passed\n")
// }

// func randclerk(lgh []string) *Clerk {
// 	sa := make([]string, len(lgh))
// 	copy(sa, lgh)
// 	for i := range sa {
// 		j := rand.Intn(i + 1)
// 		sa[i], sa[j] = sa[j], sa[i]
// 	}
// 	return MakeClerk(sa)
// }

// func TestUnreliable(t *testing.T) {
// 	runtime.GOMAXPROCS(4)

// 	const nservers = 3
// 	var lga []*Ledger = make([]*Ledger, nservers)
// 	var lgh []string = make([]string, nservers)
// 	defer cleanup(lga)

// 	for i := 0; i < nservers; i++ {
// 		lgh[i] = port("un", i)
// 	}
// 	for i := 0; i < nservers; i++ {
// 		lga[i] = StartServer(lgh, i)
// 		lga[i].setunreliable(true)
// 	}

// 	ck := MakeClerk(lgh)
// 	var cka [nservers]*Clerk
// 	for i := 0; i < nservers; i++ {
// 		cka[i] = MakeClerk([]string{lgh[i]})
// 	}

// 	fmt.Printf("Test: Basic insert/get, unreliable ...\n")

// 	ck.InsertCoins("a", 10)
// 	check(t, ck, "a", 10)

// 	cka[1].InsertCoins("a", 20)

// 	check(t, cka[2], "a", 30)
// 	check(t, cka[1], "a", 30)
// 	check(t, ck, "a", 30)

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: Sequence of insert, unreliable ...\n")

// 	for iters := 0; iters < 6; iters++ {
// 		const ncli = 5
// 		var ca [ncli]chan bool
// 		for cli := 0; cli < ncli; cli++ {
// 			ca[cli] = make(chan bool)
// 			go func(me int) {
// 				ok := false
// 				defer func() { ca[me] <- ok }()
// 				myck := randclerk(lgh)
// 				acc := strconv.Itoa(me)
// 				vv, _ := myck.GetBalance(acc)
// 				myck.InsertCoins(acc, 0)
// 				vv += 0
// 				myck.InsertCoins(acc, 1)
// 				vv += 1
// 				myck.InsertCoins(acc, 2)
// 				vv += 2
// 				time.Sleep(100 * time.Millisecond)
// 				if val, _ := myck.GetBalance(acc); val != vv {
// 					t.Fatalf("wrong value")
// 				}
// 				ok = true
// 			}(cli)
// 		}
// 		for cli := 0; cli < ncli; cli++ {
// 			x := <-ca[cli]
// 			if x == false {
// 				t.Fatalf("failure")
// 			}
// 		}
// 	}

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: Concurrent Transaction to same acc, unreliable ...\n")

// 	ck.InsertCoins("rich", 1000000)

// 	ck.Transaction("rich", "poor", 1, "sign")

// 	ff := func(me int, ch chan int) {
// 		ret := -1
// 		defer func() { ch <- ret }()
// 		myck := randclerk(lgh)
// 		n := 0
// 		for n < 5 {
// 			myck.Transaction("rich", "poor", 10, "sign")
// 			n++
// 		}
// 		ret = n
// 	}

// 	ncli := 5
// 	cha := []chan int{}
// 	for i := 0; i < ncli; i++ {
// 		cha = append(cha, make(chan int))
// 		go ff(i, cha[i])
// 	}

// 	counts := []int{}
// 	for i := 0; i < ncli; i++ {
// 		n := <-cha[i]
// 		if n < 0 {
// 			t.Fatal("client failed")
// 		}
// 		counts = append(counts, n)
// 	}

// 	vx, _ := ck.GetBalance("poor")
// 	var sum float32
// 	sum += 1
// 	for _, v := range counts {
// 		sum += 10*float32(v)
// 	}
// 	if vx != sum {
// 		t.Fatalf("mismatch; poor got %v, should be %v", vx, sum)
// 	}


// 	{
// 		for i := 0; i < nservers; i++ {
// 			vi, _ := cka[i].GetBalance("poor")
// 			if vi != vx {
// 				t.Fatalf("mismatch; 0 got %v, %v got %v", vx, i, vi)
// 			}
// 		}
// 	}

// 	fmt.Printf("  ... Passed\n")

// 	time.Sleep(1 * time.Second)
// }
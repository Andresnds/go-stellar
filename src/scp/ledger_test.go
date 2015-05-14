package scp

import "testing"
// import "runtime"
import "strconv"
import "os"
// import "time"
// import "fmt"
// import mrand "math/rand"
// import "strings"
// import "sync/atomic"

import crand "crypto/rand"
import "crypto/dsa"
import "bytes"
import "encoding/gob"
import "math/big"

func check(t *testing.T, ck *Clerk, account string, value float32) {
	v, _ := ck.GetBalance(account)
	if v != value {
		t.Fatalf("GetBalance(%v) -> %f, expected %f", account, v, value)
	}
}

func ledgerPort(tag string, host int) string {
	s := "/var/tmp/857-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "ledger-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func ledgerCleanup(lga []*Ledger) {
	for i := 0; i < len(lga); i++ {
		if lga[i] != nil {
			lga[i].kill()
		}
	}
}

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

func getTransaction(acc1 *dsa.PublicKey, acc2 *dsa.PublicKey, value float32) []byte {
	gob.Register(dsa.PublicKey{})
	var transaction []byte

	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(*acc1)
	transaction = mCache.Bytes()

	mCache.Reset()
	encCache.Encode(*acc2)
	transaction = append(transaction, mCache.Bytes()...)

	mCache.Reset()
	encCache.Encode(value)
	transaction = append(transaction, mCache.Bytes()...)

	return transaction
}

func pkToString(acc *dsa.PublicKey) string {
	gob.Register(dsa.PublicKey{})

	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(*acc)
	return mCache.String()
}

func bigIntToString(i *big.Int) string {
	gob.Register(big.Int{})

	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(*i)
	return mCache.String()
}

// func TestLedgerBasic(t *testing.T) {
// 	runtime.GOMAXPROCS(4)

// 	const nservers = 4

// 	var lga []*Ledger = make([]*Ledger, nservers)
// 	var lgh []string = make([]string, nservers)
// 	lgm := make(map[int][][]int)
// 	defer ledgerCleanup(lga)

// 	for i := 0; i < nservers; i++ {
// 		lgh[i] = ledgerPort("basic", i)
// 	}
// 	lgm[0] = [][]int{ []int{0, 1, 2} }
// 	lgm[1] = [][]int{ []int{1, 2, 3} }
// 	lgm[2] = [][]int{ []int{1, 2, 3} }
// 	lgm[3] = [][]int{ []int{1, 2, 3} }

// 	for i := 0; i < nservers; i++ {
// 		lga[i] = Make(lgh, i, lgm)
// 	}

// 	ck := MakeClerk(lgh)
// 	var cka [nservers]*Clerk
// 	for i := 0; i < nservers; i++ {
// 		cka[i] = MakeClerk([]string{lgh[i]})
// 	}

// 	fmt.Printf("Test: Basic InsertCoins/Transaction/GetBalance ...\n")
	
// 	var params dsa.Parameters
// 	dsa.GenerateParameters(&params, crand.Reader, dsa.L2048N256)

// 	// privKey := dsa.PrivateKey{}
// 	// privKey.Parameters = params
// 	// dsa.GenerateKey(&privKey, crand.Reader)
// 	// acc := privKey.PublicKey

// 	privKey1 := dsa.PrivateKey{}
// 	privKey1.Parameters = params
// 	dsa.GenerateKey(&privKey1, crand.Reader)
// 	acc1 := privKey1.PublicKey
// 	acc1String := pkToString(&acc1)

// 	privKey2 := dsa.PrivateKey{}
// 	privKey2.Parameters = params
// 	dsa.GenerateKey(&privKey2, crand.Reader)
// 	acc2 := privKey2.PublicKey
// 	acc2String := pkToString(&acc2)

// 	ck.InsertCoins(acc1String, 100.0)

// 	r, s, _ := dsa.Sign(crand.Reader, &privKey1, getTransaction(&acc1, &acc2, 70.0))
// 	ck.Transaction(acc1String, acc2String, 70.0, bigIntToString(r), bigIntToString(s))
// 	check(t, ck, acc1String, 30.0)
// 	check(t, ck, acc2String, 70.0)

// 	// ck.InsertCoins(acc, 10.0)
// 	// check(t, ck, acc, 10.0)

// 	// cka[1].InsertCoins(acc, 15.0)

// 	// check(t, cka[2], acc, 25.0)
// 	// check(t, cka[1], acc, 25.0)
// 	// check(t, ck, acc, 25.0)

// 	fmt.Printf("  ... Passed\n")

// 	fmt.Printf("Test: Basic Invalid Transaction ...\n")
// }

func _testDigitalSignature(t *testing.T) {
	var params dsa.Parameters
	dsa.GenerateParameters(&params, crand.Reader, dsa.L2048N256)

	priv := dsa.PrivateKey{}
	priv.Parameters = params
	dsa.GenerateKey(&priv, crand.Reader)

	mCache := new(bytes.Buffer)
	encCache := gob.NewEncoder(mCache)
	encCache.Encode(Op{})

	r, s, _ := dsa.Sign(crand.Reader, &priv, mCache.Bytes())
	if !dsa.Verify(&priv.PublicKey, mCache.Bytes(), r, s) {
		t.Fatalf("Digital signature failed")
	}
}

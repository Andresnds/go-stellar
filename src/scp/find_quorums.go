package scp

import "fmt"
import "sort"

func merge(l1, l2 [][]int) ([][]int) {
	
	if len(l1) == 0 {
		return l2
	}
	if len(l2) == 0 {
		return l1
	}

	l := make([][]int, 0, len(l1) * len(l2))

	for _, l1_it := range l1 {
		for _, l2_it := range l2 {
			l = append(l, _merge(l1_it, l2_it))
		}
	}
	
	return l
}

func _merge(l1, l2 []int) ([]int) {
	m := make(map[int]bool)
	for _, i := range l1 {
		m[i] = true
	}

	for _, i := range l2 {
		m[i] = true
	}

	l := make([]int, len(m))
	i := 0
	for k, _ := range m {
		l[i] = k
		i++
	}
	sort.Ints(l)
	return l
}

// assumes l1 and l2 sorted

func isSubset(l1, l2 []int) bool {
	l1_index := 0
	l2_index := 0

	for l1_index != len(l1) && l2_index != len(l2) {
		if l1[l1_index] == l2[l2_index] {
			l1_index++
			l2_index++
		} else {
			l2_index++
		}
	}

	return l1_index == len(l1)
}

// l1 sorted

func _findQuorum(m map[int][][]int, l1 []int, l2 []int, r [][]int, out *[][]int) {
	if len(l1) == 0 {
		return
	}

	el := l1[0]
	l1 = l1[1:]
	l2 = append(l2, el)

	el_merged := merge(r, m[el])

	// fmt.Println(l2)
	// fmt.Println(el_merged)


	for _, el_slice_merged := range el_merged {
		if isSubset(el_slice_merged, l2) {
			*out = append(*out, el_slice_merged)
		}
	}

	for i := 0; i < len(l1); i++ {
		_findQuorum(m, l1[i:], l2, el_merged, out)
	}
}

func _algo(m map[int][][]int) [][]int {
	l1 := make([]int, 0, len(m))
	for k, v := range m {
		l1 = append(l1, k)
		for _, s := range v {
			sort.Ints(s)
		}
	}
	sort.Ints(l1)

	out := make([][]int, 0, len(m))
	for i := 0; i < len(l1); i++ {
		_findQuorum(m, l1[i:], []int{}, [][]int{}, &out)
	}
	return out
}

func findQuorums(quorumSliceM map[int][]QuorumSlice) []Quorum {

	// Transforming map[int][]QuorumSlice to of map[int][][]ints
	m := make(map[int][][]int)
	for k, v := range quorumSliceM {
		m[k] = make([][]int, len(v))
		for i, qSlice := range v {
			m[k][i] = []int(qSlice)
		}
	}

	out := _algo(m)

	// Transforming [][]int to of []Quorum
	quorumArray := make([]Quorum, len(out))
	for i, v := range out {
		quorumArray[i] = []int(v)
	}

	return quorumArray
}

func test1() {
	m := make(map[int][][]int)
	
	m[1] = [][]int{ []int{2, 3} }
	m[2] = [][]int{ []int{3, 4} }
	m[3] = [][]int{ []int{2, 4} }
	m[4] = [][]int{ []int{2, 3} }

	l1 := []int{1, 2, 3, 4}
	l2 := []int{}
	r := make([][]int, 0, 0)
	out := make([][]int, 0, 0)

	_findQuorum(m, l1, l2, r, &out)
	fmt.Println(out)
}

func test2() {
	m := make(map[int][][]int)
	
	m[1] = [][]int{ []int{1, 2, 3} }
	m[2] = [][]int{ []int{2, 3, 4} }
	m[3] = [][]int{ []int{2, 3, 4} }
	m[4] = [][]int{ []int{2, 3, 4} }

	l1 := []int{1, 2, 3, 4}
	l2 := []int{}
	r := make([][]int, 0, 0)
	out := make([][]int, 0, 0)

	_findQuorum(m, l1, l2, r, &out)
	fmt.Println(out)
}

func test3() {
	m := make(map[int][][]int)
	
	m[1] = [][]int{ []int{2, 3} }
	m[2] = [][]int{ []int{3, 4} }
	m[3] = [][]int{ []int{2, 4} }
	m[4] = [][]int{ []int{2, 3} }

	out := _algo(m)
	fmt.Println(out)
}


func test4() {
	m := make(map[int][][]int)
	
	m[1] = [][]int{ []int{1, 2, 3} }
	m[2] = [][]int{ []int{2, 3, 4} }
	m[3] = [][]int{ []int{2, 3, 4} }
	m[4] = [][]int{ []int{2, 3, 4} }

	out := _algo(m)
	fmt.Println(out)
}

func main() {
	// test1()
	// test2()
	// test3()
	// test4()
}
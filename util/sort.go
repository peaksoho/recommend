package util

import (
	"sort"
)

type SfPair struct {
	Key   string
	Value float64
}

type SfPairList []SfPair

func (p SfPairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p SfPairList) Len() int           { return len(p) }
func (p SfPairList) Less(i, j int) bool { return p[i].Value > p[j].Value } // 重写 Less() 方法， 从大到小排序

func SortMapByValue(m map[string]float64, typ string) SfPairList {
	p := make(SfPairList, len(m))
	i := 0
	for k, v := range m {
		p[i] = SfPair{k, v}
		i = i + 1
	}
	switch typ {
	case "desc":
		fallthrough
	case "DESC":
		sort.Sort(p)

	case "asc":
		fallthrough
	case "ASC":
		sort.Sort(sort.Reverse(p))
	default:
		sort.Sort(p)
	}
	return p
}

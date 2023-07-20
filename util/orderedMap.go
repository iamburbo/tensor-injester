package util

import (
	"container/list"

	"github.com/gagliardetto/solana-go/rpc"
)

// An OrderedMap is a map that remembers the order that keys were inserted.
// and limits the number of items it holds in memory.
type OrderedMap struct {
	m   map[string]*list.Element
	l   *list.List
	cap int
}

func NewOrderedMap(cap int) *OrderedMap {
	return &OrderedMap{
		m:   make(map[string]*list.Element),
		l:   list.New(),
		cap: cap,
	}
}

type entry struct {
	key   string
	value *rpc.TransactionSignature
}

// Put adds a value to the map.
func (om *OrderedMap) Put(sig *rpc.TransactionSignature) {
	// Check for existing item
	key := sig.Signature.String()

	// If element already exists, bump to front of
	// list
	if e, ok := om.m[key]; ok {
		om.l.MoveToFront(e)
		e.Value.(*entry).value = sig
		return
	}

	// If at capacity, remove oldest item
	if om.l.Len() > om.cap {
		om.removeOldest()
	}

	// Add new item
	ele := om.l.PushFront(&entry{key, sig})
	om.m[key] = ele
}

func (om *OrderedMap) Has(sig *rpc.TransactionSignature) bool {
	key := sig.Signature.String()
	_, ok := om.m[key]
	return ok
}

// Get fetches a value from the map.
func (om *OrderedMap) Get(key string) (value interface{}, ok bool) {
	if ele, hit := om.m[key]; hit {
		return ele.Value.(*entry).value, true
	}
	return
}

// removeOldest removes the oldest item from the map.
func (om *OrderedMap) removeOldest() {
	ele := om.l.Back()
	if ele != nil {
		om.removeElement(ele)
	}
}

// removeElement removes a given element from the map.
func (om *OrderedMap) removeElement(e *list.Element) {
	om.l.Remove(e)
	kv := e.Value.(*entry)
	delete(om.m, kv.key)
}

// Len returns the number of items in the map.
func (om *OrderedMap) Len() int {
	return om.l.Len()
}

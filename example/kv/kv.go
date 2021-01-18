package main

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/dshulyak/raft/types"
)

const (
	writeOpType byte = iota + 1
	deleteOpType
)

type op interface {
	apply(map[string]string) interface{}
}

type writeOp struct {
	key, val string
}

func (w *writeOp) apply(data map[string]string) interface{} {
	data[w.key] = w.val
	return w.val
}

type deleteOp struct {
	key string
}

func (d *deleteOp) apply(data map[string]string) interface{} {
	_, exists := data[d.key]
	if exists {
		delete(data, d.key)
	}
	return exists
}

type codec struct{}

func (c codec) write(key, val string) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&writeOp{key: key, val: val}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c codec) delete(key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&deleteOp{key: key}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c codec) decode(buf []byte) (op, error) {
	var o op
	if err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&o); err != nil {
		return nil, err
	}
	return o, nil
}

func newKv() *kv {
	return &kv{
		data: map[string]string{},
	}
}

type kv struct {
	cdc codec

	mu   sync.RWMutex
	data map[string]string
}

func (k *kv) Get(key string) (string, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	val, exists := k.data[key]
	return val, exists
}

func (k *kv) Apply(entry *types.Entry) interface{} {
	o, err := k.cdc.decode(entry.Op)
	if err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	return o.apply(k.data)
}

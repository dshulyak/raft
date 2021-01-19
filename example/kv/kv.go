package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

const (
	writeOpType byte = iota + 1
	deleteOpType
)

func init() {
	gob.Register(&writeOp{})
	gob.Register(&deleteOp{})
}

type op interface {
	apply(map[string]string) interface{}
	String() string
}

type writeOp struct {
	Key, Val string
}

func (w *writeOp) apply(data map[string]string) interface{} {
	data[w.Key] = w.Val
	return w.Val
}

func (w *writeOp) String() string {
	return fmt.Sprintf("write(%s)=%s", w.Key, w.Val)
}

type deleteOp struct {
	Key string
}

func (d *deleteOp) apply(data map[string]string) interface{} {
	_, exists := data[d.Key]
	if exists {
		delete(data, d.Key)
	}
	return exists
}

func (d *deleteOp) String() string {
	return fmt.Sprintf("delete(%s)", d.Key)
}

type codec struct{}

func (c codec) write(key, val string) ([]byte, error) {
	return c.encode(&writeOp{Key: key, Val: val})
}

func (c codec) delete(key string) ([]byte, error) {
	return c.encode(&deleteOp{Key: key})
}

func (c codec) encode(o op) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&o); err != nil {
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

func newKv(logger *zap.SugaredLogger) *kv {
	return &kv{
		logger: logger,
		data:   map[string]string{},
	}
}

type kv struct {
	logger *zap.SugaredLogger
	cdc    codec

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
	k.logger.Infow("applying operation", "operation", o, "error", err)
	if err != nil {
		return err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	rst := o.apply(k.data)
	k.logger.Infow("operation result", "operation", o, "result", rst)
	return rst
}

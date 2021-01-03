package raft

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkDurableStateSync(b *testing.B) {
	f, err := ioutil.TempFile("", "bench-durable-state-sync-")
	require.NoError(b, err)
	b.Cleanup(func() {
		f.Close()
		os.Remove(f.Name())
	})
	ds, err := NewDurableState(f)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := ds.Sync(); err != nil {
			require.NoError(b, err)
		}
	}
}

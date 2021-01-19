package raft

import "github.com/prometheus/client_golang/prometheus"

var (
	commitSec = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "consensus",
		Name:      "write_request_commit_seconds",
		Help:      "The latency distributions of time it takes to commit write request.",
		// from 1ms (0.001s) to 1s
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 11),
	})

	applySec = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "consensus",
		Name:      "write_request_apply_seconds",
		Help:      "The latency distributions of time it takes to apply write request.",
		// from 1ms (0.001s) to 1s
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 11),
	})

	readSec = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "consensus",
		Name:      "read_request_complete_seconds",
		Help:      "The latency distributions of time it takes to compete linearizable read request.",
		// from 1ms (0.001s) to 1s
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 11),
	})

	readCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "raft",
			Subsystem: "consensus",
			Name:      "read_total",
			Help:      "Total number of read requests.",
		})

	completedReadCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "raft",
			Subsystem: "consensus",
			Name:      "read_completed",
			Help:      "Total number of completed read requests.",
		})

	writeCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "raft",
			Subsystem: "consensus",
			Name:      "write_total",
			Help:      "Total number of write requests.",
		})

	commitedWriteCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "raft",
			Subsystem: "consensus",
			Name:      "write_commited",
			Help:      "Total number of commited write requests.",
		})

	appliedWriteCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "raft",
			Subsystem: "consensus",
			Name:      "write_applied",
			Help:      "Total number of applied write requests.",
		})
)

func init() {
	prometheus.MustRegister(commitSec)
	prometheus.MustRegister(applySec)
	prometheus.MustRegister(readSec)
	prometheus.MustRegister(readCounter)
	prometheus.MustRegister(completedReadCounter)
	prometheus.MustRegister(writeCounter)
	prometheus.MustRegister(commitedWriteCounter)
	prometheus.MustRegister(appliedWriteCounter)
}

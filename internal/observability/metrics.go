package observability

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Metrics holds Prometheus-compatible counters and histograms.
type Metrics struct {
	mu          sync.RWMutex
	counters    map[string]int64
	gauges      map[string]int64
	opLatencies map[string][]float64
	opBytes     map[string]int64
	opOK        map[string]int64
	opErrors    map[string]int64
}

// NewMetrics initialises empty metric storage.
func NewMetrics() *Metrics {
	return &Metrics{
		counters:    make(map[string]int64),
		gauges:      make(map[string]int64),
		opLatencies: make(map[string][]float64),
		opBytes:     make(map[string]int64),
		opOK:        make(map[string]int64),
		opErrors:    make(map[string]int64),
	}
}

// Record captures one operation for latency and throughput metrics.
func (m *Metrics) Record(op, status string, dur time.Duration, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.counters["wourifs_op_total{op=\""+op+"\",status=\""+status+"\"}"]++
	if status == "ok" {
		m.opOK[op]++
	} else {
		m.opErrors[op]++
	}
	m.opLatencies[op] = append(m.opLatencies[op], dur.Seconds())
	m.opBytes[op] += bytes
}

// SetGauge sets a named gauge to a value.
func (m *Metrics) SetGauge(name string, val int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name] = val
}

// ServeHTTP exposes metrics in Prometheus exposition format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	for name, val := range m.counters {
		writeLine(w, "# HELP "+name+" WouriFS metric", "")
		writeLine(w, "# TYPE "+name+" counter", "")
		writeLine(w, name+" "+strconv.FormatInt(val, 10), "\n")
	}

	for name, val := range m.gauges {
		writeLine(w, "# HELP "+name+" WouriFS gauge", "")
		writeLine(w, "# TYPE "+name+" gauge", "")
		writeLine(w, name+" "+strconv.FormatInt(val, 10), "\n")
	}

	for op, count := range m.opOK {
		writeLine(w, "# HELP wourifs_ops_ok WouriFS successful operations", "")
		writeLine(w, "# TYPE wourifs_ops_ok counter", "")
		writeLine(w, "wourifs_ops_ok{op=\""+op+"\"} "+strconv.FormatInt(count, 10), "\n")
	}

	for op, count := range m.opErrors {
		writeLine(w, "# HELP wourifs_ops_errors WouriFS failed operations", "")
		writeLine(w, "# TYPE wourifs_ops_errors counter", "")
		writeLine(w, "wourifs_ops_errors{op=\""+op+"\"} "+strconv.FormatInt(count, 10), "\n")
	}

	for op, latencies := range m.opLatencies {
		var sum float64
		for _, l := range latencies {
			sum += l
		}
		count := len(latencies)
		writeLine(w, "# HELP wourifs_op_duration_seconds WouriFS operation latency", "")
		writeLine(w, "# TYPE wourifs_op_duration_seconds histogram", "")
		writeLine(w, fmt.Sprintf("wourifs_op_duration_seconds_count{op=\"%s\"} %d", op, count), "")
		writeLine(w, fmt.Sprintf("wourifs_op_duration_seconds_sum{op=\"%s\"} %g", op, sum), "\n")
	}

	for op, bytes := range m.opBytes {
		writeLine(w, "# HELP wourifs_op_bytes_total WouriFS bytes transferred", "")
		writeLine(w, "# TYPE wourifs_op_bytes_total counter", "")
		writeLine(w, "wourifs_op_bytes_total{op=\""+op+"\"} "+strconv.FormatInt(bytes, 10), "\n")
	}
}

func writeLine(w http.ResponseWriter, line, suffix string) {
	data := append([]byte(line), '\n')
	if suffix != "" {
		data = append(data, '\n')
	}
	w.Write(data)
}

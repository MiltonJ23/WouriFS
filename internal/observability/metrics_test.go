package observability

import (
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetrics_BDD(t *testing.T) {
	t.Run("Given a fresh metrics registry", func(t *testing.T) {
		m := NewMetrics()

		t.Run("When recording operations", func(t *testing.T) {
			m.Record("CreateFile", "ok", 10*time.Millisecond, 1024)
			m.Record("CreateFile", "ok", 15*time.Millisecond, 2048)
			m.Record("CreateFile", "error", 5*time.Millisecond, 0)
			m.Record("ReadChunk", "ok", 25*time.Millisecond, 65536)

			t.Run("Then counters increment correctly", func(t *testing.T) {
				if m.opOK["CreateFile"] != 2 {
					t.Errorf("expected 2 ok CreateFile ops, got %d", m.opOK["CreateFile"])
				}
				if m.opErrors["CreateFile"] != 1 {
					t.Errorf("expected 1 error CreateFile op, got %d", m.opErrors["CreateFile"])
				}
				if m.opBytes["ReadChunk"] != 65536 {
					t.Errorf("expected 65536 bytes for ReadChunk, got %d", m.opBytes["ReadChunk"])
				}
			})

			t.Run("Then latencies are recorded", func(t *testing.T) {
				lats := m.opLatencies["CreateFile"]
				if len(lats) != 3 {
					t.Fatalf("expected 3 latencies, got %d", len(lats))
				}
			})
		})

		t.Run("When setting a gauge", func(t *testing.T) {
			m.SetGauge("wourifs_datanodes_available", 5)
			if m.gauges["wourifs_datanodes_available"] != 5 {
				t.Errorf("expected 5, got %d", m.gauges["wourifs_datanodes_available"])
			}
		})

		t.Run("When serving HTTP metrics endpoint", func(t *testing.T) {
			m.Record("StatFile", "ok", 1*time.Millisecond, 0)

			rec := httptest.NewRecorder()
			m.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

			body := rec.Body.String()
			if rec.Code != 200 {
				t.Errorf("expected 200, got %d", rec.Code)
			}
			if !strings.Contains(body, "wourifs_op_total") {
				t.Error("metrics output should contain wourifs_op_total counter")
			}
			if !strings.Contains(body, "wourifs_ops_ok") {
				t.Error("metrics output should contain wourifs_ops_ok")
			}
			if !strings.Contains(body, "wourifs_op_duration_seconds") {
				t.Error("metrics output should contain latency histogram")
			}
		})
	})
}

func TestLogger_BDD(t *testing.T) {
	t.Run("Given a test logger", func(t *testing.T) {
		l := NewTestLogger()

		t.Run("When starting and ending an operation", func(t *testing.T) {
			start, _ := l.OpStart("TestOp", "/test/path")
			time.Sleep(1 * time.Millisecond)
			l.OpEnd(start, "TestOp", "/test/path", nil, 42)

			if l.metrics.opOK["TestOp"] != 1 {
				t.Error("operation should be counted")
			}
		})

		t.Run("When ending an operation with error", func(t *testing.T) {
			start, _ := l.OpStart("TestOp", "/test/fail")
			time.Sleep(1 * time.Millisecond)
			l.OpEnd(start, "TestOp", "/test/fail", errTest, 0)

			if l.metrics.opErrors["TestOp"] != 1 {
				t.Error("error operation should be counted")
			}
		})
	})

	t.Run("Given a production logger", func(t *testing.T) {
		l := NewLogger(slog.LevelInfo)
		if l == nil {
			t.Fatal("production logger must not be nil")
		}
		if l.Metrics() == nil {
			t.Fatal("logger must have a metrics registry")
		}
	})
}

var errTest = fmt.Errorf("test error")

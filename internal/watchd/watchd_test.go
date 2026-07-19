package watchd

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWatchd_ProcessEmpty(t *testing.T) {
	w := New()
	alerts := w.Process(context.Background(), AuditEntry{
		EntryID: 1, Timestamp: time.Now(), UserID: "user-1",
		OperationType: "create_file", FilePath: "/wourifs/test/a.csv",
	})
	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts with no rules, got %d", len(alerts))
	}
}

func TestWatchd_OutOfHoursRule(t *testing.T) {
	w := New()
	w.RegisterRule(OutOfHoursRule(8, 17)) // business hours 08:00-17:00

	// Within hours: 10:00 AM -> no alert
	entryAM := AuditEntry{
		EntryID: 1, Timestamp: time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC),
		UserID: "user-1", OperationType: "create_file", FilePath: "/ledger.csv",
	}
	alerts := w.Process(context.Background(), entryAM)
	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts during business hours, got %d", len(alerts))
	}

	// Outside hours: 03:00 AM -> alert
	entryPM := AuditEntry{
		EntryID: 2, Timestamp: time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC),
		UserID: "user-2", OperationType: "delete_file", FilePath: "/secret.csv",
	}
	alerts = w.Process(context.Background(), entryPM)
	if len(alerts) != 1 {
		t.Fatalf("expected 1 alert outside hours, got %d", len(alerts))
	}
	if alerts[0].Type != "out_of_hours_write" {
		t.Errorf("expected out_of_hours_write alert, got %s", alerts[0].Type)
	}
	if alerts[0].UserID != "user-2" {
		t.Errorf("expected user-2, got %s", alerts[0].UserID)
	}
}

func TestWatchd_DormantUserRule(t *testing.T) {
	lastActivity := make(map[string]time.Time)
	var mu sync.Mutex

	getLast := func(userID string) (time.Time, bool) {
		mu.Lock()
		defer mu.Unlock()
		t, ok := lastActivity[userID]
		return t, ok
	}

	w := New()
	w.RegisterRule(DormantUserRule(30, getLast))

	// First activity — not dormant
	entry := AuditEntry{
		EntryID: 1, Timestamp: time.Now(), UserID: "user-1",
		OperationType: "create_file", FilePath: "/test.csv",
	}
	alerts := w.Process(context.Background(), entry)
	if len(alerts) != 0 {
		t.Errorf("first activity should not be flagged as dormant, got %d alerts", len(alerts))
	}

	// Simulate 45 days of inactivity
	mu.Lock()
	lastActivity["user-1"] = time.Now().Add(-45 * 24 * time.Hour)
	mu.Unlock()

	entry.EntryID = 2
	entry.Timestamp = time.Now()
	alerts = w.Process(context.Background(), entry)
	if len(alerts) != 1 {
		t.Fatalf("dormant user activity should fire, got %d alerts", len(alerts))
	}
	if alerts[0].Type != "dormant_user_activity" {
		t.Errorf("expected dormant_user_activity, got %s", alerts[0].Type)
	}
}

func TestWatchd_MultipleRules(t *testing.T) {
	w := New()
	w.RegisterRule(OutOfHoursRule(8, 17))

	// Register a custom rule
	w.RegisterRule(func(ctx context.Context, entry AuditEntry) *Alert {
		if entry.OperationType == "delete_file" {
			return &Alert{
				Type: "suspicious_delete", EntryID: entry.EntryID,
				UserID: entry.UserID, FilePath: entry.FilePath,
				Timestamp: entry.Timestamp, Description: "file deletion detected",
			}
		}
		return nil
	})

	entry := AuditEntry{
		EntryID: 1, Timestamp: time.Date(2026, 7, 19, 2, 0, 0, 0, time.UTC),
		UserID: "user-1", OperationType: "delete_file", FilePath: "/critical.csv",
	}
	alerts := w.Process(context.Background(), entry)
	// Both rules fire: out_of_hours + suspicious_delete
	if len(alerts) != 2 {
		t.Errorf("expected 2 alerts (out_of_hours + suspicious_delete), got %d", len(alerts))
	}
}

func TestWatchd_AlertCount(t *testing.T) {
	w := New()
	w.RegisterRule(OutOfHoursRule(8, 17))

	entry := AuditEntry{
		EntryID: 1, Timestamp: time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC),
		UserID: "user-1", OperationType: "create_file", FilePath: "/f.csv",
	}
	w.Process(context.Background(), entry)
	w.Process(context.Background(), entry)

	counts := w.AlertCount()
	if counts["out_of_hours_write"] != 2 {
		t.Errorf("expected 2 out_of_hours alerts, got %d", counts["out_of_hours_write"])
	}
}

func TestWatchd_Reset(t *testing.T) {
	w := New()
	w.RegisterRule(OutOfHoursRule(8, 17))

	entry := AuditEntry{
		EntryID: 1, Timestamp: time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC),
		UserID: "user-1", OperationType: "create_file", FilePath: "/f.csv",
	}
	w.Process(context.Background(), entry)
	if len(w.Alerts()) != 1 {
		t.Fatal("expected 1 alert before reset")
	}

	w.Reset()
	if len(w.Alerts()) != 0 {
		t.Error("expected 0 alerts after reset")
	}
}

func TestWatchd_Dispatcher(t *testing.T) {
	w := New()
	w.RegisterRule(OutOfHoursRule(8, 17))

	dispatched := make(chan Alert, 1)
	w.RegisterDispatcher(func(ctx context.Context, alert Alert) error {
		dispatched <- alert
		return nil
	})

	entry := AuditEntry{
		EntryID: 1, Timestamp: time.Date(2026, 7, 19, 3, 0, 0, 0, time.UTC),
		UserID: "user-1", OperationType: "create_file", FilePath: "/f.csv",
	}
	w.Process(context.Background(), entry)

	select {
	case a := <-dispatched:
		if a.Type != "out_of_hours_write" {
			t.Errorf("expected out_of_hours_write alert in dispatcher, got %s", a.Type)
		}
	case <-time.After(time.Second):
		t.Error("dispatcher was not called")
	}
}

func TestWatchd_LastActivity(t *testing.T) {
	w := New()
	now := time.Now()
	w.Process(context.Background(), AuditEntry{
		EntryID: 1, Timestamp: now, UserID: "user-a",
		OperationType: "create_file", FilePath: "/f.csv",
	})

	tm, ok := w.LastActivity("user-a")
	if !ok {
		t.Fatal("expected LastActivity for user-a to be tracked")
	}
	if !tm.Equal(now) {
		t.Errorf("expected %v, got %v", now, tm)
	}

	_, ok = w.LastActivity("ghost")
	if ok {
		t.Error("expected LastActivity for unknown user to return false")
	}
}

func TestWatchd_Uptime(t *testing.T) {
	w := New()
	time.Sleep(10 * time.Millisecond)
	if w.Uptime() < 10*time.Millisecond {
		t.Errorf("uptime too short: %v", w.Uptime())
	}
}

func TestWatchd_VelocityRule(t *testing.T) {
	rule := VelocityRule(10, time.Minute)
	// VelocityRule factory returns nil — actual counting is done by caller
	alert := rule(context.Background(), AuditEntry{
		EntryID: 1, Timestamp: time.Now(), UserID: "user-1",
		OperationType: "create_file", FilePath: "/f.csv",
	})
	if alert != nil {
		t.Error("VelocityRule factory should return nil")
	}
}

func TestWatchd_ChainIntegrityRule(t *testing.T) {
	rule := ChainIntegrityRule()
	alert := rule(context.Background(), AuditEntry{
		EntryID: 1, Timestamp: time.Now(), UserID: "watchd",
	})
	if alert != nil {
		t.Error("ChainIntegrityRule should return nil for empty chain hash")
	}
}

func TestWatchd_RaftLeadershipChange(t *testing.T) {
	rule := RaftLeadershipChangeRule("Leader", "Follower")
	alert := rule(context.Background(), AuditEntry{
		EntryID: 1, Timestamp: time.Now(),
	})
	if alert == nil {
		t.Fatal("expected raft_leadership_change alert")
	}
	if alert.Type != "raft_leadership_change" {
		t.Errorf("expected raft_leadership_change, got %s", alert.Type)
	}

	// Follower → Leader is normal operation, no alert
	rule2 := RaftLeadershipChangeRule("Follower", "Leader")
	alert = rule2(context.Background(), AuditEntry{EntryID: 2, Timestamp: time.Now()})
	if alert != nil {
		t.Error("Follower → Leader should not trigger alert")
	}
}

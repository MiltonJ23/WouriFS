package provision

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"
)

// Institution holds one Shamir share for an EMF.
type Institution struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Share     string `json:"share"` // hex-encoded big.Int (share y-value)
	ShareIdx  int32  `json:"share_idx"`
	Revoked   bool   `json:"revoked"`
	RevReason string `json:"rev_reason,omitempty"`
	CreatedAt int64  `json:"created_at"`
}

// Entry records one provisioning event for audit.
type Entry struct {
	InstitutionID string `json:"institution_id"`
	NodeID        string `json:"node_id"`
	TimestampUnix int64  `json:"timestamp_unix"`
	Action        string `json:"action"` // "get_share" or "revoke"
}

// DB is the in-memory store for institution shares and audit log.
// Shares are generated at deploy time (offline) and loaded from a JSON file.
// Not persisted by the service itself — the JSON file is the source of truth.
type DB struct {
	mu           sync.RWMutex
	institutions map[string]*Institution
	auditLog     []Entry
}

// NewDB creates an empty share database.
func NewDB() *DB {
	return &DB{institutions: make(map[string]*Institution)}
}

// LoadInstitutions reads the institution share file (JSON array).
func (db *DB) LoadInstitutions(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("load institutions: %w", err)
	}

	var insts []Institution
	if err := json.Unmarshal(data, &insts); err != nil {
		return fmt.Errorf("parse institutions: %w", err)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	for i := range insts {
		db.institutions[insts[i].ID] = &insts[i]
	}
	return nil
}

// GetShare returns the institution's share if not revoked.
func (db *DB) GetShare(institutionID, nodeID string) ([]byte, int32, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	inst, ok := db.institutions[institutionID]
	if !ok {
		return nil, 0, fmt.Errorf("institution %s not found", institutionID)
	}
	if inst.Revoked {
		return nil, 0, fmt.Errorf("institution %s revoked: %s", institutionID, inst.RevReason)
	}

	// Decode share from hex
	shareInt := new(big.Int)
	shareInt.SetString(inst.Share, 16)

	db.auditLog = append(db.auditLog, Entry{
		InstitutionID: institutionID,
		NodeID:        nodeID,
		TimestampUnix: time.Now().Unix(),
		Action:        "get_share",
	})

	return shareInt.Bytes(), inst.ShareIdx, nil
}

// Revoke blocks an institution permanently.
func (db *DB) Revoke(institutionID, reason string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	inst, ok := db.institutions[institutionID]
	if !ok {
		return fmt.Errorf("institution %s not found", institutionID)
	}
	inst.Revoked = true
	inst.RevReason = reason

	db.auditLog = append(db.auditLog, Entry{
		InstitutionID: institutionID,
		TimestampUnix: time.Now().Unix(),
		Action:        "revoke",
	})

	return nil
}

// AuditTrail returns provisioning entries for an institution since a timestamp.
func (db *DB) AuditTrail(institutionID string, since int64) []Entry {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var result []Entry
	for _, e := range db.auditLog {
		if e.InstitutionID == institutionID && e.TimestampUnix >= since {
			result = append(result, e)
		}
	}
	return result
}

// Generate shares for an institution using Shamir (offline tool, not gRPC).
// Returns the hex-encoded shares to be stored in the institutions JSON file.
func GenerateShares(secret []byte, k, n int) ([]string, error) {
	// Use shamir package internally — this is the provisioning tool path
	return nil, fmt.Errorf("use wouri-provision tool directly")
}

// Ensure imports used
var _ = sha256.New
var _ = fmt.Sprintf

package audit

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestAuditChain_BDD(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")

	t.Run("Given a fresh audit chain", func(t *testing.T) {
		c, err := OpenChain(path)
		if err != nil {
			t.Fatalf("open chain: %v", err)
		}
		defer c.Close()

		t.Run("When appending an audit entry", func(t *testing.T) {
			entry, err := c.Append("create_file", "/wourifs/test/ledger.csv", "", "user-auditor-1")
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if entry.Index != 0 {
				t.Errorf("expected index 0, got %d", entry.Index)
			}
			if entry.PrevHash == "genesis" {
				t.Error("prev_hash must differ from genesis after first entry")
			}
		})

		t.Run("When appending multiple entries", func(t *testing.T) {
			c.Append("write_chunk", "/wourifs/test/ledger.csv", fmt.Sprintf("%x", sha256.Sum256([]byte("data"))), "user-auditor-1")
			c.Append("delete_file", "/wourifs/test/ledger.csv", "", "user-auditor-1")

			// Verify chain integrity
			if err := c.Verify(); err != nil {
				t.Fatalf("chain verification failed: %v", err)
			}
		})
	})

	t.Run("Given a tampered audit log file", func(t *testing.T) {
		path2 := filepath.Join(dir, "audit-tampered.jsonl")

		c, _ := OpenChain(path2)
		c.Append("create_file", "/a", "", "user-1")
		c.Append("delete_file", "/a", "", "user-1")
		c.Close()

		// Tamper with the file: append a forged entry
		f, _ := os.OpenFile(path2, os.O_APPEND|os.O_WRONLY, 0640)
		f.Write([]byte(`{"index":2,"op":"forge","path":"/evil","prev_hash":"fake"}` + "\n"))
		f.Close()

		c2, _ := OpenChain(path2)
		defer c2.Close()

		err := c2.Verify()
		if err == nil {
			t.Error("chain verification MUST fail after tampering")
		}
	})

	t.Run("Given chain recovery on restart", func(t *testing.T) {
		path3 := filepath.Join(dir, "audit-recover.jsonl")
		c, _ := OpenChain(path3)
		c.Append("create_file", "/x", "", "user-1")
		c.Append("create_file", "/y", "", "user-1")
		c.Close()

		// Re-open — should recover lastHash and continue
		c2, _ := OpenChain(path3)
		entry, err := c2.Append("delete_file", "/x", "", "user-2")
		if err != nil {
			t.Fatalf("append after reopen: %v", err)
		}
		if entry.Index != 2 {
			t.Errorf("expected index 2 after reopen, got %d", entry.Index)
		}
		c2.Close()
	})
}

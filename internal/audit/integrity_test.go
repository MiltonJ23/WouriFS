package audit

import (
	"os"
	"path/filepath"
	"testing"
)

/*
 * End-to-end audit integrity test — create entries, verify chain,
 * corrupt an entry on disk, detect the corruption.
 */
func TestAuditLog_E2EIntegrity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")

	chain, err := OpenChain(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 10; i++ {
		if _, err := chain.Append("create_file", "/wourifs/test/file.csv",
			"0000000000000000000000000000000000000000000000000000000000000000",
			"user-test"); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	chain.Close()

	// Verify clean chain
	chain2, _ := OpenChain(path)
	if err := chain2.Verify(); err != nil {
		t.Fatalf("verify clean: %v", err)
	}
	chain2.Close()

	// Corrupt entry 5
	raw, _ := os.ReadFile(path)
	nl := 0
	for i, b := range raw {
		if b == '\n' {
			nl++
			if nl == 5 {
				raw[i-1] ^= 0x01 // flip last char of hash field
				break
			}
		}
	}
	os.WriteFile(path, raw, 0644)

	// Verify corrupted — must fail
	chain3, _ := OpenChain(path)
	if err := chain3.Verify(); err == nil {
		t.Error("expected verification failure after corruption")
	} else {
		t.Logf("corruption detected: %v", err)
	}
	chain3.Close()
}

func TestAuditLog_VerifyEmptyChain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.jsonl")
	os.WriteFile(path, []byte{}, 0644)
	chain, err := OpenChain(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := chain.Verify(); err != nil {
		t.Errorf("empty chain should verify ok: %v", err)
	}
	chain.Close()
}

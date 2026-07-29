package provision

import (
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/MiltonJ23/WouriFS/internal/shamir"
)

func TestDB_BDD(t *testing.T) {
	dir := t.TempDir()

	t.Run("Given a share database with institutions", func(t *testing.T) {
		// Generate real shares for institutions
		secret := make([]byte, 32)
		secret[0] = 0x42 // identifiable test secret

		shares, err := shamir.Split(secret, 2, 2)
		if err != nil {
			t.Fatalf("shamir split: %v", err)
		}

		// Write institutions JSON file
		institutions := `[
  {"id": "emf-001", "name": "FIFFA Test", "share": "` + shares[0][1].Text(16) + `", "share_idx": 1, "revoked": false, "created_at": 1700000000}
]`
		path := filepath.Join(dir, "institutions.json")
		os.WriteFile(path, []byte(institutions), 0644)

		db := NewDB()
		if err := db.LoadInstitutions(path); err != nil {
			t.Fatalf("load: %v", err)
		}

		t.Run("When getting a share for a valid institution", func(t *testing.T) {
			share, idx, err := db.GetShare("emf-001", "node-xyz")
			if err != nil {
				t.Fatalf("get share: %v", err)
			}
			if idx != 1 {
				t.Errorf("expected share index 1, got %d", idx)
			}
			if share == nil {
				t.Error("share must not be nil")
			}
		})

		t.Run("When revoking an institution", func(t *testing.T) {
			if err := db.Revoke("emf-001", "license revoked"); err != nil {
				t.Fatalf("revoke: %v", err)
			}

			_, _, err := db.GetShare("emf-001", "node-xyz")
			if err == nil {
				t.Error("expected error for revoked institution")
			}
		})

		t.Run("When requesting share for unknown institution", func(t *testing.T) {
			_, _, err := db.GetShare("unknown-emf", "node-xyz")
			if err == nil {
				t.Error("expected error for unknown institution")
			}
		})

		t.Run("When querying audit trail", func(t *testing.T) {
			entries := db.AuditTrail("emf-001", 0)
			if len(entries) < 2 { // get_share + revoke
				t.Errorf("expected at least 2 audit entries, got %d", len(entries))
			}
		})
	})

	t.Run("Given share reconstruction from DB shares", func(t *testing.T) {
		// Full round-trip: generate shares, store one in DB, one on "USB",
		// then reconstruct
		secret := make([]byte, 32)
		secret[0] = 0x53 // identifiable
		shares, err := shamir.Split(secret, 2, 2)
		if err != nil {
			t.Fatalf("split: %v", err)
		}

		// Store share-2 in DB (simulating COBAC)
		institutions := `[
  {"id": "emf-002", "name": "COMECI Test", "share": "` + shares[1][1].Text(16) + `", "share_idx": 2, "revoked": false, "created_at": 1700000000}
]`
		path := filepath.Join(dir, "institutions2.json")
		os.WriteFile(path, []byte(institutions), 0644)

		db := NewDB()
		db.LoadInstitutions(path)

		// Admin inserts USB with share-1
		usbShare := shares[0]

		// Fetch COBAC share over "network"
		cobacShareBytes, _, err := db.GetShare("emf-002", "node-abc")
		if err != nil {
			t.Fatalf("fetch cobac share: %v", err)
		}
		cobacShare := [2]*big.Int{big.NewInt(2), new(big.Int).SetBytes(cobacShareBytes)}

		// Combine
		recovered, err := shamir.Combine([][2]*big.Int{usbShare, cobacShare})
		if err != nil {
			t.Fatalf("combine: %v", err)
		}
		// Compare via big.Int to avoid leading-zero stripping
		recoveredInt := new(big.Int).SetBytes(recovered)
		expectedInt := new(big.Int).SetBytes(secret)
		if recoveredInt.Cmp(expectedInt) != 0 {
			t.Errorf("recovered secret mismatch: got %x want %x", recovered, secret)
		}
	})
}

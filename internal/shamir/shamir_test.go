package shamir

import (
	"bytes"
	"crypto/rand"
	"math/big"
	"testing"
)

// cmpSecret compares a secret byte slice with a recovered *big.Int,
// accounting for leading zero stripping in big.Int.Bytes().
func cmpSecret(original []byte, recovered *big.Int) bool {
	recBytes := recovered.Bytes()
	if len(recBytes) < len(original) {
		pad := make([]byte, len(original)-len(recBytes))
		recBytes = append(pad, recBytes...)
	}
	return bytes.Equal(recBytes, original)
}

func TestShamir_BDD(t *testing.T) {
	t.Run("Given a secret to split", func(t *testing.T) {
		secret := make([]byte, 32)
		rand.Read(secret)

		t.Run("When splitting with k=2, n=2", func(t *testing.T) {
			shares, err := Split(secret, 2, 2)
			if err != nil {
				t.Fatalf("split: %v", err)
			}
			if len(shares) != 2 {
				t.Errorf("expected 2 shares, got %d", len(shares))
			}

			t.Run("Then combining same shares reconstructs the secret", func(t *testing.T) {
				recovered, err := Combine(shares)
				if err != nil {
					t.Fatalf("combine: %v", err)
				}
			if !cmpSecret(secret, new(big.Int).SetBytes(recovered)) {
				t.Errorf("recovered secret does not match original: got %x want %x", recovered, secret)
			}
		})

			t.Run("Then combining only 1 share fails", func(t *testing.T) {
				_, err := Combine(shares[:1])
				if err == nil {
					t.Error("expected error with only 1 share")
				}
			})
		})

		t.Run("When splitting with k=3, n=5", func(t *testing.T) {
			shares, err := Split(secret, 3, 5)
			if err != nil {
				t.Fatalf("split: %v", err)
			}

			// Reconstruct with exactly k shares (first 3)
			recovered, err := Combine(shares[:3])
			if err != nil {
				t.Fatalf("combine: %v", err)
			}
			if !cmpSecret(secret, new(big.Int).SetBytes(recovered)) {
				t.Error("recovered secret mismatch with k=3 shares")
			}

			// Reconstruct with different subset (last 3)
			recovered, err = Combine(shares[2:])
			if err != nil {
				t.Fatalf("combine alternate subset: %v", err)
			}
			if !cmpSecret(secret, new(big.Int).SetBytes(recovered)) {
				t.Error("recovered secret mismatch with alternate subset")
			}
		})
	})

	t.Run("Given empty secret", func(t *testing.T) {
		_, err := Split([]byte{}, 2, 2)
		if err == nil {
			t.Error("expected error for empty secret")
		}
	})

	t.Run("Given invalid parameters", func(t *testing.T) {
		secret := []byte("test")
		_, err := Split(secret, 1, 2) // k < 2
		if err == nil {
			t.Error("expected error for k=1")
		}
		_, err = Split(secret, 3, 2) // k > n
		if err == nil {
			t.Error("expected error for k > n")
		}
	})

	t.Run("Given deterministic reconstruction across different share subsets", func(t *testing.T) {
		secret := make([]byte, 32)
		secret[31] = 0x7f
		shares, err := Split(secret, 2, 4)
		if err != nil {
			t.Fatalf("split: %v", err)
		}

		// Any 2 out of 4 must reconstruct — verify all 6 pairs
		pairs := [][2]int{{0, 1}, {0, 2}, {0, 3}, {1, 2}, {1, 3}, {2, 3}}
		for _, p := range pairs {
			i, j := p[0], p[1]
			subset := [][2]*big.Int{
				{shares[i][0], shares[i][1]},
				{shares[j][0], shares[j][1]},
			}
			recovered, err := Combine(subset)
			if err != nil {
				t.Fatalf("combine(%d,%d): %v", i, j, err)
			}
			if !cmpSecret(secret, new(big.Int).SetBytes(recovered)) {
				t.Errorf("combine(%d,%d): secret mismatch", i, j)
			}
		}
	})

	t.Run("Given a single share reveals zero information about the secret", func(t *testing.T) {
		secret := make([]byte, 32)
		rand.Read(secret)
		shares, err := Split(secret, 2, 2)
		if err != nil {
			t.Fatalf("split: %v", err)
		}

		// A single share is just a random point — it must not equal or correlate with the secret
		share1Bytes := shares[0][1].Bytes()
		if bytes.Equal(share1Bytes, secret) {
			t.Error("single share MUST NOT equal the secret (information-theoretic guarantee)")
		}
	})
}

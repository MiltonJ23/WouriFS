package keyring

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestKeyring_EncryptDecrypt_RoundTrip(t *testing.T) {
	share1 := make([]byte, 32)
	share2 := make([]byte, 32)
	rand.Read(share1)
	rand.Read(share2)

	var k Keyring
	if err := k.Unseal(share1, share2); err != nil {
		t.Fatalf("unseal: %v", err)
	}

	plain := []byte("ledger entry: deposit 50000 CFA by user a1b2c3")
	for i := 0; i < 10; i++ {
		chunkID := "chunk-" + string(rune('0'+i))
		enc, err := k.Encrypt(plain, chunkID)
		if err != nil {
			t.Fatalf("encrypt %s: %v", chunkID, err)
		}

		// Verify nonce is present (12 bytes)
		if len(enc) < nonceLen {
			t.Fatalf("encrypted output too short: %d bytes", len(enc))
		}

		// Verify ciphertext is different each time (random nonce)
		enc2, _ := k.Encrypt(plain, chunkID)
		if bytes.Equal(enc, enc2) {
			t.Error("encrypted output must differ across calls (random nonce)")
		}

		// Decrypt and verify
		dec, err := k.Decrypt(enc, chunkID)
		if err != nil {
			t.Fatalf("decrypt %s: %v", chunkID, err)
		}
		if !bytes.Equal(dec, plain) {
			t.Errorf("round-trip failed for %s: got %q want %q", chunkID, dec, plain)
		}
	}
}

func TestKeyring_DifferentChunkIDs(t *testing.T) {
	var k Keyring
	k.Unseal([]byte("aaaa0000bbbb0000cccc0000dddd0000"), []byte("eeee0000ffff0000gggg0000hhhh0000"))

	enc1, _ := k.Encrypt([]byte("data"), "chunk-A")
	enc2, _ := k.Encrypt([]byte("data"), "chunk-B")

	// Decrypting with wrong chunk ID must fail
	_, err := k.Decrypt(enc1, "chunk-B")
	if err != ErrDecrypt {
		t.Errorf("expected ErrDecrypt for wrong chunk ID, got %v", err)
	}
	_, err = k.Decrypt(enc2, "chunk-A")
	if err != ErrDecrypt {
		t.Errorf("expected ErrDecrypt for wrong chunk ID, got %v", err)
	}
}

func TestKeyring_SealedState(t *testing.T) {
	var k Keyring

	if !k.IsSealed() {
		t.Error("zero-value keyring must be sealed")
	}

	_, err := k.Encrypt([]byte("data"), "chunk-1")
	if err != ErrSealed {
		t.Errorf("expected ErrSealed before unseal, got %v", err)
	}

	_, err = k.Decrypt([]byte("nonce12b+ciphertext+tag"), "chunk-1")
	if err != ErrSealed {
		t.Errorf("expected ErrSealed before unseal, got %v", err)
	}

	// Unseal
	k.Unseal(make([]byte, 32), make([]byte, 32))
	if k.IsSealed() {
		t.Error("keyring must not be sealed after Unseal")
	}

	// Encrypt should work now
	enc, err := k.Encrypt([]byte("after unseal"), "chunk-2")
	if err != nil {
		t.Fatalf("encrypt after unseal: %v", err)
	}

	// Seal
	k.Seal()
	if !k.IsSealed() {
		t.Error("keyring must be sealed after Seal()")
	}

	// Decrypt on sealed must fail
	_, err = k.Decrypt(enc, "chunk-2")
	if err != ErrSealed {
		t.Errorf("expected ErrSealed after seal, got %v", err)
	}
}

func TestKeyring_CorruptedCiphertext(t *testing.T) {
	var k Keyring
	k.Unseal(make([]byte, 32), make([]byte, 32))

	enc, _ := k.Encrypt([]byte("sensitive data"), "chunk-x")

	// Flip a byte
	flipped := make([]byte, len(enc))
	copy(flipped, enc)
	flipped[nonceLen+1] ^= 0x01

	_, err := k.Decrypt(flipped, "chunk-x")
	if err != ErrDecrypt {
		t.Errorf("expected ErrDecrypt for corrupted ciphertext, got %v", err)
	}
}

func TestKeyring_ShortCiphertext(t *testing.T) {
	var k Keyring
	k.Unseal(make([]byte, 32), make([]byte, 32))

	_, err := k.Decrypt([]byte{0x01}, "chunk-x")
	if err == nil {
		t.Error("expected error for short ciphertext")
	}
}

func TestKeyring_UnsealInvalidShares(t *testing.T) {
	var k Keyring

	err := k.Unseal([]byte{0x01}, make([]byte, 32))
	if err == nil {
		t.Error("expected error for short share (1 byte)")
	}

	err = k.Unseal(make([]byte, 32), []byte{0x01})
	if err == nil {
		t.Error("expected error for short share 2")
	}
}

func TestKeyring_LargePlaintext(t *testing.T) {
	var k Keyring
	k.Unseal(make([]byte, 32), make([]byte, 32))

	// 1 MB plaintext
	plain := make([]byte, 1<<20)
	rand.Read(plain)

	enc, err := k.Encrypt(plain, "large-chunk")
	if err != nil {
		t.Fatalf("encrypt large: %v", err)
	}

	dec, err := k.Decrypt(enc, "large-chunk")
	if err != nil {
		t.Fatalf("decrypt large: %v", err)
	}
	if !bytes.Equal(dec, plain) {
		t.Error("large plaintext round-trip failed")
	}
}

func TestKeyring_ConcurrentAccess(t *testing.T) {
	var k Keyring
	k.Unseal(make([]byte, 32), make([]byte, 32))

	done := make(chan bool, 10)
	for g := 0; g < 10; g++ {
		go func(id int) {
			for i := 0; i < 100; i++ {
				cid := "chunk-concurrent-" + string(rune(id))
				plain := []byte("concurrent test " + string(rune(i)))
				enc, err := k.Encrypt(plain, cid)
				if err != nil {
					t.Errorf("concurrent encrypt: %v", err)
					return
				}
				dec, err := k.Decrypt(enc, cid)
				if err != nil {
					t.Errorf("concurrent decrypt: %v", err)
					return
				}
				if !bytes.Equal(dec, plain) {
					t.Errorf("concurrent round-trip mismatch")
					return
				}
			}
			done <- true
		}(g)
	}
	for g := 0; g < 10; g++ {
		<-done
	}
}

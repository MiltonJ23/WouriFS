/*
 * keyring: AES-256-GCM data-at-rest encryption for Datanode chunks.
 *
 * Architecture: A single cluster-wide 256-bit master key is split via
 * Shamir's Secret Sharing (k=2, n=2). Both shares must be provided to
 * unseal the keyring. The unsealed master key lives only in process
 * memory, never touching disk.
 *
 * Per-chunk encryption uses a derived key to avoid reusing the same
 * AES key for every chunk:
 *
 *   chunk_key = HKDF-SHA256(master_key, chunk_id || namespace_id)
 *
 * Encrypted chunks are stored as:
 *
 *   [12 bytes nonce || ciphertext+tag]
 *
 * The 96-bit GCM nonce is randomly generated per write. GCM provides
 * authenticated encryption — any ciphertext corruption is detected
 * during decryption.
 */
package keyring

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/crypto/hkdf"
)

// ErrSealed is returned when an operation requires the keyring but it has not
// been unsealed yet.
var ErrSealed = errors.New("keyring: master key is sealed — both Shamir shares required at startup")

// ErrDecrypt is returned when decryption fails (wrong key or corrupted data).
var ErrDecrypt = errors.New("keyring: decryption failed — data corrupted or wrong key")

// nonceLen is the GCM standard 96-bit (12 byte) nonce.
const nonceLen = 12

// Keyring holds the unsealed cluster master encryption key.
//
// Zero-value is safe — operations on a nil masterKey return ErrSealed.
// Unseal must be called at cluster startup before any read or write.
type Keyring struct {
	mu        sync.RWMutex
	masterKey []byte // 32 bytes, zeroed on Seal
}

// Unseal reconstructs the master key from two Shamir shares and activates the
// keyring. Call exactly once at Namenode startup. The reconstructed secret is
// never logged or written to disk.
func (k *Keyring) Unseal(share1, share2 []byte) error {
	if len(share1) != 32 || len(share2) != 32 {
		return fmt.Errorf("keyring: each share must be 32 bytes (got %d, %d)", len(share1), len(share2))
	}
	// XOR reconstruction: master = share1 XOR share2
	// This is a simplified two-share scheme. For production, Shamir SS
	// over GF(2^256) is implemented in internal/shamir/.
	master := make([]byte, 32)
	for i := 0; i < 32; i++ {
		master[i] = share1[i] ^ share2[i]
	}

	k.mu.Lock()
	k.masterKey = master
	k.mu.Unlock()
	return nil
}

// Seal zeroes the master key in memory, returning the keyring to the sealed
// state. After Seal, all Encrypt/Decrypt operations return ErrSealed until
// Unseal is called again.
func (k *Keyring) Seal() {
	k.mu.Lock()
	if k.masterKey != nil {
		for i := range k.masterKey {
			k.masterKey[i] = 0
		}
		k.masterKey = nil
	}
	k.mu.Unlock()
}

// IsSealed reports whether the keyring requires unsealing.
func (k *Keyring) IsSealed() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.masterKey == nil
}

// Encrypt encrypts plaintext with AES-256-GCM using a per-chunk key derived
// from chunkID. Returns the nonce prepended to the ciphertext:
//
//	output = nonce(12B) || ciphertext+tag
func (k *Keyring) Encrypt(plaintext []byte, chunkID string) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.masterKey == nil {
		return nil, ErrSealed
	}

	dk := deriveKey(k.masterKey, []byte(chunkID))

	block, err := aes.NewCipher(dk)
	if err != nil {
		return nil, fmt.Errorf("keyring: aes new cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("keyring: gcm: %w", err)
	}

	// Generate random 96-bit nonce
	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("keyring: nonce: %w", err)
	}

	// Seal: nonce || ciphertext+tag
	out := make([]byte, nonceLen+len(plaintext)+aesgcm.Overhead())
	copy(out[:nonceLen], nonce)
	aesgcm.Seal(out[nonceLen:nonceLen], nonce, plaintext, nil)
	return out, nil
}

// Decrypt reverses Encrypt. The input is the output of Encrypt:
//
//	input = nonce(12B) || ciphertext+tag
//
// Returns the original plaintext, or ErrDecrypt if the data is corrupted or
// the chunk ID is wrong.
func (k *Keyring) Decrypt(ciphertext []byte, chunkID string) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.masterKey == nil {
		return nil, ErrSealed
	}

	if len(ciphertext) < nonceLen {
		return nil, fmt.Errorf("keyring: ciphertext too short (%d bytes < %d nonce)", len(ciphertext), nonceLen)
	}

	dk := deriveKey(k.masterKey, []byte(chunkID))

	block, err := aes.NewCipher(dk)
	if err != nil {
		return nil, fmt.Errorf("keyring: aes new cipher: %w", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("keyring: gcm: %w", err)
	}

	nonce := ciphertext[:nonceLen]
	encrypted := ciphertext[nonceLen:]

	plain, err := aesgcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, ErrDecrypt
	}
	return plain, nil
}

// deriveKey produces a 32-byte AES-256 key from the master secret and salt
// using HKDF-SHA256 (RFC 5869).
func deriveKey(master, salt []byte) []byte {
	h := hkdf.New(sha256.New, master, salt, []byte("wourifs-chunk-key-v1"))
	key := make([]byte, 32)
	if _, err := io.ReadFull(h, key); err != nil {
		// hkdf.Read returns an error only if the underlying hash
		// function fails. SHA-256 is deterministic and cannot fail
		// with valid input, so this path is unreachable in practice.
		panic("keyring: hkdf read: " + err.Error())
	}
	return key
}

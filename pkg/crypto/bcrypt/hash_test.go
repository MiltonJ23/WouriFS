package bcrypt

import (
	"testing"
)

func TestBcryptHasher_BDD(t *testing.T) {
	t.Run("Given a work factor lower than 12", func(t *testing.T) {
		_, err := NewBcryptHashser(10)

		if err == nil {
			t.Errorf("Expected an error due to low work factor, got nil")
		}
	})

	t.Run("Given a valid hasher with work factor 12", func(t *testing.T) {
		hasher, err := NewBcryptHashser(12)
		if err != nil {
			t.Fatalf("Failed to create hasher: %v", err)
		}

		password := "SuperSecretPassword123!"
		var hash string

		t.Run("When hashing a valid password", func(t *testing.T) {
			hash, err = hasher.HashPassword(password)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if hash == password || hash == "" {
				t.Errorf("Hash is invalid or matches plaintext")
			}
		})

		t.Run("When verifying with the correct password", func(t *testing.T) {
			err := hasher.CheckPassword(password, hash)
			if err != nil {
				t.Errorf("Expected password to match, got error: %v", err)
			}
		})

		t.Run("When verifying with an incorrect password", func(t *testing.T) {
			err := hasher.CheckPassword("WrongPassword!", hash)
			if err == nil {
				t.Errorf("Expected password check to fail for wrong password, but it succeeded")
			}
		})
	})
}

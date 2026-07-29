package bcrypt

import (
	"errors"
	"fmt"
	"strconv"

	"golang.org/x/crypto/bcrypt"
)

const MinWorkFactor = 12

type BcryptHashser struct {
	cost int
}

func NewBcryptHashser(cost int) (*BcryptHashser, error) {
	if cost < MinWorkFactor {
		return nil, errors.New("security violation: Bcrypt hash cost must be >= " + strconv.Itoa(MinWorkFactor))
	}
	return &BcryptHashser{cost: MinWorkFactor}, nil
}

func (h *BcryptHashser) HashPassword(password string) (string, error) {
	passwordBytes, err := bcrypt.GenerateFromPassword([]byte(password), h.cost)
	if err != nil {
		return "", fmt.Errorf("an error occured while hashing password: %v", err)
	}
	return string(passwordBytes), nil
}

func (h *BcryptHashser) CheckPassword(password, hash string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

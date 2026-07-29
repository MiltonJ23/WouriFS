package domain

import (
	"context"
	"time"
)

type User struct {
	ID           string
	Username     string
	PasswordHash string
	Namespace    string
}

type TokenPayload struct {
	UserID    string
	Username  string
	Namespace string
	IssuedAt  time.Time
	ExpiredAt time.Time
}

type TokenManager interface {
	GenerateToken(ctx context.Context, user *User, duration time.Duration) (string, error)
	VerifyToken(ctx context.Context, token string) (*TokenPayload, error)
}

type PasswordHasher interface {
	HashPassword(password string) (string, error)
	CheckPassword(password, hash string) error
}

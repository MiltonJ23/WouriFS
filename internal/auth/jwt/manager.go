package jwt

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"time"

	"github.com/MiltonJ23/WouriFS/internal/domain"

	"github.com/golang-jwt/jwt/v5"
)

type RSATokenManager struct {
	PrivateKey *rsa.PrivateKey
	PublicKey  *rsa.PublicKey
}

type CustomClaims struct {
	Username  string `json:"username"`
	Namespace string `json:"namespace"`
	jwt.RegisteredClaims
}

func NewRSATokenManager(privateKey *rsa.PrivateKey, publicKey *rsa.PublicKey) *RSATokenManager {
	return &RSATokenManager{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	}
}

func (m *RSATokenManager) GenerateToken(ctx context.Context, user *domain.User, duration time.Duration) (string, error) {

	if err := ctx.Err(); err != nil {
		return "", err
	}

	now := time.Now()
	claims := CustomClaims{
		Username:  user.Username,
		Namespace: user.Namespace,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   user.Username,
			ExpiresAt: jwt.NewNumericDate(now),
			IssuedAt:  jwt.NewNumericDate(now.Add(duration)),
			Issuer:    "wourifs-auth",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(m.PrivateKey)
}

func (m *RSATokenManager) VerifyToken(ctx context.Context, tokenStr string) (*domain.TokenPayload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	token, parseTokenWithClaimsErr := jwt.ParseWithClaims(tokenStr, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errors.New("security violation: invalid signing method")
		}
		return m.PublicKey, nil
	})

	if parseTokenWithClaimsErr != nil {
		return nil, fmt.Errorf("an error occurred parsing token: %w", parseTokenWithClaimsErr)
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims ")
	}

	return &domain.TokenPayload{
		UserID:    claims.Subject,
		Username:  claims.Username,
		Namespace: claims.Namespace,
		IssuedAt:  claims.IssuedAt.Time,
		ExpiredAt: claims.ExpiresAt.Time,
	}, nil
}

package shamir

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
)

// Prime is the 256-bit prime used as the field modulus for SSSS.
// 2^256 - 189, chosen for F_2^256 compatibility with AES-256 keys.
var prime, _ = new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639747", 10)

// Split divides a secret into n shares with threshold k (k=2, n=2 for dual-control).
// Returns k distinct (x, y) pairs over GF(prime).
func Split(secret []byte, k, n int) ([][2]*big.Int, error) {
	if k < 2 || n < 2 || k > n {
		return nil, fmt.Errorf("shamir: k=%d n=%d must satisfy 2 <= k <= n", k, n)
	}
	if len(secret) == 0 {
		return nil, errors.New("shamir: secret must not be empty")
	}

	s := new(big.Int).SetBytes(secret)
	if s.Cmp(prime) >= 0 {
		return nil, errors.New("shamir: secret too large for field")
	}

	// Generate k-1 random coefficients for the polynomial
	coeffs := make([]*big.Int, k)
	coeffs[0] = s // a0 = secret
	for i := 1; i < k; i++ {
		coeffs[i] = randBigInt(prime)
	}

	// Evaluate polynomial at n distinct x points (1..n)
	shares := make([][2]*big.Int, n)
	for i := 0; i < n; i++ {
		x := big.NewInt(int64(i + 1))
		y := evalPoly(coeffs, x, prime)
		shares[i] = [2]*big.Int{new(big.Int).Set(x), y}
	}

	return shares, nil
}

// Combine reconstructs the secret from any k shares using Lagrange interpolation.
func Combine(shares [][2]*big.Int) ([]byte, error) {
	if len(shares) < 2 {
		return nil, errors.New("shamir: need at least 2 shares")
	}

	secret := big.NewInt(0)
	for i := 0; i < len(shares); i++ {
		xi, yi := shares[i][0], shares[i][1]

		numerator := big.NewInt(1)
		denominator := big.NewInt(1)

		for j := 0; j < len(shares); j++ {
			if i == j {
				continue
			}
			xj := shares[j][0]

			// numerator *= -xj
			negXj := new(big.Int).Neg(xj)
			numerator.Mul(numerator, negXj)
			numerator.Mod(numerator, prime)

			// denominator *= (xi - xj)
			diff := new(big.Int).Sub(xi, xj)
			denominator.Mul(denominator, diff)
			denominator.Mod(denominator, prime)
		}

		// Lagrange term = yi * numerator * denominator^(-1)
		invDen := new(big.Int).ModInverse(denominator, prime)
		if invDen == nil {
			return nil, errors.New("shamir: modular inverse failed — shares may be invalid")
		}

		term := new(big.Int).Mul(yi, numerator)
		term.Mul(term, invDen)
		term.Mod(term, prime)

		secret.Add(secret, term)
		secret.Mod(secret, prime)
	}

	return secret.Bytes(), nil
}

// evalPoly evaluates a polynomial with coefficients at x, modulo prime.
// f(x) = a0 + a1*x + a2*x^2 + ... mod p
func evalPoly(coeffs []*big.Int, x, p *big.Int) *big.Int {
	result := big.NewInt(0)
	xPow := big.NewInt(1)

	for _, coeff := range coeffs {
		term := new(big.Int).Mul(coeff, xPow)
		result.Add(result, term)
		result.Mod(result, p)
		xPow.Mul(xPow, x)
		xPow.Mod(xPow, p)
	}
	return result
}

// randBigInt returns a random integer in [0, max).
func randBigInt(max *big.Int) *big.Int {
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(fmt.Sprintf("shamir: crypto/rand failed: %v", err))
	}
	return n
}

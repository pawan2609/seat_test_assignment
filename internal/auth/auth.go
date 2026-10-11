// Package auth issues and verifies stateless bearer tokens.
//
// A token is "<user_id>.<base64url(HMAC-SHA256(secret, user_id))>". The server
// picks the user id, so a client can never mint a token for someone else's
// id, and verification needs no DB round-trip on the hot path.
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

type Signer struct {
	secret []byte
}

func NewSigner(secret string) *Signer {
	return &Signer{secret: []byte(secret)}
}

// NewUserID returns a fresh random user id.
func NewUserID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return "u_" + hex.EncodeToString(b)
}

func (s *Signer) Issue(userID string) string {
	return userID + "." + s.sign(userID)
}

// Verify returns the user id carried by a valid token.
func (s *Signer) Verify(token string) (string, bool) {
	userID, sig, ok := strings.Cut(token, ".")
	if !ok || userID == "" {
		return "", false
	}
	want := s.sign(userID)
	if subtle.ConstantTimeCompare([]byte(sig), []byte(want)) != 1 {
		return "", false
	}
	return userID, true
}

// AdminOK compares an admin token in constant time.
func AdminOK(got, want string) bool {
	return want != "" && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *Signer) sign(userID string) string {
	m := hmac.New(sha256.New, s.secret)
	m.Write([]byte(userID))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

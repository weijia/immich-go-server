package crypto

import (
	"crypto/rand"
	"testing"
)

func TestSignAndVerify(t *testing.T) {
	secret := "cluster-secret"
	body := []byte(`{"x":1}`)
	nonce := make([]byte, 16)
	rand.Read(nonce)
	ts := int64(1000)
	sig := SignRequest(secret, "POST", "/api/cluster/task", ts, nonce, body)
	seen := map[string]bool{}
	if !VerifyRequest(secret, "POST", "/api/cluster/task", ts, nonce, body, sig, 1000, 300, seen) {
		t.Fatal("valid request should verify")
	}
}

func TestVerifyRejectsTamper(t *testing.T) {
	secret := "s"
	body := []byte("a")
	nonce := []byte("nonce1234567890")
	sig := SignRequest(secret, "POST", "/p", 1000, nonce, body)
	seen := map[string]bool{}
	if VerifyRequest(secret, "POST", "/p", 1000, nonce, []byte("b"), sig, 1000, 300, seen) {
		t.Fatal("tampered body should fail")
	}
}

func TestVerifyRejectsReplay(t *testing.T) {
	secret := "s"
	body := []byte("a")
	nonce := []byte("nonce1234567890")
	sig := SignRequest(secret, "POST", "/p", 1000, nonce, body)
	seen := map[string]bool{}
	if !VerifyRequest(secret, "POST", "/p", 1000, nonce, body, sig, 1000, 300, seen) {
		t.Fatal("first verify should pass")
	}
	if VerifyRequest(secret, "POST", "/p", 1000, nonce, body, sig, 1000, 300, seen) {
		t.Fatal("replay should fail")
	}
}

func TestVerifyRejectsSkew(t *testing.T) {
	secret := "s"
	body := []byte("a")
	nonce := []byte("nonce1234567890")
	sig := SignRequest(secret, "POST", "/p", 1000, nonce, body)
	seen := map[string]bool{}
	if VerifyRequest(secret, "POST", "/p", 1000, nonce, body, sig, 2000, 300, seen) {
		t.Fatal("clock skew > 300 should fail")
	}
}

func TestDeterministic(t *testing.T) {
	a := SignRequest("s", "GET", "/x", 1, []byte("n"), []byte("b"))
	b := SignRequest("s", "GET", "/x", 1, []byte("n"), []byte("b"))
	if a != b {
		t.Fatal("signing should be deterministic")
	}
}

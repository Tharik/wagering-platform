package httpapi

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testJWTIssuer = "https://issuer.example.test/realms/wagering"
	testJWTKeyID  = "auth-test-key"
)

type testJWTClaims struct {
	Issuer          string   `json:"iss"`
	Audience        []string `json:"aud"`
	Expiry          int64    `json:"exp"`
	NotBefore       int64    `json:"nbf,omitempty"`
	IssuedAt        int64    `json:"iat"`
	AuthorizedParty string   `json:"azp"`
}

func TestAuthMiddlewareValidatesOIDCEdgeCases(t *testing.T) {
	trustedKey := generateRSAKey(t)
	untrustedKey := generateRSAKey(t)
	jwksServer := newJWKSServer(t, &trustedKey.PublicKey, testJWTKeyID)

	auth, err := NewAuthMiddlewareWithJWKSURL(
		context.Background(),
		testJWTIssuer,
		jwksServer.URL,
	)
	if err != nil {
		t.Fatalf("create auth middleware: %v", err)
	}

	now := time.Now().UTC()
	validClaims := testJWTClaims{
		Issuer:          testJWTIssuer,
		Audience:        []string{wageringAPIAudience},
		Expiry:          now.Add(30 * time.Minute).Unix(),
		IssuedAt:        now.Add(-time.Minute).Unix(),
		AuthorizedParty: "provider-a",
	}
	validToken := signTestJWT(t, trustedKey, testJWTKeyID, validClaims)

	for _, scheme := range []string{"Bearer", "bearer", "BEARER"} {
		t.Run("valid "+scheme+" scheme sets authenticated principal", func(t *testing.T) {
			called := false
			handler := auth.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				principal, ok := PrincipalFromContext(r.Context())
				if !ok {
					t.Fatal("authenticated request has no principal")
				}
				if principal.ClientID != "provider-a" {
					t.Fatalf("expected provider-a principal, got %q", principal.ClientID)
				}
				w.WriteHeader(http.StatusNoContent)
			}))

			response := executeAuthRequest(handler, scheme+" "+validToken)
			if response.Code != http.StatusNoContent {
				t.Fatalf("expected 204, got %d: %s", response.Code, response.Body.String())
			}
			if !called {
				t.Fatal("valid token did not reach downstream handler")
			}
		})
	}

	expired := validClaims
	expired.Expiry = now.Add(-10 * time.Minute).Unix()
	wrongIssuer := validClaims
	wrongIssuer.Issuer = "https://different-issuer.example.test/realms/wagering"
	wrongAudience := validClaims
	wrongAudience.Audience = []string{"some-other-api"}
	futureNotBefore := validClaims
	futureNotBefore.NotBefore = now.Add(10 * time.Minute).Unix()

	tests := []struct {
		name   string
		header string
	}{
		{"expired token", "Bearer " + signTestJWT(t, trustedKey, testJWTKeyID, expired)},
		{"wrong issuer", "Bearer " + signTestJWT(t, trustedKey, testJWTKeyID, wrongIssuer)},
		{"wrong audience", "Bearer " + signTestJWT(t, trustedKey, testJWTKeyID, wrongAudience)},
		{"invalid signature", "Bearer " + signTestJWT(t, untrustedKey, testJWTKeyID, validClaims)},
		{"future not before", "Bearer " + signTestJWT(t, trustedKey, testJWTKeyID, futureNotBefore)},
		{"missing authorization", ""},
		{"bare bearer scheme", "Bearer"},
		{"bearer with empty credentials", "Bearer "},
		{"basic scheme", "Basic abc"},
		{"malformed bearer JWT", "Bearer not-a-jwt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := auth.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			}))

			response := executeAuthRequest(handler, tt.header)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d: %s", response.Code, response.Body.String())
			}
			if response.Body.String() != "{\"error\":\"unauthorized\"}\n" {
				t.Fatalf("unexpected unauthorized response: %q", response.Body.String())
			}
			if called {
				t.Fatal("invalid authentication reached downstream handler")
			}
		})
	}
}

func generateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func newJWKSServer(t *testing.T, key *rsa.PublicKey, keyID string) *httptest.Server {
	t.Helper()
	exponent := big.NewInt(int64(key.E)).Bytes()
	payload, err := json.Marshal(map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			"kid": keyID,
			"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(exponent),
		}},
	})
	if err != nil {
		t.Fatalf("marshal JWKS: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)
	return server
}

func signTestJWT(t *testing.T, key *rsa.PrivateKey, keyID string, claims testJWTClaims) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{
		"alg": "RS256",
		"kid": keyID,
		"typ": "JWT",
	})
	if err != nil {
		t.Fatalf("marshal JWT header: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal JWT claims: %v", err)
	}

	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signingInput := encodedHeader + "." + encodedPayload
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign JWT: %v", err)
	}

	return strings.Join([]string{
		encodedHeader,
		encodedPayload,
		base64.RawURLEncoding.EncodeToString(signature),
	}, ".")
}

func executeAuthRequest(handler http.Handler, authorization string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

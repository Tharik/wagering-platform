package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

const (
	wageringAPIAudience = "wagering-api"
	internalClientID    = "wagering-internal"
)

var allowedProviderClients = map[string]struct{}{
	"provider-a": {},
	"provider-b": {},
}

type principalContextKey struct{}

type Principal struct {
	ClientID string
}

type AuthMiddleware struct {
	verifier *oidc.IDTokenVerifier
}

func NewAuthMiddleware(
	ctx context.Context,
	issuer string,
) (*AuthMiddleware, error) {
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, err
	}

	verifier := provider.Verifier(
		&oidc.Config{
			ClientID: wageringAPIAudience,
		},
	)

	return &AuthMiddleware{
		verifier: verifier,
	}, nil
}

func NewAuthMiddlewareWithJWKSURL(
	ctx context.Context,
	issuer string,
	jwksURL string,
) (*AuthMiddleware, error) {
	keySet := oidc.NewRemoteKeySet(ctx, jwksURL)
	verifier := oidc.NewVerifier(
		issuer,
		keySet,
		&oidc.Config{
			ClientID: wageringAPIAudience,
		},
	)

	return &AuthMiddleware{
		verifier: verifier,
	}, nil
}

func (m *AuthMiddleware) Authenticate(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			token, err := bearerToken(r)
			if err != nil {
				writeJSON(
					w,
					http.StatusUnauthorized,
					map[string]string{
						"error": "unauthorized",
					},
				)
				return
			}

			idToken, err := m.verifier.Verify(
				r.Context(),
				token,
			)
			if err != nil {
				writeJSON(
					w,
					http.StatusUnauthorized,
					map[string]string{
						"error": "unauthorized",
					},
				)
				return
			}

			var claims struct {
				AuthorizedParty string `json:"azp"`
				ClientID        string `json:"client_id"`
			}

			if err := idToken.Claims(&claims); err != nil {
				writeJSON(
					w,
					http.StatusUnauthorized,
					map[string]string{
						"error": "unauthorized",
					},
				)
				return
			}

			clientID := claims.AuthorizedParty
			if clientID == "" {
				clientID = claims.ClientID
			}

			if !isAllowedClient(clientID) {
				writeJSON(
					w,
					http.StatusForbidden,
					map[string]string{
						"error": "forbidden",
					},
				)
				return
			}

			ctx := context.WithValue(
				r.Context(),
				principalContextKey{},
				Principal{
					ClientID: clientID,
				},
			)

			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		},
	)
}

func isAllowedClient(clientID string) bool {
	if clientID == internalClientID {
		return true
	}

	_, ok := allowedProviderClients[clientID]
	return ok
}

func PrincipalFromContext(
	ctx context.Context,
) (Principal, bool) {
	principal, ok := ctx.Value(
		principalContextKey{},
	).(Principal)

	return principal, ok
}

func bearerToken(
	r *http.Request,
) (string, error) {
	header := r.Header.Get("Authorization")
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", errors.New("missing bearer token")
	}

	token := strings.TrimSpace(credential)

	if token == "" {
		return "", errors.New("missing bearer token")
	}

	return token, nil
}

func (m *AuthMiddleware) ProviderOnly(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			principal, ok := PrincipalFromContext(r.Context())
			if !ok {
				writeJSON(
					w,
					http.StatusUnauthorized,
					map[string]string{
						"error": "unauthorized",
					},
				)
				return
			}

			if _, ok := allowedProviderClients[principal.ClientID]; !ok {
				writeJSON(
					w,
					http.StatusForbidden,
					map[string]string{
						"error": "forbidden",
					},
				)
				return
			}

			next.ServeHTTP(w, r)
		},
	)
}

func (m *AuthMiddleware) InternalOnly(
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			principal, ok := PrincipalFromContext(r.Context())
			if !ok {
				writeJSON(
					w,
					http.StatusUnauthorized,
					map[string]string{
						"error": "unauthorized",
					},
				)
				return
			}

			if principal.ClientID != internalClientID {
				writeJSON(
					w,
					http.StatusForbidden,
					map[string]string{
						"error": "forbidden",
					},
				)
				return
			}

			next.ServeHTTP(w, r)
		},
	)
}

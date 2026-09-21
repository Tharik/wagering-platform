package httpapi

import (
	"context"
	"errors"
	"log"
	"net/http"
)

type Server struct {
	server *http.Server
}

func NewServer(
	walletHandler *WalletHandler,
	wagerHandler *WagerHandler,
	healthHandler *HealthHandler,
	auth *AuthMiddleware,
) *Server {
	mux := http.NewServeMux()

	// Internal wallet operations.
	mux.Handle(
		"POST /wallets",
		auth.Authenticate(
			auth.InternalOnly(
				http.HandlerFunc(walletHandler.Create),
			),
		),
	)

	mux.Handle(
		"GET /wallets/{id}",
		auth.Authenticate(
			auth.InternalOnly(
				http.HandlerFunc(walletHandler.Get),
			),
		),
	)

	mux.Handle(
		"GET /wallets/{id}/ledger",
		auth.Authenticate(
			auth.InternalOnly(
				http.HandlerFunc(walletHandler.Ledger),
			),
		),
	)

	mux.Handle(
		"GET /wallets/{id}/reconciliation",
		auth.Authenticate(
			auth.InternalOnly(
				http.HandlerFunc(walletHandler.Reconcile),
			),
		),
	)

	// Provider operations.
	mux.Handle(
		"POST /wagers",
		auth.Authenticate(
			auth.ProviderOnly(
				http.HandlerFunc(wagerHandler.Process),
			),
		),
	)

	mux.Handle(
		"GET /wagers/{id}",
		auth.Authenticate(
			auth.ProviderOnly(
				http.HandlerFunc(wagerHandler.Get),
			),
		),
	)

	// Health endpoints intentionally remain unauthenticated.
	mux.HandleFunc(
		"GET /health/live",
		healthHandler.Live,
	)

	mux.HandleFunc(
		"GET /health/ready",
		healthHandler.Ready,
	)

	return &Server{
		server: &http.Server{
			Addr:    ":8080",
			Handler: mux,
		},
	}
}

func (s *Server) Start() {
	go func() {
		log.Printf(
			"HTTP server listening on %s",
			s.server.Addr,
		)

		if err := s.server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf(
				"HTTP server error: %v",
				err,
			)
		}
	}()
}

func (s *Server) Shutdown(
	ctx context.Context,
) error {
	return s.server.Shutdown(ctx)
}

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
) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc(
		"POST /wallets",
		walletHandler.Create,
	)

	mux.HandleFunc(
		"GET /wallets/{id}",
		walletHandler.Get,
	)

	mux.HandleFunc(
		"GET /wallets/{id}/ledger",
		walletHandler.Ledger,
	)

	mux.HandleFunc(
		"GET /wallets/{id}/reconciliation",
		walletHandler.Reconcile,
	)

	mux.HandleFunc(
		"POST /wagers",
		wagerHandler.Process,
	)

	mux.HandleFunc(
		"GET /wagers/{id}",
		wagerHandler.Get,
	)

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

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
		"POST /wagers",
		wagerHandler.Process,
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

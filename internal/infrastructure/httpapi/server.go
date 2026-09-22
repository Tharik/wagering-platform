package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
)

type Server struct {
	server  *http.Server
	handler http.Handler
	logger  *slog.Logger
}

func NewServer(
	walletHandler *WalletHandler,
	wagerHandler *WagerHandler,
	healthHandler *HealthHandler,
	metricsHandler *MetricsHandler,
	auth *AuthMiddleware,
	logger *slog.Logger,
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
		"POST /wallets/{id}/reconciliation",
		auth.Authenticate(
			auth.InternalOnly(
				http.HandlerFunc(walletHandler.Reconcile),
			),
		),
	)

	// Provider wagering operations.
	mux.Handle(
		"POST /wagering/transactions",
		auth.Authenticate(
			auth.ProviderOnly(
				http.HandlerFunc(wagerHandler.Process),
			),
		),
	)

	mux.Handle(
		"GET /wagering/transactions/{id}",
		auth.Authenticate(
			auth.ProviderOnly(
				http.HandlerFunc(wagerHandler.Get),
			),
		),
	)

	mux.Handle(
		"GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
		auth.Authenticate(
			auth.ProviderOnly(
				http.HandlerFunc(wagerHandler.GetByExternalTransactionID),
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

	mux.Handle("GET /metrics", metricsHandler)

	return &Server{
		handler: mux,
		logger: logger.With(
			slog.String("component", "http_server"),
		),
		server: &http.Server{
			Addr:    ":8080",
			Handler: mux,
		},
	}
}

func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) Start() {
	go func() {
		s.logger.Info(
			"server started",
			slog.String("address", s.server.Addr),
		)

		if err := s.server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			s.logger.Error(
				"server failed",
				slog.Any("error", err),
			)
		}
	}()
}

func (s *Server) Shutdown(
	ctx context.Context,
) error {
	return s.server.Shutdown(ctx)
}

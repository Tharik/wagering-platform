package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/domain"
)

type WagerHandler struct {
	service *wagering.Service
}

func NewWagerHandler(
	service *wagering.Service,
) *WagerHandler {
	return &WagerHandler{
		service: service,
	}
}

type processWagerRequest struct {
	IdempotencyKey                 string `json:"idempotencyKey"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Amount                         string `json:"amount"`
	Currency                       string `json:"currency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

type processWagerResponse struct {
	TransactionID    string `json:"transactionId"`
	State            string `json:"state"`
	Balance          string `json:"balance"`
	Currency         string `json:"currency"`
	IdempotentReplay bool   `json:"idempotentReplay"`
	FailureCode      string `json:"failureCode,omitempty"`
}

type wagerResponse struct {
	TransactionID                  string  `json:"transactionId"`
	ProviderID                     string  `json:"providerId,omitempty"`
	ExternalTransactionID          string  `json:"externalTransactionId,omitempty"`
	IdempotencyKey                 string  `json:"idempotencyKey,omitempty"`
	WalletID                       string  `json:"walletId"`
	PlayerID                       string  `json:"playerId"`
	RoundID                        string  `json:"roundId,omitempty"`
	GameID                         string  `json:"gameId,omitempty"`
	Kind                           string  `json:"kind"`
	State                          string  `json:"state"`
	Amount                         string  `json:"amount"`
	Currency                       string  `json:"currency"`
	ReferenceExternalTransactionID string  `json:"referenceExternalTransactionId,omitempty"`
	ReferencedTransactionID        string  `json:"referencedTransactionId,omitempty"`
	FailureCode                    string  `json:"failureCode,omitempty"`
	ResultBalance                  *string `json:"resultBalance,omitempty"`
	CreatedAt                      string  `json:"createdAt"`
	UpdatedAt                      string  `json:"updatedAt"`
}

func (h *WagerHandler) Process(
	w http.ResponseWriter,
	r *http.Request,
) {
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

	var request processWagerRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "invalid request body",
			},
		)
		return
	}

	if request.IdempotencyKey == "" ||
		request.ExternalTransactionID == "" ||
		request.PlayerID == "" ||
		request.WalletID == "" ||
		request.RoundID == "" ||
		request.GameID == "" ||
		request.Kind == "" ||
		request.Amount == "" ||
		request.Currency == "" {

		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "missing required field",
			},
		)
		return
	}

	currency := domain.Currency(request.Currency)

	if currency != domain.BRL {
		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "unsupported currency",
			},
		)
		return
	}

	amount, err := domain.ParseMoney(
		request.Amount,
		currency,
	)
	if err != nil {
		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "invalid amount",
			},
		)
		return
	}

	command := wagering.ProcessCommand{
		IdempotencyKey: request.IdempotencyKey,
		Request: domain.WagerRequest{
			ProviderID:                     principal.ClientID,
			ExternalTransactionID:          request.ExternalTransactionID,
			PlayerID:                       request.PlayerID,
			WalletID:                       request.WalletID,
			RoundID:                        request.RoundID,
			GameID:                         request.GameID,
			Kind:                           domain.WagerKind(request.Kind),
			Amount:                         amount,
			ReferenceExternalTransactionID: request.ReferenceExternalTransactionID,
		},
	}

	result, err := h.service.Process(
		r.Context(),
		command,
	)
	if err != nil {
		writeWagerError(w, err)
		return
	}

	status := http.StatusOK

	if !result.IdempotentReplay {
		status = http.StatusCreated
	}

	writeJSON(
		w,
		status,
		processWagerResponse{
			TransactionID:    result.TransactionID,
			State:            string(result.State),
			Balance:          result.Balance.String(),
			Currency:         string(result.Balance.Currency()),
			IdempotentReplay: result.IdempotentReplay,
			FailureCode:      result.FailureCode,
		},
	)
}

func (h *WagerHandler) Get(
	w http.ResponseWriter,
	r *http.Request,
) {
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

	result, err := h.service.GetForProvider(
		r.Context(),
		r.PathValue("id"),
		principal.ClientID,
	)
	if err != nil {
		if errors.Is(err, wagering.ErrWagerNotFound) {
			writeJSON(
				w,
				http.StatusNotFound,
				map[string]string{
					"error": "wager transaction not found",
				},
			)
			return
		}

		writeJSON(
			w,
			http.StatusInternalServerError,
			map[string]string{
				"error": "failed to query wager transaction",
			},
		)
		return
	}

	amount := domain.NewMoney(
		result.Amount,
		domain.Currency(result.Currency),
	)

	var resultBalance *string

	if result.ResultBalance != nil {
		balance := domain.NewMoney(
			*result.ResultBalance,
			domain.Currency(result.Currency),
		).String()

		resultBalance = &balance
	}

	writeJSON(
		w,
		http.StatusOK,
		wagerResponse{
			TransactionID:                  result.TransactionID,
			ProviderID:                     result.ProviderID,
			ExternalTransactionID:          result.ExternalTransactionID,
			IdempotencyKey:                 result.IdempotencyKey,
			WalletID:                       result.WalletID,
			PlayerID:                       result.PlayerID,
			RoundID:                        result.RoundID,
			GameID:                         result.GameID,
			Kind:                           result.Kind,
			State:                          result.State,
			Amount:                         amount.String(),
			Currency:                       result.Currency,
			ReferenceExternalTransactionID: result.ReferenceExternalTransactionID,
			ReferencedTransactionID:        result.ReferencedTransactionID,
			FailureCode:                    result.FailureCode,
			ResultBalance:                  resultBalance,
			CreatedAt:                      result.CreatedAt.UTC().Format(time.RFC3339Nano),
			UpdatedAt:                      result.UpdatedAt.UTC().Format(time.RFC3339Nano),
		},
	)
}

func writeWagerError(
	w http.ResponseWriter,
	err error,
) {
	switch {
	case errors.Is(err, wagering.ErrIdempotencyConflict):
		writeJSON(
			w,
			http.StatusConflict,
			map[string]string{
				"error": "idempotency conflict",
			},
		)

	case errors.Is(err, wagering.ErrExternalTransactionExists):
		writeJSON(
			w,
			http.StatusConflict,
			map[string]string{
				"error": "external transaction already exists",
			},
		)

	case errors.Is(err, wagering.ErrWalletNotFound):
		writeJSON(
			w,
			http.StatusNotFound,
			map[string]string{
				"error": "wallet not found",
			},
		)

	case errors.Is(err, wagering.ErrWalletPlayerMismatch):
		writeJSON(
			w,
			http.StatusForbidden,
			map[string]string{
				"error": "wallet does not belong to player",
			},
		)

	case errors.Is(err, wagering.ErrInvalidLossAmount),
		errors.Is(err, domain.ErrInvalidWagerKind),
		errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrCurrencyMismatch):

		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": err.Error(),
			},
		)

	default:
		writeJSON(
			w,
			http.StatusInternalServerError,
			map[string]string{
				"error": "failed to process wager",
			},
		)
	}
}

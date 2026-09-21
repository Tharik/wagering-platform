package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

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
	ProviderID                     string `json:"providerId"`
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

func (h *WagerHandler) Process(
	w http.ResponseWriter,
	r *http.Request,
) {
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
		request.ProviderID == "" ||
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
			ProviderID:                     request.ProviderID,
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

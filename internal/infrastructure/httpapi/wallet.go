package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
)

type WalletHandler struct {
	service *wallet.Service
}

func NewWalletHandler(
	service *wallet.Service,
) *WalletHandler {
	return &WalletHandler{
		service: service,
	}
}

type createWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID       string   `json:"id"`
	PlayerID string   `json:"playerId"`
	Balance  moneyDTO `json:"balance"`
	Version  int64    `json:"version"`
}

type ledgerEntryResponse struct {
	ID            string `json:"id"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Amount        string `json:"amount"`
	BalanceBefore string `json:"balanceBefore"`
	BalanceAfter  string `json:"balanceAfter"`
	CreatedAt     string `json:"createdAt"`
}

type ledgerResponse struct {
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

type reconciliationResponse struct {
	WalletID          string   `json:"walletId"`
	StoredBalance     moneyDTO `json:"storedBalance"`
	CalculatedBalance moneyDTO `json:"calculatedBalance"`
	Difference        moneyDTO `json:"difference"`
	Consistent        bool     `json:"consistent"`
	CheckedEntries    int      `json:"checkedEntries"`
}

func (h *WalletHandler) Create(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request createWalletRequest

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

	if request.PlayerID == "" {
		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "playerId is required",
			},
		)
		return
	}

	currency := domain.Currency(request.InitialBalance.Currency)

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

	initialBalance, err := domain.ParseMoney(
		request.InitialBalance.Amount,
		currency,
	)
	if err != nil {
		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "invalid initial balance",
			},
		)
		return
	}

	result, err := h.service.Create(
		r.Context(),
		wallet.CreateWalletCommand{
			PlayerID:       request.PlayerID,
			InitialBalance: initialBalance,
		},
	)
	if errors.Is(err, wallet.ErrWalletAlreadyExists) {
		writeJSON(
			w,
			http.StatusConflict,
			map[string]string{
				"error": "wallet already exists",
			},
		)
		return
	}

	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			map[string]string{
				"error": "failed to create wallet",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusCreated,
		walletResponse{
			ID:       result.WalletID,
			PlayerID: request.PlayerID,
			Balance: moneyDTO{
				Amount:   result.Balance.String(),
				Currency: string(result.Balance.Currency()),
			},
			Version: result.Version,
		},
	)
}

func (h *WalletHandler) Get(
	w http.ResponseWriter,
	r *http.Request,
) {
	result, err := h.service.Get(
		r.Context(),
		r.PathValue("id"),
	)

	if errors.Is(err, wallet.ErrWalletNotFound) {
		writeJSON(
			w,
			http.StatusNotFound,
			map[string]string{
				"error": "wallet not found",
			},
		)
		return
	}

	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			map[string]string{
				"error": "failed to get wallet",
			},
		)
		return
	}

	money := domain.NewMoney(
		result.Balance,
		domain.Currency(result.Currency),
	)

	writeJSON(
		w,
		http.StatusOK,
		walletResponse{
			ID:       result.WalletID,
			PlayerID: result.PlayerID,
			Balance: moneyDTO{
				Amount:   money.String(),
				Currency: result.Currency,
			},
			Version: result.Version,
		},
	)
}

func (h *WalletHandler) Ledger(
	w http.ResponseWriter,
	r *http.Request,
) {
	limit := wallet.DefaultLedgerPageSize

	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit <= 0 {
			writeJSON(
				w,
				http.StatusBadRequest,
				map[string]string{
					"error": "invalid limit",
				},
			)
			return
		}

		limit = parsedLimit
	}

	page, err := h.service.LedgerPage(
		r.Context(),
		r.PathValue("id"),
		limit,
		r.URL.Query().Get("cursor"),
	)

	if errors.Is(err, wallet.ErrWalletNotFound) {
		writeJSON(
			w,
			http.StatusNotFound,
			map[string]string{
				"error": "wallet not found",
			},
		)
		return
	}

	if errors.Is(err, wallet.ErrInvalidLedgerCursor) {
		writeJSON(
			w,
			http.StatusBadRequest,
			map[string]string{
				"error": "invalid cursor",
			},
		)
		return
	}

	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			map[string]string{
				"error": "failed to get wallet ledger",
			},
		)
		return
	}

	responseEntries := make(
		[]ledgerEntryResponse,
		0,
		len(page.Entries),
	)

	for _, entry := range page.Entries {
		responseEntries = append(
			responseEntries,
			ledgerEntryResponse{
				ID:            entry.ID,
				TransactionID: entry.TransactionID,
				Direction:     entry.Direction,
				Amount: domain.NewMoney(
					entry.Amount,
					domain.BRL,
				).String(),
				BalanceBefore: domain.NewMoney(
					entry.BalanceBefore,
					domain.BRL,
				).String(),
				BalanceAfter: domain.NewMoney(
					entry.BalanceAfter,
					domain.BRL,
				).String(),
				CreatedAt: entry.CreatedAt.UTC().Format(
					"2006-01-02T15:04:05.999999999Z07:00",
				),
			},
		)
	}

	writeJSON(
		w,
		http.StatusOK,
		ledgerResponse{
			Entries:    responseEntries,
			NextCursor: page.NextCursor,
		},
	)
}

func (h *WalletHandler) Reconcile(
	w http.ResponseWriter,
	r *http.Request,
) {
	result, err := h.service.Reconcile(
		r.Context(),
		r.PathValue("id"),
	)

	if errors.Is(err, wallet.ErrWalletNotFound) {
		writeJSON(
			w,
			http.StatusNotFound,
			map[string]string{
				"error": "wallet not found",
			},
		)
		return
	}

	if err != nil {
		writeJSON(
			w,
			http.StatusInternalServerError,
			map[string]string{
				"error": "failed to reconcile wallet",
			},
		)
		return
	}

	currency := domain.Currency(result.Currency)

	writeJSON(
		w,
		http.StatusOK,
		reconciliationResponse{
			WalletID: result.WalletID,
			StoredBalance: moneyDTO{
				Amount: domain.NewMoney(
					result.StoredBalance,
					currency,
				).String(),
				Currency: result.Currency,
			},
			CalculatedBalance: moneyDTO{
				Amount: domain.NewMoney(
					result.CalculatedBalance,
					currency,
				).String(),
				Currency: result.Currency,
			},
			Difference: moneyDTO{
				Amount: domain.NewMoney(
					result.Difference,
					currency,
				).String(),
				Currency: result.Currency,
			},
			Consistent:     result.Consistent,
			CheckedEntries: result.CheckedEntries,
		},
	)
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}

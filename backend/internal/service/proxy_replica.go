package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

// These reserved extra fields are persisted independently of Spark's parent and
// quota dimension. They must survive scheduler projection and cannot be edited
// through ordinary account configuration endpoints.
const CredentialOwnerAccountIDKey = "credential_owner_account_id"
const ProxyReplicaSourceKey = "proxy_replica_source"
const ProxyReplicaOperationKey = "proxy_replica_operation_id"

type proxyOwnerRefreshContextKey struct{}

type ProxyReplicaInput struct {
	ProxyID     int64 `json:"proxy_id"`
	Concurrency int   `json:"concurrency"`
}

type ProxyReplicasInput struct {
	Proxies           []ProxyReplicaInput `json:"proxies"`
	WSMode            string              `json:"ws_mode"`
	ActivateOnSuccess bool                `json:"activate_on_success"`
}

type ProxyReplicaResult struct {
	AccountID                int64   `json:"account_id"`
	CredentialOwnerAccountID int64   `json:"credential_owner_account_id"`
	ProxyID                  int64   `json:"proxy_id"`
	Concurrency              int     `json:"concurrency"`
	WSMode                   string  `json:"ws_mode"`
	GroupIDs                 []int64 `json:"group_ids"`
	Schedulable              bool    `json:"schedulable"`
	Status                   string  `json:"status"`
	Error                    string  `json:"error,omitempty"`
}

// This DTO deliberately has no credentials, extra, or upstream error body.
type ProxyReplicaOperation struct {
	OperationID     string               `json:"operation_id"`
	SourceAccountID int64                `json:"source_account_id"`
	Status          string               `json:"status"`
	Replicas        []ProxyReplicaResult `json:"replicas"`
	Error           string               `json:"error,omitempty"`
}

type ProxyReplicaRepository interface {
	CreateProxyReplicas(context.Context, int64, string, ProxyReplicasInput, string) (*ProxyReplicaOperation, error)
	GetProxyReplicaOperation(context.Context, int64, string) (*ProxyReplicaOperation, error)
}

type ProxyReplicaAdminService interface {
	CreateProxyReplicas(context.Context, int64, string, ProxyReplicasInput) (*ProxyReplicaOperation, error)
	GetProxyReplicaOperation(context.Context, int64, string) (*ProxyReplicaOperation, error)
}

func NormalizeProxyReplicasInput(in ProxyReplicasInput) (ProxyReplicasInput, string, error) {
	if len(in.Proxies) == 0 || len(in.Proxies) > 100 {
		return in, "", infraerrors.BadRequest("INVALID_PROXY_REPLICAS", "proxies must contain between 1 and 100 entries")
	}
	in.Proxies = append([]ProxyReplicaInput(nil), in.Proxies...)
	sort.Slice(in.Proxies, func(i, j int) bool { return in.Proxies[i].ProxyID < in.Proxies[j].ProxyID })
	for i, p := range in.Proxies {
		if p.ProxyID <= 0 || p.Concurrency <= 0 || p.Concurrency > 10000 || (i > 0 && p.ProxyID == in.Proxies[i-1].ProxyID) {
			return in, "", infraerrors.BadRequest("INVALID_PROXY_REPLICAS", "proxy IDs must be positive and unique; concurrency must be between 1 and 10000")
		}
	}
	switch in.WSMode {
	case OpenAIWSIngressModeOff, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough, OpenAIWSIngressModeHTTPBridge:
	default:
		return in, "", infraerrors.BadRequest("INVALID_WS_MODE", "unsupported ws_mode")
	}
	b, _ := json.Marshal(in)
	sum := sha256.Sum256(b)
	return in, hex.EncodeToString(sum[:]), nil
}

func ValidateProxyReplicaOperationKey(key string) error {
	if key == "" || len(key) > 200 || strings.TrimSpace(key) != key || strings.ContainsAny(key, "/\\\r\n\t") {
		return infraerrors.BadRequest("INVALID_IDEMPOTENCY_KEY", "a non-empty Idempotency-Key of at most 200 characters is required")
	}
	return nil
}

func (s *adminServiceImpl) CreateProxyReplicas(ctx context.Context, id int64, key string, in ProxyReplicasInput) (*ProxyReplicaOperation, error) {
	if err := ValidateProxyReplicaOperationKey(key); err != nil {
		return nil, err
	}
	in, hash, err := NormalizeProxyReplicasInput(in)
	if err != nil {
		return nil, err
	}
	repo, ok := s.accountRepo.(ProxyReplicaRepository)
	if !ok {
		return nil, infraerrors.New(503, "PROXY_REPLICAS_UNAVAILABLE", "proxy replica repository unavailable")
	}
	return repo.CreateProxyReplicas(ctx, id, key, in, hash)
}

func (s *adminServiceImpl) GetProxyReplicaOperation(ctx context.Context, id int64, key string) (*ProxyReplicaOperation, error) {
	if err := ValidateProxyReplicaOperationKey(key); err != nil {
		return nil, err
	}
	repo, ok := s.accountRepo.(ProxyReplicaRepository)
	if !ok {
		return nil, infraerrors.New(503, "PROXY_REPLICAS_UNAVAILABLE", "proxy replica repository unavailable")
	}
	return repo.GetProxyReplicaOperation(ctx, id, key)
}

func (a *Account) CredentialOwnerAccountID() int64 {
	if a == nil {
		return 0
	}
	return int64(a.getExtraFloat64(CredentialOwnerAccountIDKey))
}

func (a *Account) IsProxyReplica() bool { return a.CredentialOwnerAccountID() > 0 }
func (a *Account) QuotaOwnerAccountID() int64 {
	if a.IsProxyReplica() {
		return a.CredentialOwnerAccountID()
	}
	if a == nil {
		return 0
	}
	return a.ID
}
func (a *Account) IsProxyReplicaSource() bool {
	return a != nil && a.Extra[ProxyReplicaSourceKey] == true
}

// ProxyReplicaExtra copies configuration without a second copy of quota
// observations, harvested tickets, refresh controls, or external sync identity.
func ProxyReplicaExtra(extra map[string]any) (map[string]any, error) {
	out, err := duplicateAccountExtra(extra)
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	delete(out, ProxyReplicaSourceKey)
	delete(out, CredentialOwnerAccountIDKey)
	delete(out, ProxyReplicaOperationKey)
	for key := range out {
		if strings.HasPrefix(key, "codex_turn_ticket:") || strings.HasPrefix(key, "openai_auto_reset_credit") || strings.HasPrefix(key, "auto_reset_credit_") || key == OpenAIAutoResetCreditStateExtraKey {
			delete(out, key)
		}
	}
	return out, nil
}

func ProxyReplicaOwnerUsable(a *Account, now time.Time) bool {
	return a != nil && a.IsOpenAIOAuth() && !a.IsCredentialShadow() && a.IsActive() &&
		(a.ExpiresAt == nil || now.Before(*a.ExpiresAt)) &&
		(a.TempUnschedulableUntil == nil || !now.Before(*a.TempUnschedulableUntil)) &&
		!a.IsRateLimited() && !a.IsOverloaded() && !a.IsQuotaExceeded()
}

// ResolveProxyReplicaOwner is deliberately uncached. Revalidate before reading
// the owner's token cache; a cached token must never outlive owner revocation.
func ResolveProxyReplicaOwner(ctx context.Context, repo AccountRepository, account *Account) (*Account, error) {
	if account == nil || !account.IsProxyReplica() {
		return account, nil
	}
	if repo == nil {
		return nil, fmt.Errorf("proxy replica state unavailable")
	}
	latest, err := repo.GetByID(ctx, account.ID)
	if err != nil || latest == nil || latest.CredentialOwnerAccountID() != account.CredentialOwnerAccountID() ||
		!latest.IsActive() || latest.ProxyID == nil || latest.Proxy == nil || !latest.Proxy.IsActive() || latest.Proxy.IsExpired(time.Now()) ||
		account.ProxyID == nil || *latest.ProxyID != *account.ProxyID {
		return nil, fmt.Errorf("proxy replica or bound proxy unavailable")
	}
	owner, err := repo.GetByID(ctx, account.CredentialOwnerAccountID())
	if err != nil || !ProxyReplicaOwnerUsable(owner, time.Now()) || !owner.IsProxyReplicaSource() {
		return nil, fmt.Errorf("proxy replica credential owner unavailable")
	}
	return owner, nil
}

// Admission may revoke an owner while an already-submitted request finishes.
// Settlement still needs the pricing identity and must not discard that usage.
func resolveUsageBillingAccount(ctx context.Context, repo AccountRepository, account *Account) (*Account, error) {
	if !account.IsProxyReplica() {
		return resolveCredentialAccount(ctx, repo, account)
	}
	if repo != nil {
		if owner, err := repo.GetByID(ctx, account.CredentialOwnerAccountID()); err == nil && owner != nil {
			return owner, nil
		}
	}
	// The replica retains inherited pricing configuration and durable ownership
	// even after source deletion; this fallback is never used for authentication.
	return account, nil
}

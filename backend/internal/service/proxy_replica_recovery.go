package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"sort"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const ProxyRecoveryReadPermission = "accounts.proxy_replicas.recovery.read"
const ProxyRecoveryWritePermission = "accounts.proxy_replicas.recovery.resume"

type ProxyRecoveryReplica struct {
	AccountID        int64   `json:"account_id"`
	ProxyOperationID string  `json:"proxy_operation_id"`
	ProxyID          int64   `json:"proxy_id"`
	Concurrency      int     `json:"concurrency"`
	WSMode           string  `json:"ws_mode"`
	GroupIDs         []int64 `json:"group_ids"`
}
type ProxyRecoveryInput struct {
	Version  int                    `json:"version"`
	Replicas []ProxyRecoveryReplica `json:"replicas"`
}
type ProxyRecoveryResult struct {
	ProxyRecoveryReplica
	Schedulable    bool   `json:"schedulable"`
	RecoveryStatus string `json:"recovery_status"`
	Reason         string `json:"reason"`
}
type ProxyRecoveryOperation struct {
	BaselineGeneration   string                `json:"-"`
	Version              int                   `json:"version"`
	OperationID          string                `json:"operation_id"`
	SourceAccountID      int64                 `json:"source_account_id"`
	Status               string                `json:"status"`
	CredentialGeneration string                `json:"credential_generation"`
	SourceAuthRecovered  bool                  `json:"source_auth_recovered"`
	Replicas             []ProxyRecoveryResult `json:"replicas"`
}
type ProxyRecoveryCredentials struct {
	Account    *Account
	Generation string
	Verified   bool
}
type ProxyRecoveryRepository interface {
	ProxyReplicaRecovery(context.Context, int64, string, *ProxyRecoveryInput, string, bool, string) (*ProxyRecoveryOperation, error)
	ProxyRecoveryCredentials(context.Context, int64) (*ProxyRecoveryCredentials, error)
	VerifyProxyRecoveryGeneration(context.Context, int64, string) error
}
type ProxyRecoveryAdminService interface {
	ProxyRecoveryCapabilities(context.Context, int64) error
	PrepareProxyRecovery(context.Context, int64, string, ProxyRecoveryInput) (*ProxyRecoveryOperation, error)
	GetProxyRecovery(context.Context, int64, string) (*ProxyRecoveryOperation, error)
	ResumeProxyRecovery(context.Context, int64, string) (*ProxyRecoveryOperation, error)
}

func NormalizeProxyRecoveryInput(in ProxyRecoveryInput) (ProxyRecoveryInput, string, error) {
	if in.Version != 1 || len(in.Replicas) == 0 || len(in.Replicas) > 3200 {
		return in, "", infraerrors.BadRequest("INVALID_PROXY_RECOVERY", "version 1 and 1 to 3200 replicas required")
	}
	in.Replicas = append([]ProxyRecoveryReplica(nil), in.Replicas...)
	sort.Slice(in.Replicas, func(i, j int) bool { return in.Replicas[i].AccountID < in.Replicas[j].AccountID })
	for i := range in.Replicas {
		p := &in.Replicas[i]
		if p.AccountID <= 0 || p.ProxyID <= 0 || p.Concurrency <= 0 || p.Concurrency > 10000 || (i > 0 && p.AccountID == in.Replicas[i-1].AccountID) || ValidateProxyReplicaOperationKey(p.ProxyOperationID) != nil || len(p.GroupIDs) > 1000 {
			return in, "", infraerrors.BadRequest("INVALID_PROXY_RECOVERY", "invalid replica identity or configuration")
		}
		switch p.WSMode {
		case OpenAIWSIngressModeOff, OpenAIWSIngressModeCtxPool, OpenAIWSIngressModePassthrough, OpenAIWSIngressModeHTTPBridge:
		default:
			return in, "", infraerrors.BadRequest("INVALID_PROXY_RECOVERY", "invalid WS mode")
		}
		p.GroupIDs = append([]int64{}, p.GroupIDs...)
		slices.Sort(p.GroupIDs)
		for j, g := range p.GroupIDs {
			if g <= 0 || (j > 0 && g == p.GroupIDs[j-1]) {
				return in, "", infraerrors.BadRequest("INVALID_PROXY_RECOVERY", "invalid group IDs")
			}
		}
	}
	b, _ := json.Marshal(in)
	h := sha256.Sum256(b)
	return in, hex.EncodeToString(h[:]), nil
}
func (s *adminServiceImpl) ProxyRecoveryCapabilities(ctx context.Context, id int64) error {
	if err := s.authorizeProxyReplicaRequest(ctx, ProxyRecoveryReadPermission, id, nil); err != nil {
		return err
	}
	if _, ok := s.accountRepo.(ProxyRecoveryRepository); !ok {
		return infraerrors.New(503, "PROXY_RECOVERY_UNAVAILABLE", "recovery unavailable")
	}
	return nil
}
func (s *adminServiceImpl) PrepareProxyRecovery(ctx context.Context, id int64, key string, in ProxyRecoveryInput) (*ProxyRecoveryOperation, error) {
	if err := ValidateProxyReplicaOperationKey(key); err != nil {
		return nil, err
	}
	in, hash, err := NormalizeProxyRecoveryInput(in)
	if err != nil {
		return nil, err
	}
	if err = s.authorizeProxyReplicaRequest(ctx, ProxyRecoveryWritePermission, id, nil); err != nil {
		return nil, err
	}
	repo, ok := s.accountRepo.(ProxyRecoveryRepository)
	if !ok {
		return nil, infraerrors.New(503, "PROXY_RECOVERY_UNAVAILABLE", "recovery unavailable")
	}
	return repo.ProxyReplicaRecovery(ctx, id, key, &in, hash, false, ProxyRecoveryWritePermission)
}
func (s *adminServiceImpl) GetProxyRecovery(ctx context.Context, id int64, key string) (*ProxyRecoveryOperation, error) {
	return s.proxyRecovery(ctx, id, key, false)
}
func (s *adminServiceImpl) ResumeProxyRecovery(ctx context.Context, id int64, key string) (*ProxyRecoveryOperation, error) {
	return s.proxyRecovery(ctx, id, key, true)
}
func (s *adminServiceImpl) proxyRecovery(ctx context.Context, id int64, key string, resume bool) (*ProxyRecoveryOperation, error) {
	if err := ValidateProxyReplicaOperationKey(key); err != nil {
		return nil, err
	}
	permission := ProxyRecoveryReadPermission
	if resume {
		permission = ProxyRecoveryWritePermission
	}
	if err := s.authorizeProxyReplicaRequest(ctx, permission, id, nil); err != nil {
		return nil, err
	}
	repo, ok := s.accountRepo.(ProxyRecoveryRepository)
	if !ok {
		return nil, infraerrors.New(503, "PROXY_RECOVERY_UNAVAILABLE", "recovery unavailable")
	}
	result, err := repo.ProxyReplicaRecovery(ctx, id, key, nil, "", resume, permission)
	if err != nil {
		return result, err
	}
	if result.Status == "completed" {
		return result, nil
	}
	credentials, err := repo.ProxyRecoveryCredentials(ctx, id)
	if err != nil {
		if infraerrors.Code(err) == 404 {
			return result, nil
		}
		return nil, err
	}
	// Verify the exact fresh snapshot outside all database locks. Never refresh
	// tokens, use cached usage, or send a billable inference to prove recovery.
	if credentials.Generation != result.BaselineGeneration && !credentials.Verified && ProxyReplicaOwnerUsable(credentials.Account, time.Now()) {
		if err = s.verifyProxyRecoveryCredentials(ctx, credentials.Account); err == nil {
			if err = repo.VerifyProxyRecoveryGeneration(ctx, id, credentials.Generation); err != nil {
				return nil, err
			}
		}
	}
	return repo.ProxyReplicaRecovery(ctx, id, key, nil, "", resume, permission)
}
func (s *adminServiceImpl) verifyProxyRecoveryCredentials(ctx context.Context, a *Account) error {
	if s.privacyClientFactory == nil || a == nil || !a.IsProxyReplicaSource() || a.GetOpenAIAccessToken() == "" {
		return infraerrors.New(503, "PROXY_RECOVERY_VERIFY_UNAVAILABLE", "credential verification unavailable")
	}
	proxyURL := ""
	if a.ProxyID != nil {
		if a.Proxy == nil || !a.Proxy.IsActive() || a.Proxy.IsExpired(time.Now()) {
			return infraerrors.Conflict("PROXY_RECOVERY_SOURCE_UNAVAILABLE", "source proxy unavailable")
		}
		proxyURL = a.Proxy.URL()
	}
	client, err := s.privacyClientFactory(proxyURL)
	if err != nil {
		return infraerrors.New(503, "PROXY_RECOVERY_VERIFY_UNAVAILABLE", "credential verification unavailable")
	}
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var usage OpenAIQuotaUsage
	resp, err := client.R().SetContext(callCtx).SetHeaders(buildCodexCommonHeaders(a.GetOpenAIAccessToken(), a.GetCredential("chatgpt_account_id"), false)).SetSuccessResult(&usage).Get(chatGPTUsageURL)
	if err != nil || resp == nil || !resp.IsSuccessState() || (usage.PlanType == "" && usage.RateLimit == nil) {
		return infraerrors.Conflict("PROXY_RECOVERY_AUTH_UNVERIFIED", "new credentials are not verified")
	}
	return nil
}

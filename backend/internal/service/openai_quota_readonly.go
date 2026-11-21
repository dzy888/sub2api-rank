package service

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

// QueryUsageReadOnly uses the authorized account snapshot without refreshing
// credentials, persisting quota evidence, recovering limits or consuming reset
// credits. An account can be paused/expired while delivery is being verified.
func (s *OpenAIQuotaService) QueryUsageReadOnly(ctx context.Context, account *Account) (*OpenAIQuotaUsage, error) {
	if err := authorizeReadOnlyQuotaAccount(ctx, account); err != nil {
		return nil, err
	}
	if s == nil || s.privacyClientFactory == nil {
		return nil, infraerrors.ServiceUnavailable("OPENAI_QUOTA_NOT_CONFIGURED", "read-only quota query unavailable")
	}
	credentialAccount, transportAccount := account, account
	if account.IsShadow() || account.IsProxyReplica() {
		ownerID := account.CredentialOwnerAccountID()
		if account.IsShadow() {
			ownerID = *account.ParentAccountID
		}
		if s.accountRepo == nil {
			return nil, infraerrors.ServiceUnavailable("OPENAI_QUOTA_NOT_CONFIGURED", "credential owner lookup unavailable")
		}
		owner, err := s.accountRepo.GetByID(ctx, ownerID)
		if err != nil || owner == nil || owner.ID != ownerID {
			return nil, ErrAccountNotFound
		}
		// A linked row cannot grant access to an otherwise unauthorized owner.
		if err := authorizeReadOnlyQuotaAccount(ctx, owner); err != nil {
			return nil, err
		}
		if owner.IsShadow() || owner.IsProxyReplica() {
			return nil, infraerrors.Conflict("OPENAI_QUOTA_CREDENTIAL_OWNER_INVALID", "credential owner is unavailable")
		}
		credentialAccount = owner
		if account.IsShadow() {
			transportAccount = owner
		}
	}
	if credentialAccount.AuthConnectionID != "" || credentialAccount.HasManagedOpenAIConnections() || credentialAccount.IsOpenAIAgentIdentity() {
		return nil, infraerrors.Conflict("OPENAI_QUOTA_READ_ONLY_AUTH_UNAVAILABLE", "read-only quota query requires stored OAuth credentials")
	}
	accessToken := strings.TrimSpace(credentialAccount.GetOpenAIAccessToken())
	if accessToken == "" {
		return nil, infraerrors.Conflict("OPENAI_QUOTA_TOKEN_UNAVAILABLE", "stored access token is unavailable; reauthorize the account")
	}
	chatGPTAccountID := strings.TrimSpace(credentialAccount.GetCredential("chatgpt_account_id"))
	if chatGPTAccountID == "" {
		chatGPTAccountID = strings.TrimSpace(credentialAccount.GetCredential("organization_id"))
	}
	if chatGPTAccountID == "" {
		return nil, infraerrors.BadRequest("OPENAI_QUOTA_MISSING_ACCOUNT_ID", "chatgpt_account_id is missing; reauthorize the account")
	}
	proxyURL, err := s.readOnlyQuotaProxy(ctx, transportAccount)
	if err != nil {
		return nil, err
	}
	client, err := s.privacyClientFactory(proxyURL)
	if err != nil || client == nil {
		return nil, infraerrors.New(http.StatusBadGateway, "OPENAI_QUOTA_CLIENT_ERROR", "upstream quota client unavailable")
	}
	callCtx, cancel := context.WithTimeout(ctx, openaiQuotaUpstreamTimeout)
	defer cancel()
	resp, err := client.R().SetContext(callCtx).
		SetHeaders(buildCodexCommonHeaders(accessToken, chatGPTAccountID, credentialAccount.IsChatGPTAccountFedRAMP())).
		Get(chatGPTUsageURL)
	if err != nil || resp == nil {
		return nil, infraerrors.New(http.StatusBadGateway, "OPENAI_QUOTA_REQUEST_FAILED", "upstream quota request failed")
	}
	if !resp.IsSuccessState() {
		// Never reflect upstream bodies, transport errors or credential values.
		return nil, infraerrors.New(mapUpstreamStatus(resp.StatusCode), "OPENAI_QUOTA_UPSTREAM_ERROR", "upstream quota query rejected")
	}
	// Missing/null flags must not silently decode as false. Delivery acceptance
	// requires the actual two booleans reported by OpenAI.
	var flags struct {
		RateLimit *struct {
			Allowed      *bool `json:"allowed"`
			LimitReached *bool `json:"limit_reached"`
		} `json:"rate_limit"`
	}
	var payload OpenAIQuotaUsage
	if err := json.Unmarshal(resp.Bytes(), &flags); err != nil || flags.RateLimit == nil || flags.RateLimit.Allowed == nil || flags.RateLimit.LimitReached == nil {
		return nil, infraerrors.New(http.StatusBadGateway, "OPENAI_QUOTA_RESPONSE_INVALID", "upstream quota response is missing boolean rate-limit flags")
	}
	if err := json.Unmarshal(resp.Bytes(), &payload); err != nil {
		return nil, infraerrors.New(http.StatusBadGateway, "OPENAI_QUOTA_RESPONSE_INVALID", "upstream quota response has invalid fields")
	}
	payload.FetchedAt = time.Now().Unix()
	return &payload, nil
}

func authorizeReadOnlyQuotaAccount(ctx context.Context, account *Account) error {
	if account == nil || account.ID <= 0 || account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return ErrAccountNotFound
	}
	if p := adminaccess.FromContext(ctx); p != nil && !p.Policy.AllowsAccount("openai.quota.read", scopedAccountResource(account)) {
		return ErrAccountNotFound
	}
	return nil
}

func (s *OpenAIQuotaService) readOnlyQuotaProxy(ctx context.Context, account *Account) (string, error) {
	if account.OpenAIRequestProxyEnabled() {
		return resolveOpenAIMaintenanceProxy(ctx, account, s.proxyRepo)
	}
	if account.ProxyID == nil && !account.IsProxyReplica() {
		return "", nil
	}
	proxy := account.Proxy
	if proxy == nil && account.ProxyID != nil && s.proxyRepo != nil {
		var err error
		proxy, err = s.proxyRepo.GetByID(ctx, *account.ProxyID)
		if err != nil {
			return "", infraerrors.ServiceUnavailable("OPENAI_PROXY_UNAVAILABLE", "bound quota proxy unavailable")
		}
	}
	if proxy == nil || account.ProxyID == nil || proxy.ID != *account.ProxyID || !proxy.IsActive() || proxy.IsExpired(time.Now()) {
		return "", infraerrors.ServiceUnavailable("OPENAI_PROXY_UNAVAILABLE", "bound quota proxy unavailable")
	}
	return proxy.URL(), nil
}

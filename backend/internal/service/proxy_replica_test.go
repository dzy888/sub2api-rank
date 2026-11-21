package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type proxyReplicaTestRepo struct {
	AccountRepository
	mu       sync.Mutex
	accounts map[int64]*Account
}

func (r *proxyReplicaTestRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.accounts[id]
	if a == nil {
		return nil, ErrAccountNotFound
	}
	copy := a.CopySchedulerSnapshot()
	return &copy, nil
}

func (r *proxyReplicaTestRepo) UpdateCredentials(_ context.Context, id int64, credentials map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.accounts[id] == nil {
		return ErrAccountNotFound
	}
	r.accounts[id].Credentials = shallowCopyMap(credentials)
	return nil
}

func (r *proxyReplicaTestRepo) UpdateOpenAIOAuthCredentialsIfUnchanged(ctx context.Context, id int64, expected map[string]any, proxyID *int64, credentials map[string]any) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	account := r.accounts[id]
	if account == nil || !account.IsOpenAIOAuth() || account.IsProxyReplica() ||
		!reflect.DeepEqual(account.Credentials, expected) || !reflect.DeepEqual(account.ProxyID, proxyID) {
		return false, nil
	}
	account.Credentials = shallowCopyMap(credentials)
	return true, nil
}

func (r *proxyReplicaTestRepo) IsProxyReplicaSourceID(ctx context.Context, id int64) (bool, error) {
	a, err := r.GetByID(ctx, id)
	if err != nil {
		return false, err
	}
	return a.IsProxyReplicaSource(), nil
}

type proxyReplicaTestCache struct{ GeminiTokenCache }

func (*proxyReplicaTestCache) AcquireRefreshLock(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (*proxyReplicaTestCache) ReleaseRefreshLock(context.Context, string) error { return nil }
func (*proxyReplicaTestCache) DeleteAccessToken(context.Context, string) error  { return nil }

type proxyReplicaTestExecutor struct{ calls atomic.Int32 }

func (*proxyReplicaTestExecutor) CacheKey(a *Account) string { return OpenAITokenCacheKey(a) }
func (*proxyReplicaTestExecutor) CanRefresh(a *Account) bool { return !a.IsProxyReplica() }
func (*proxyReplicaTestExecutor) NeedsRefresh(a *Account, _ time.Duration) bool {
	return a.GetCredential("access_token") == "old"
}
func (e *proxyReplicaTestExecutor) Refresh(_ context.Context, a *Account) (map[string]any, error) {
	if a.ID != 123 {
		return nil, errors.New("refreshed a replica")
	}
	e.calls.Add(1)
	return map[string]any{"access_token": "new", "refresh_token": "rotated"}, nil
}

func proxyReplicaFixture() (*proxyReplicaTestRepo, *Account, *Account) {
	owner := &Account{ID: 123, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: false, Extra: map[string]any{ProxyReplicaSourceKey: true}, Credentials: map[string]any{"access_token": "old", "refresh_token": "refresh"}}
	proxyID := int64(11)
	replica := &Account{ID: 456, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, ProxyID: &proxyID, Proxy: &Proxy{ID: 11, Status: StatusActive}, Extra: map[string]any{CredentialOwnerAccountIDKey: int64(123)}}
	return &proxyReplicaTestRepo{accounts: map[int64]*Account{123: owner, 456: replica}}, owner, replica
}

func TestProxyReplicaConfigIdentityAndValidation(t *testing.T) {
	a := ProxyReplicasInput{Proxies: []ProxyReplicaInput{{12, 8}, {11, 5}}, WSMode: "ctx_pool", ActivateOnSuccess: true}
	b := a
	b.Proxies = []ProxyReplicaInput{{11, 5}, {12, 8}}
	_, ah, err := NormalizeProxyReplicasInput(a)
	require.NoError(t, err)
	_, bh, err := NormalizeProxyReplicasInput(b)
	require.NoError(t, err)
	require.Equal(t, ah, bh)
	b.ActivateOnSuccess = false
	_, bh, err = NormalizeProxyReplicasInput(b)
	require.NoError(t, err)
	require.NotEqual(t, ah, bh)
	for _, in := range []ProxyReplicasInput{{}, {Proxies: []ProxyReplicaInput{{11, 5}, {11, 8}}, WSMode: "ctx_pool"}, {Proxies: []ProxyReplicaInput{{11, 0}}, WSMode: "ctx_pool"}, {Proxies: []ProxyReplicaInput{{11, 5}}, WSMode: "bogus"}} {
		_, _, err := NormalizeProxyReplicasInput(in)
		require.Error(t, err)
	}
	for _, key := range []string{"", " bad", "a/b", "a\nb"} {
		require.Error(t, ValidateProxyReplicaOperationKey(key))
	}
}

func TestProxyReplicaOwnerSchedulingAndRevocation(t *testing.T) {
	repo, owner, replica := proxyReplicaFixture()
	require.False(t, owner.IsSchedulable())
	require.True(t, ProxyReplicaOwnerUsable(owner, time.Now()))
	resolved, err := ResolveProxyReplicaOwner(context.Background(), repo, replica)
	require.NoError(t, err)
	require.Equal(t, owner.ID, resolved.ID)
	require.Nil(t, replica.ParentAccountID)
	require.False(t, replica.IsShadow())
	require.Equal(t, owner.ID, replica.RPMAccountID())
	require.Equal(t, owner.ID, replica.QuotaOwnerAccountID())
	require.Equal(t, OpenAITokenCacheKey(owner), OpenAITokenCacheKey(replica))
	owner.Status = "disabled"
	_, err = ResolveProxyReplicaOwner(context.Background(), repo, replica)
	require.Error(t, err)
	owner.Status = StatusActive
	expired := time.Now().Add(-time.Minute)
	owner.ExpiresAt = &expired
	_, err = ResolveProxyReplicaOwner(context.Background(), repo, replica)
	require.Error(t, err)
	owner.ExpiresAt = nil
	delete(repo.accounts, owner.ID)
	_, err = ResolveProxyReplicaOwner(context.Background(), repo, replica)
	require.Error(t, err)
}

func TestProxyReplicaConcurrentRefreshUsesOneOwner(t *testing.T) {
	repo, _, replica := proxyReplicaFixture()
	second := replica.CopySchedulerSnapshot()
	second.ID = 457
	pid := int64(12)
	second.ProxyID = &pid
	second.Proxy = &Proxy{ID: 12, Status: StatusActive}
	repo.accounts[457] = &second
	api := NewOAuthRefreshAPI(repo, &proxyReplicaTestCache{})
	executor := &proxyReplicaTestExecutor{}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			selected := replica
			if i%2 == 1 {
				selected = &second
			}
			_, err := api.RefreshIfNeeded(context.Background(), selected, executor, time.Minute)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), executor.calls.Load())
	require.Empty(t, repo.accounts[456].Credentials)
	require.Empty(t, repo.accounts[457].Credentials)
	require.Equal(t, "new", repo.accounts[123].GetCredential("access_token"))
}

func TestProxyReplicaInvalidProxyCannotFallBack(t *testing.T) {
	repo, _, replica := proxyReplicaFixture()
	expired := time.Now().Add(-time.Second)
	replica.Proxy.ExpiresAt = &expired
	replica.Proxy.FallbackMode = FallbackModeDirect
	_, err := ResolveProxyReplicaOwner(context.Background(), repo, replica)
	require.Error(t, err)
	svc := &OpenAIGatewayService{}
	_, ok := svc.resolveRuntimeProxyFallback(context.Background(), replica)
	require.False(t, ok)
	_, err = svc.doUpstreamWithProxyFallback(context.Background(), nil, replica, "")
	require.Error(t, err)
}

func TestProxyReplicaResultContainsNoCredentials(t *testing.T) {
	b, err := json.Marshal(ProxyReplicaOperation{OperationID: "harbor-example", Replicas: []ProxyReplicaResult{{AccountID: 456, CredentialOwnerAccountID: 123}}})
	require.NoError(t, err)
	for _, secret := range []string{"access_token", "refresh_token", "credentials", "id_token"} {
		require.NotContains(t, string(b), secret)
	}
}

func TestProxyReplicaTokenReadsCurrentOwnerWithoutCache(t *testing.T) {
	repo, owner, replica := proxyReplicaFixture()
	owner.Credentials["access_token"] = "current-token"
	owner.Credentials["expires_at"] = time.Now().Add(time.Hour).Format(time.RFC3339)
	// The cache intentionally has no Get/Set implementation. Any use of a
	// stale token cache rather than the current owner document will panic.
	cache := &proxyReplicaTestCache{}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), &proxyReplicaTestExecutor{})
	token, err := provider.GetAccessToken(context.Background(), replica)
	require.NoError(t, err)
	require.Equal(t, "current-token", token)
	owner.Credentials["access_token"] = "rotated-token"
	token, err = provider.GetAccessToken(context.Background(), replica)
	require.NoError(t, err)
	require.Equal(t, "rotated-token", token)
	owner.Status = "disabled"
	_, err = provider.GetAccessToken(context.Background(), replica)
	require.Error(t, err)
}

func TestProxyReplicaConversionFencesOldSourceAdmissionSnapshot(t *testing.T) {
	repo, _, _ := proxyReplicaFixture()
	svc := &OpenAIGatewayService{accountRepo: repo}
	old := &Account{ID: 123, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true}
	_, err := svc.latestOpenAITurnAccountForGroup(context.Background(), old, 0, false)
	require.ErrorContains(t, err, "credential_owner_only")
}

func TestProxyReplicaExtraDoesNotDuplicateQuotaOrTickets(t *testing.T) {
	extra, err := ProxyReplicaExtra(map[string]any{ProxyReplicaSourceKey: true, "codex_7d_used_percent": float64(100), "codex_turn_ticket:example": "synthetic-ticket", "openai_auto_reset_credit_enabled": true, "crs_account_id": "external", "keep_config": true})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"keep_config": true}, extra)
}

func TestProxyReplicaKeepsBoundExitWithRequestPoolSettings(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		_, _, replica := proxyReplicaFixture()
		modeKey, idsKey := OpenAIProxyModeKey, OpenAIProxyIDsKey
		if legacy {
			modeKey, idsKey = ExcelBPSProxyModeKey, ExcelBPSProxyIDsKey
			replica.Extra["openai_excel_bps"] = true
		}
		replica.Extra[modeKey] = "request"
		replica.Extra[idsKey] = []int64{22, 33}
		require.False(t, replica.OpenAIRequestProxyEnabled())
		require.False(t, replica.ExcelBPSRequestProxyEnabled())
		svc := &adminServiceImpl{}
		require.Error(t, svc.validateExcelBPSProxySettings(context.Background(), replica))
		extra, err := ProxyReplicaExtra(replica.Extra)
		require.NoError(t, err)
		require.NotContains(t, extra, modeKey)
		require.NotContains(t, extra, idsKey)
		require.Equal(t, int64(11), *replica.ProxyID)
		require.Equal(t, "request", replica.Extra[modeKey], "copy must preserve the source configuration")
		if legacy {
			replica.Extra[modeKey] = ExcelBPSProxyModeSession
		} else {
			replica.Extra[modeKey] = OpenAIProxyModeSingle
		}
		require.NoError(t, svc.validateExcelBPSProxySettings(context.Background(), replica), "single-proxy edits must remain available")
	}
}

func TestProxyReplicaRevocationDoesNotDiscardInflightUsage(t *testing.T) {
	repo, owner, replica := proxyReplicaFixture()
	owner.Status = "disabled"
	billing, err := resolveUsageBillingAccount(context.Background(), repo, replica)
	require.NoError(t, err)
	require.Equal(t, owner.ID, billing.ID)
	delete(repo.accounts, owner.ID)
	billing, err = resolveUsageBillingAccount(context.Background(), repo, replica)
	require.NoError(t, err)
	require.Equal(t, owner.ID, billing.QuotaOwnerAccountID())
	_, err = ResolveProxyReplicaOwner(context.Background(), repo, replica)
	require.Error(t, err)
}

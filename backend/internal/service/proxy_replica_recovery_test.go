package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

func TestProxyRecoveryIdentityValidation(t *testing.T) {
	in := ProxyRecoveryInput{Version: 1, Replicas: []ProxyRecoveryReplica{{AccountID: 10, ProxyOperationID: "original", ProxyID: 5, Concurrency: 8, WSMode: "ctx_pool", GroupIDs: []int64{2, 1}}}}
	normalized, hash, err := NormalizeProxyRecoveryInput(in)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, normalized.Replicas[0].GroupIDs)
	same := normalized
	_, other, err := NormalizeProxyRecoveryInput(same)
	require.NoError(t, err)
	require.Equal(t, hash, other)
	same.Replicas[0].Concurrency++
	_, other, err = NormalizeProxyRecoveryInput(same)
	require.NoError(t, err)
	require.NotEqual(t, hash, other)
	for _, body := range []string{`{"version":2,"replicas":[]}`, `{"version":1,"replicas":[]}`, `{"version":1,"replicas":[{"account_id":1,"proxy_operation_id":"op","proxy_id":1,"concurrency":1,"ws_mode":"off","group_ids":[1,1]}]}`} {
		var bad ProxyRecoveryInput
		require.NoError(t, json.Unmarshal([]byte(body), &bad))
		_, _, err = NormalizeProxyRecoveryInput(bad)
		require.Equal(t, 400, infraerrors.Code(err))
	}
}

type expiredProxyRecoveryRepo struct{ *proxyReplicaTestRepo }

func (*expiredProxyRecoveryRepo) ProxyReplicaRecovery(_ context.Context, _ int64, _ string, _ *ProxyRecoveryInput, _ string, resume bool, _ string) (*ProxyRecoveryOperation, error) {
	if resume {
		return nil, infraerrors.Conflict("PROXY_RECOVERY_EXPIRED", "recovery expired")
	}
	return &ProxyRecoveryOperation{Status: "waiting"}, nil
}
func (*expiredProxyRecoveryRepo) ProxyRecoveryCredentials(context.Context, int64) (*ProxyRecoveryCredentials, error) {
	return nil, infraerrors.NotFound("SOURCE_DELETED", "source deleted")
}
func (*expiredProxyRecoveryRepo) VerifyProxyRecoveryGeneration(context.Context, int64, string) error {
	panic("expired recovery must not verify")
}
func TestProxyRecoveryExpiredResumeIsRejectedBeforeDeletedCredentialLookup(t *testing.T) {
	repo, source, _ := proxyReplicaFixture()
	s := &adminServiceImpl{accountRepo: &expiredProxyRecoveryRepo{repo}}
	_, err := s.ResumeProxyRecovery(context.Background(), source.ID, "expired-op")
	require.Equal(t, 409, infraerrors.Code(err))
}
func TestProxyRecoveryVerificationUsesExactSnapshotWithoutInference(t *testing.T) {
	for _, tc := range []struct {
		status  int
		body    string
		success bool
	}{{200, `{"plan_type":"plus","rate_limit":{"allowed":true}}`, true}, {401, `{"error":{"code":"invalid_token"}}`, false}, {200, `{}`, false}, {200, `{"plan_type":`, false}} {
		t.Run(tc.body, func(t *testing.T) {
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/backend-api/wham/usage", r.URL.Path)
				require.Equal(t, "Bearer fresh-token", r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			s := &adminServiceImpl{privacyClientFactory: func(string) (*req.Client, error) {
				return req.C().SetBaseURL(upstream.URL).OnBeforeRequest(func(_ *req.Client, r *req.Request) error {
					r.RawURL = upstream.URL + "/backend-api/wham/usage"
					return nil
				}), nil
			}}
			a := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Extra: map[string]any{ProxyReplicaSourceKey: true}, Credentials: map[string]any{"access_token": "fresh-token"}}
			err := s.verifyProxyRecoveryCredentials(context.Background(), a)
			if tc.success {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, 1, calls)
		})
	}
}

func TestProxyReplicaPermissionRequiresAllGroupsAndProxies(t *testing.T) {
	repo, source, _ := proxyReplicaFixture()
	source.GroupIDs = []int64{1, 2}
	s := &adminServiceImpl{accountRepo: repo}
	principal := &adminaccess.Principal{Policy: adminaccess.Policy{GlobalPermissions: []string{"proxies.read"}, Rules: []adminaccess.Rule{{Permissions: []string{"accounts.proxy_replicas.create", ProxyRecoveryWritePermission}, GroupIDs: []int64{1}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"}}}}}
	ctx := adminaccess.WithPrincipal(context.Background(), principal)
	for _, permission := range []string{"accounts.proxy_replicas.create", ProxyRecoveryWritePermission} {
		require.Equal(t, 404, infraerrors.Code(s.authorizeProxyReplicaRequest(ctx, permission, source.ID, nil)))
		principal.Policy.Rules[0].GroupIDs = []int64{1, 2}
		require.NoError(t, s.authorizeProxyReplicaRequest(ctx, permission, source.ID, nil))
		principal.Policy.Rules[0].GroupIDs = []int64{1}
	}
	principal.Policy.GlobalPermissions = nil
	require.Equal(t, 403, infraerrors.Code(s.authorizeProxyReplicaRequest(ctx, ProxyRecoveryWritePermission, source.ID, nil)))
}

//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

func TestScopedQuotaReadUsesDatabaseGroupsAndPreservesAccountState(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := newAccountRepositoryWithSQL(client, integrationDB, nil)
	group := mustCreateGroup(t, client, &service.Group{Name: "readonly-quota-authorized", Platform: "openai"})
	otherGroup := mustCreateGroup(t, client, &service.Group{Name: "readonly-quota-foreign", Platform: "openai"})
	expired, reset := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	create := func(name string, groupID int64) *service.Account {
		account := &service.Account{Name: name, Platform: "openai", Type: "oauth", Status: "active", Schedulable: false,
			ExpiresAt: &expired, AutoPauseOnExpired: true, RateLimitedAt: &expired, RateLimitResetAt: &reset,
			Concurrency: 5, Credentials: map[string]any{"access_token": "PRIVATE_ACCESS", "refresh_token": "PRIVATE_REFRESH", "chatgpt_account_id": "fixture-workspace"},
			Extra: map[string]any{"quota_test_marker": "unchanged"}}
		require.NoError(t, repo.CreateWithAccountGroups(ctx, account, []service.AccountGroup{{GroupID: groupID}}))
		return account
	}
	allowed := create("readonly-quota-paused", group.ID)
	foreign := create("readonly-quota-out-of-scope", otherGroup.ID)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "/backend-api/wham/usage", r.URL.Path)
		require.Equal(t, "Bearer PRIVATE_ACCESS", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"account_id":"fixture-workspace","plan_type":"plus","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":0,"limit_window_seconds":18000,"reset_after_seconds":3600},"secondary_window":{"used_percent":0,"limit_window_seconds":604800,"reset_after_seconds":3600}}}`))
	}))
	defer upstream.Close()
	target, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	factory := func(string) (*req.Client, error) {
		return req.C().WrapRoundTripFunc(func(rt req.RoundTripper) req.RoundTripFunc {
			return func(r *req.Request) (*req.Response, error) {
				r.URL.Scheme, r.URL.Host = target.Scheme, target.Host
				return rt.RoundTrip(r)
			}
		}), nil
	}
	quota := service.NewOpenAIQuotaService(repo, nil, nil, factory, nil)
	principal := &adminaccess.Principal{KeyID: "same-scoped-key", Policy: adminaccess.Policy{Rules: []adminaccess.Rule{{
		Permissions: []string{"openai.quota.read"}, GroupIDs: []int64{group.ID}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"},
	}}}}
	ctx = adminaccess.WithPrincipal(ctx, principal)
	snapshot := func(id int64) string {
		var row string
		require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT to_jsonb(a)::text FROM accounts a WHERE id=$1`, id).Scan(&row))
		return row
	}
	for _, tc := range []struct {
		account *service.Account
		status  int
	}{{allowed, 200}, {foreign, 404}} {
		before := snapshot(tc.account.ID)
		account, err := repo.GetByID(ctx, tc.account.ID)
		require.NoError(t, err)
		usage, err := quota.QueryUsageReadOnly(ctx, account)
		require.Equal(t, tc.status, infraerrors.Code(err))
		require.Equal(t, before, snapshot(tc.account.ID), "GET must preserve the entire row, including scheduling, limits, credentials and quota snapshots")
		if tc.status == 200 {
			encoded, err := json.Marshal(usage)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "PRIVATE")
			require.True(t, usage.RateLimit.Allowed)
			require.False(t, usage.RateLimit.LimitReached)
			require.False(t, usage.UsageCachePersisted)
		}
	}
	require.EqualValues(t, 1, calls.Load(), "unauthorized account must never reach OpenAI")
}

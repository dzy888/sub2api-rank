//go:build integration

package repository_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type requestProxyReadbackConcurrency struct{ service.ConcurrencyCache }

func (*requestProxyReadbackConcurrency) GetAccountConcurrencyBatch(_ context.Context, ids []int64) (map[int64]int, error) {
	counts := make(map[int64]int, len(ids))
	for _, id := range ids {
		counts[id] = 0
	}
	return counts, nil
}

func TestScopedRequestProxyGroupHTTPRoundTripAndRollback(t *testing.T) {
	db, client := repository.UpstreamChannelsTestDatabase(t)
	ctx := context.Background()
	accounts := repository.NewAdminAccountRepository(client, db, nil)
	groups := repository.NewAdminGroupRepository(client, db)
	proxies := repository.NewProxyRepository(client, db)
	api := service.NewAdminService(&config.Config{}, nil, groups, accounts, proxies, nil, nil, nil, nil, nil, nil, nil, nil, client, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	group, err := client.Group.Create().SetName("push-network-authorized").SetPlatform("openai").Save(ctx)
	require.NoError(t, err)
	foreignGroup, err := client.Group.Create().SetName("push-network-foreign").SetPlatform("openai").Save(ctx)
	require.NoError(t, err)
	newProxy := func(name, status string, expired bool) int64 {
		p := &service.Proxy{Name: name, Protocol: "http", Host: "127.0.0.1", Port: 8080, Status: status, Username: "PRIVATE_PROXY", Password: "PRIVATE_PROXY_PASSWORD"}
		if expired {
			past := time.Now().Add(-time.Hour)
			p.ExpiresAt = &past
		}
		require.NoError(t, proxies.Create(ctx, p))
		return p.ID
	}
	first, second := newProxy("first", "active", false), newProxy("second", "active", false)
	disabled, expired := newProxy("disabled", "inactive", false), newProxy("expired", "active", true)
	guardExpiry := time.Now().Add(-time.Hour).Truncate(time.Second)
	create := func(name, platform, kind string, groupID int64) *service.Account {
		notes := "harbor-oauth:original"
		a := &service.Account{Name: name, Platform: platform, Type: kind, Notes: &notes, Status: "active", Schedulable: false,
			Concurrency: 5, Priority: 2, ProxyID: &first, ExpiresAt: &guardExpiry, AutoPauseOnExpired: true,
			Credentials: map[string]any{"access_token": "PRIVATE_ACCESS", "refresh_token": "PRIVATE_REFRESH", "model_mapping": map[string]any{"fixture": "fixture"}},
			Extra:       map[string]any{"private_setting": map[string]any{"keep": true}, "openai_oauth_responses_websockets_v2_mode": "ctx_pool"}}
		require.NoError(t, accounts.CreateWithAccountGroups(ctx, a, []service.AccountGroup{{GroupID: groupID}}))
		return a
	}
	allowed := create("push-paused-account", "openai", "oauth", group.ID)
	foreign := create("push-foreign-account", "openai", "oauth", foreignGroup.ID)
	otherPlatform := create("push-other-platform", "anthropic", "oauth", group.ID)
	otherType := create("push-api-key", "openai", "apikey", group.ID)
	principal := &adminaccess.Principal{KeyID: "same-push-key", Policy: adminaccess.Policy{Rules: []adminaccess.Rule{{
		Permissions: []string{"accounts.network", "accounts.read", "accounts.delete"}, GroupIDs: []int64{group.ID}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"},
	}}}}
	h := admin.NewAccountHandler(api, nil, nil, nil, nil, nil, nil, nil, nil, service.NewConcurrencyService(&requestProxyReadbackConcurrency{}), nil, nil, nil, nil)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		if !adminaccess.RouteAllowed(principal.Policy, c.Request.Method, c.FullPath()) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		c.Request = c.Request.WithContext(adminaccess.WithPrincipal(c.Request.Context(), principal))
		c.Next()
	})
	router.Use(h.ScopedAdminKeyAccounts)
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		router.Handle(method, "/api/v1/admin/accounts/:id", func(*gin.Context) { t.Fatal("must use scoped handler") })
	}
	request := func(method string, id int64, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, fmt.Sprintf("/api/v1/admin/accounts/%d", id), strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		require.NotContains(t, w.Body.String(), "PRIVATE")
		return w
	}
	payload := func(ids ...int64) string {
		raw, err := json.Marshal(map[string]any{"extra": map[string]any{"openai_proxy_mode": "request", "openai_proxy_ids": ids}})
		require.NoError(t, err)
		return string(raw)
	}
	snapshot := func(id int64) string {
		var row string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT to_jsonb(a)::text FROM accounts a WHERE id=$1`, id).Scan(&row))
		return row
	}
	readback := func(ids ...int64) {
		w := request(http.MethodGet, allowed.ID, "")
		require.Equal(t, 200, w.Code, w.Body.String())
		var out struct {
			Data struct {
				Extra struct {
					Mode string  `json:"openai_proxy_mode"`
					IDs  []int64 `json:"openai_proxy_ids"`
				} `json:"extra"`
				Schedulable bool `json:"schedulable"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
		require.Equal(t, "request", out.Data.Extra.Mode)
		require.Equal(t, ids, out.Data.Extra.IDs)
		require.False(t, out.Data.Schedulable)
		require.NotContains(t, w.Body.String(), "private_setting")
	}
	w := request(http.MethodPut, allowed.ID, payload(first, second))
	require.Equal(t, 200, w.Code, w.Body.String())
	readback(first, second)
	saved, err := accounts.GetByID(ctx, allowed.ID)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"keep": true}, saved.Extra["private_setting"])
	require.Equal(t, "ctx_pool", saved.Extra["openai_oauth_responses_websockets_v2_mode"])
	require.Equal(t, allowed.Credentials, saved.Credentials)
	require.Equal(t, allowed.ProxyID, saved.ProxyID)
	require.Equal(t, allowed.Concurrency, saved.Concurrency)
	require.Equal(t, allowed.ExpiresAt.Unix(), saved.ExpiresAt.Unix())
	require.Equal(t, allowed.Notes, saved.Notes)
	require.Equal(t, []int64{group.ID}, saved.GroupIDs)
	require.False(t, saved.Schedulable)
	// Patch the existing group, deduplicating IDs through the existing validator.
	w = request(http.MethodPut, allowed.ID, fmt.Sprintf(`{"extra":{"openai_proxy_ids":[%d,%d,%d]}}`, second, first, second))
	require.Equal(t, 200, w.Code, w.Body.String())
	readback(second, first)
	for _, tc := range []struct {
		name string
		id   int64
		body string
		want int
	}{
		{"foreign group", foreign.ID, payload(first, second), 404},
		{"other platform", otherPlatform.ID, payload(first, second), 404},
		{"other type", otherType.ID, payload(first, second), 404},
		{"disabled proxy", allowed.ID, payload(disabled), 400},
		{"expired proxy", allowed.ID, payload(expired), 400},
		{"missing proxy", allowed.ID, payload(999999999), 400},
		{"empty group", allowed.ID, `{"extra":{"openai_proxy_mode":"request","openai_proxy_ids":[]}}`, 400},
		{"invalid mode", allowed.ID, `{"extra":{"openai_proxy_mode":"random","openai_proxy_ids":[1]}}`, 400},
		{"foreign extra", allowed.ID, `{"extra":{"private_setting":false}}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := snapshot(tc.id)
			w := request(http.MethodPut, tc.id, tc.body)
			require.Equal(t, tc.want, w.Code, w.Body.String())
			require.Equal(t, before, snapshot(tc.id), "failure must roll back all configuration changes")
		})
	}
	for _, account := range []*service.Account{foreign, otherPlatform, otherType} {
		require.Equal(t, 404, request(http.MethodGet, account.ID, "").Code)
	}
	// Matching account scope plus read/delete permission does not grant network.
	principal.Policy.Rules[0].Permissions = []string{"accounts.read", "accounts.delete"}
	before := snapshot(allowed.ID)
	require.Equal(t, 403, request(http.MethodPut, allowed.ID, payload(first)).Code)
	require.Equal(t, before, snapshot(allowed.ID))
	readback(second, first)
	// A network-only key may write its own scope but cannot read accounts.
	principal.Policy.Rules[0].Permissions = []string{"accounts.network"}
	require.Equal(t, 200, request(http.MethodPut, allowed.ID, payload(first, second)).Code)
	require.Equal(t, 403, request(http.MethodGet, allowed.ID, "").Code)
	principal.Policy.Rules[0].Permissions = []string{"accounts.read", "accounts.delete", "accounts.network"}
	readback(first, second)
	require.Equal(t, 404, request(http.MethodDelete, foreign.ID, "").Code)
	require.Equal(t, 200, request(http.MethodDelete, allowed.ID, "").Code, "Push can clean up its paused preflight account")
	require.Equal(t, 404, request(http.MethodGet, allowed.ID, "").Code)
}

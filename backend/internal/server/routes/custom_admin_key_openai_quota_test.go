//go:build unit

package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

type scopedQuotaAccounts struct {
	service.AdminService
	accounts map[int64]*service.Account
	lookups  int
}

func (s *scopedQuotaAccounts) GetAccount(_ context.Context, id int64) (*service.Account, error) {
	s.lookups++
	if a := s.accounts[id]; a != nil {
		return a, nil
	}
	return nil, service.ErrAccountNotFound
}

func TestScopedAdminQuotaRegisteredRouteReadOnlyAndAccountScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, permission := range []string{"openai.quota.read", "accounts.read", "accounts.refresh", ""} {
		t.Run(permission, func(t *testing.T) {
			upstreamCalls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls++
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/backend-api/wham/usage", r.URL.Path)
				require.Equal(t, "Bearer PRIVATE_ACCESS", r.Header.Get("Authorization"))
				require.Equal(t, "fixture-workspace", r.Header.Get("ChatGPT-Account-ID"))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":12.5,"limit_window_seconds":18000}},"access_token":"PRIVATE_UPSTREAM"}`))
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
			api := &scopedQuotaAccounts{accounts: map[int64]*service.Account{}}
			for id := int64(1); id <= 4; id++ {
				// This intentionally expired, paused account models the delivery
				// guard before activation. Scheduling must not gate quota reads.
				expired := time.Now().Add(-time.Hour)
				api.accounts[id] = &service.Account{ID: id, Platform: "openai", Type: "oauth", Status: "active", GroupIDs: []int64{7},
					Schedulable: false, ExpiresAt: &expired, AutoPauseOnExpired: true,
					Credentials: map[string]any{"access_token": "PRIVATE_ACCESS", "refresh_token": "PRIVATE_REFRESH", "chatgpt_account_id": "fixture-workspace"}}
			}
			api.accounts[2].GroupIDs = []int64{8}
			api.accounts[3].Platform = "anthropic"
			api.accounts[4].Type = "apikey"
			keys := &scopedRouteKeys{}
			settings := service.NewSettingService(nil, nil)
			settings.SetScopedAdminKeyRepository(keys)
			permissions := []string{}
			if permission != "" {
				permissions = append(permissions, permission)
			}
			policy := adminaccess.Policy{Rules: []adminaccess.Rule{{Permissions: permissions, GroupIDs: []int64{7}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"}}}}
			key, _, err := settings.CreateScopedAdminKey(context.Background(), service.ScopedAdminKeyInput{Name: "额度只读验收", Enabled: true, Policy: policy}, 9)
			require.NoError(t, err)
			users := service.NewUserService(&scopedRouteUsers{}, nil, nil, nil)
			auth := middleware.NewAdminAuthMiddleware(nil, users, settings, nil)
			// Any mutation or TokenProvider call would panic: the read-only
			// query has no repository, refresh service or cache dependency.
			quota := service.NewOpenAIQuotaService(nil, nil, nil, factory, nil)
			oauth := adminhandler.NewOpenAIOAuthHandler(nil, api, quota, nil)
			router := gin.New()
			RegisterAdminRoutes(router.Group("/api/v1"), &handler.Handlers{Admin: &handler.AdminHandlers{OpenAIOAuth: oauth}}, auth,
				middleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() }), middleware.StepUpAuthMiddleware(func(c *gin.Context) { c.Next() }), nil, nil)
			for _, tc := range []struct {
				method, suffix string
				status         int
			}{
				{"GET", "/1/quota", 200}, {"GET", "/2/quota", 404}, {"GET", "/3/quota", 404}, {"GET", "/4/quota", 404},
				{"GET", "/999/quota", 404}, {"GET", "/0/quota", 400}, {"GET", "/-1/quota", 400}, {"GET", "/bad/quota", 400},
				{"POST", "/1/quota/refresh", 403}, {"POST", "/1/reset-quota", 403},
			} {
				request := httptest.NewRequest(tc.method, "/api/v1/admin/openai/accounts"+tc.suffix, nil)
				request.Header.Set("x-api-key", key)
				recorder := httptest.NewRecorder()
				beforeLookups := api.lookups
				router.ServeHTTP(recorder, request)
				want := tc.status
				if permission != "openai.quota.read" {
					want = 403
				}
				require.Equal(t, want, recorder.Code, tc.method+tc.suffix+": "+recorder.Body.String())
				require.NotContains(t, recorder.Body.String(), "PRIVATE")
				if want == 400 || want == 403 {
					require.Equal(t, beforeLookups, api.lookups)
				}
				if want == 200 {
					require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
					var result struct {
						Data struct {
							RateLimit struct {
								Allowed      bool `json:"allowed"`
								LimitReached bool `json:"limit_reached"`
							} `json:"rate_limit"`
						} `json:"data"`
					}
					require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
					require.True(t, result.Data.RateLimit.Allowed)
					require.False(t, result.Data.RateLimit.LimitReached)
				}
			}
			expectedCalls := 0
			if permission == "openai.quota.read" {
				expectedCalls = 1
			}
			require.Equal(t, expectedCalls, upstreamCalls)
			require.False(t, api.accounts[1].Schedulable)
		})
	}
}

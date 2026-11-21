package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

func readOnlyQuotaAccount() *Account {
	guardExpiry := time.Now().Add(-time.Hour)
	return &Account{ID: 101, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Schedulable: false, ExpiresAt: &guardExpiry, AutoPauseOnExpired: true, GroupIDs: []int64{7},
		Credentials: map[string]any{"access_token": "synthetic-access", "refresh_token": "synthetic-refresh", "chatgpt_account_id": "synthetic-workspace"}}
}

func readOnlyQuotaContext() context.Context {
	return adminaccess.WithPrincipal(context.Background(), &adminaccess.Principal{Policy: adminaccess.Policy{Rules: []adminaccess.Rule{{
		Permissions: []string{"openai.quota.read"}, GroupIDs: []int64{7}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"},
	}}}})
}

func TestOpenAIQuotaReadOnlyReturnsRealFlagsWithoutMutation(t *testing.T) {
	for _, flags := range []string{`"allowed":true,"limit_reached":false`, `"allowed":false,"limit_reached":true`, `"allowed":false,"limit_reached":false`} {
		t.Run(flags, func(t *testing.T) {
			account := readOnlyQuotaAccount()
			before, err := json.Marshal(account)
			require.NoError(t, err)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/backend-api/wham/usage", r.URL.Path)
				require.Equal(t, "Bearer synthetic-access", r.Header.Get("Authorization"))
				require.Equal(t, "synthetic-workspace", r.Header.Get("ChatGPT-Account-ID"))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"plan_type":"plus","rate_limit":{` + flags + `,"primary_window":{"used_percent":0,"limit_window_seconds":18000}},"access_token":"upstream-secret","refresh_token":"upstream-secret"}`))
			}))
			defer server.Close()
			// No repository/token-provider methods are available: accidental
			// snapshot writes, state recovery, token refresh or cache reads panic.
			svc := NewOpenAIQuotaService(nil, nil, &OpenAITokenProvider{}, newQuotaRedirectingFactory(server), nil)
			usage, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), account)
			require.NoError(t, err)
			require.Equal(t, 1, calls, "must not fetch/reset credits or retry upstream")
			require.False(t, usage.UsageCachePersisted)
			require.Positive(t, usage.FetchedAt)
			encoded, err := json.Marshal(usage)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "secret")
			require.NotContains(t, string(encoded), "token")
			after, err := json.Marshal(account)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after))
			var response map[string]any
			require.NoError(t, json.Unmarshal(encoded, &response))
			rate := response["rate_limit"].(map[string]any)
			require.IsType(t, true, rate["allowed"])
			require.IsType(t, true, rate["limit_reached"])
			require.Equal(t, strings.Contains(flags, `"allowed":true`), rate["allowed"])
			require.Equal(t, strings.Contains(flags, `"limit_reached":true`), rate["limit_reached"])
		})
	}
}

func TestOpenAIQuotaReadOnlyRejectsMissingOrNonBooleanFlags(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"rate_limit":null}`, `{"rate_limit":{}}`,
		`{"rate_limit":{"allowed":true}}`, `{"rate_limit":{"allowed":null,"limit_reached":false}}`,
		`{"rate_limit":{"allowed":"true","limit_reached":false}}`, `{"rate_limit":{"allowed":true,"limit_reached":0}}`,
		`{"rate_limit":{"allowed":true,"limit_reached":null}}`, `not-json`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			svc := NewOpenAIQuotaService(nil, nil, nil, newQuotaRedirectingFactory(server), nil)
			usage, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), readOnlyQuotaAccount())
			require.Nil(t, usage)
			require.Equal(t, 502, infraerrors.Code(err))
		})
	}
}

func TestOpenAIQuotaReadOnlyHidesUpstreamAndTransportErrors(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "synthetic-access synthetic-refresh private-proxy-password", status)
			}))
			defer server.Close()
			svc := NewOpenAIQuotaService(nil, nil, nil, newQuotaRedirectingFactory(server), nil)
			usage, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), readOnlyQuotaAccount())
			require.Nil(t, usage)
			require.Equal(t, mapUpstreamStatus(status), infraerrors.Code(err))
			require.NotContains(t, err.Error(), "synthetic")
			require.NotContains(t, err.Error(), "password")
		})
	}
	for _, factory := range []PrivacyClientFactory{
		func(string) (*req.Client, error) { return nil, errors.New("synthetic-access private-proxy-password") },
		func(string) (*req.Client, error) {
			return req.C().WrapRoundTripFunc(func(req.RoundTripper) req.RoundTripFunc {
				return func(*req.Request) (*req.Response, error) {
					return nil, errors.New("synthetic-access private-proxy-password")
				}
			}), nil
		},
	} {
		svc := NewOpenAIQuotaService(nil, nil, nil, factory, nil)
		_, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), readOnlyQuotaAccount())
		require.Equal(t, 502, infraerrors.Code(err))
		require.NotContains(t, err.Error(), "synthetic")
		require.NotContains(t, err.Error(), "password")
	}
}

func TestOpenAIQuotaReadOnlyDoesNotRefreshOrFallBackToDirect(t *testing.T) {
	for _, mode := range []string{"missing token", "missing account ID", "agent identity", "missing proxy", "disabled proxy", "expired proxy", "request pool"} {
		t.Run(mode, func(t *testing.T) {
			account := readOnlyQuotaAccount()
			proxyID := int64(11)
			switch mode {
			case "missing token":
				delete(account.Credentials, "access_token")
			case "missing account ID":
				delete(account.Credentials, "chatgpt_account_id")
			case "agent identity":
				account.Credentials["auth_mode"] = OpenAIAuthModeAgentIdentity
			case "missing proxy", "disabled proxy", "expired proxy":
				account.ProxyID = &proxyID
				if mode != "missing proxy" {
					account.Proxy = &Proxy{ID: proxyID, Status: StatusDisabled}
					if mode == "expired proxy" {
						expired := time.Now().Add(-time.Minute)
						account.Proxy.Status, account.Proxy.ExpiresAt = StatusActive, &expired
					}
				}
			case "request pool":
				account.Extra = map[string]any{OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11}}
			}
			svc := NewOpenAIQuotaService(nil, nil, &OpenAITokenProvider{}, func(string) (*req.Client, error) {
				t.Fatal("invalid local authentication/proxy must fail before network")
				return nil, nil
			}, nil)
			_, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), account)
			require.Error(t, err)
		})
	}
}

func TestOpenAIQuotaReadOnlyAuthorizesCredentialOwner(t *testing.T) {
	repo, owner, replica := proxyReplicaFixture()
	replica.GroupIDs, owner.GroupIDs = []int64{7}, []int64{8}
	svc := NewOpenAIQuotaService(repo, nil, nil, func(string) (*req.Client, error) {
		t.Fatal("out-of-scope credential owner must not be queried")
		return nil, nil
	}, nil)
	_, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), replica)
	require.Equal(t, 404, infraerrors.Code(err))
}

func TestOpenAIQuotaReadOnlyReplicaKeepsOwnProxyAndPausedOwner(t *testing.T) {
	repo, owner, replica := proxyReplicaFixture()
	replica.GroupIDs, owner.GroupIDs = []int64{7}, []int64{7}
	replica.Schedulable = false
	owner.Credentials["chatgpt_account_id"] = "owner-workspace"
	replica.Proxy.Protocol, replica.Proxy.Host, replica.Proxy.Port = "http", "bound-proxy.invalid", 8080
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer old", r.Header.Get("Authorization"))
		require.Equal(t, "owner-workspace", r.Header.Get("ChatGPT-Account-ID"))
		_, _ = w.Write([]byte(`{"rate_limit":{"allowed":true,"limit_reached":false}}`))
	}))
	defer server.Close()
	redirect := newQuotaRedirectingFactory(server)
	factory := func(proxyURL string) (*req.Client, error) {
		require.Equal(t, replica.Proxy.URL(), proxyURL)
		return redirect("")
	}
	svc := NewOpenAIQuotaService(repo, nil, &OpenAITokenProvider{}, factory, nil)
	usage, err := svc.QueryUsageReadOnly(readOnlyQuotaContext(), replica)
	require.NoError(t, err)
	require.True(t, usage.RateLimit.Allowed)
	require.False(t, owner.Schedulable)
	require.False(t, replica.Schedulable)
	require.Empty(t, replica.Credentials)
	require.Equal(t, "refresh", owner.GetOpenAIRefreshToken())
}

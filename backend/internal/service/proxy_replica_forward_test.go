//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProxyReplicaTwoProxiesSendWithSharedCredentials(t *testing.T) {
	repo, owner, first := proxyReplicaFixture()
	owner.Credentials["expires_at"] = time.Now().Add(time.Hour).Format(time.RFC3339)
	first.Proxy = proxyForTest(11, "first.invalid", 8080)
	first.Concurrency = 5
	second := first.CopySchedulerSnapshot()
	second.ID = 457
	second.Concurrency = 8
	pid := int64(12)
	second.ProxyID = &pid
	second.Proxy = proxyForTest(12, "second.invalid", 8080)
	repo.accounts[second.ID] = &second
	cache := &proxyReplicaTestCache{}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), &proxyReplicaTestExecutor{})
	calls := 0
	svc := &OpenAIGatewayService{accountRepo: repo, openAITokenProvider: provider, httpUpstream: &runtimeFallbackUpstream{do: func(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
		calls++
		require.Equal(t, "Bearer old", req.Header.Get("Authorization"))
		if id == first.ID {
			require.Equal(t, first.Proxy.URL(), proxy)
			require.Equal(t, 5, concurrency)
		} else {
			require.Equal(t, second.ID, id)
			require.Equal(t, second.Proxy.URL(), proxy)
			require.Equal(t, 8, concurrency)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`)), Request: req}, nil
	}}}
	for _, account := range []*Account{first, &second} {
		token, kind, err := svc.GetAccessToken(context.Background(), account)
		require.NoError(t, err)
		require.Equal(t, "oauth", kind)
		req, err := http.NewRequest(http.MethodPost, "https://example.invalid/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := svc.doUpstreamWithProxyFallback(context.Background(), req, account, account.Proxy.URL())
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}
	require.Equal(t, 2, calls)
	require.Empty(t, first.GetCredential("access_token"))
	require.Empty(t, second.GetCredential("access_token"))
}

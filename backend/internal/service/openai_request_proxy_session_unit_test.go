//go:build unit

package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRequestProxyAffinityUsesStableContentPrefixFallback(t *testing.T) {
	account := &Account{ID: 3901, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11, 22}, OpenAIProxySessionAffinityKey: true,
	}}
	makeContext := func(apiKeyID int64) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		c.Set("api_key", &APIKey{ID: apiKeyID})
		return c
	}
	firstTurn := []byte(`{"model":"gpt-6.1-sol","messages":[{"role":"system","content":"Be concise."},{"role":"user","content":"Hello"}]}`)
	continuedTurn := []byte(`{"model":"gpt-6.1-sol","messages":[{"role":"system","content":"Be concise."},{"role":"user","content":"Hello"},{"role":"assistant","content":"Hi"},{"role":"user","content":"Follow up"}]}`)
	differentConversation := []byte(`{"model":"gpt-6.1-sol","messages":[{"role":"system","content":"Be concise."},{"role":"user","content":"Different opening"}]}`)

	firstContext := makeContext(100)
	first := withOpenAIRequestProxySession(firstContext.Request.Context(), firstContext, account, firstTurn).Value(openAIRequestProxySessionKey{})
	continuedContext := makeContext(100)
	continued := withOpenAIRequestProxySession(continuedContext.Request.Context(), continuedContext, account, continuedTurn).Value(openAIRequestProxySessionKey{})
	require.NotNil(t, first)
	require.Equal(t, first, continued, "the existing stable-prefix seed should survive later turns")

	differentContext := makeContext(100)
	different := withOpenAIRequestProxySession(differentContext.Request.Context(), differentContext, account, differentConversation).Value(openAIRequestProxySessionKey{})
	require.NotEqual(t, first, different)

	otherKeyContext := makeContext(101)
	otherKey := withOpenAIRequestProxySession(otherKeyContext.Request.Context(), otherKeyContext, account, firstTurn).Value(openAIRequestProxySessionKey{})
	require.NotEqual(t, first, otherKey, "content seeds remain isolated by API key")

	disabledAccount := &Account{ID: account.ID, Platform: account.Platform, Type: account.Type,
		Extra: map[string]any{OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11, 22}}}
	disabledContext := makeContext(100)
	disabled := withOpenAIRequestProxySession(disabledContext.Request.Context(), disabledContext, disabledAccount, firstTurn)
	require.Nil(t, disabled.Value(openAIRequestProxySessionKey{}), "content fallback is opt-in")
}

func TestOpenAIRequestProxyAffinityDoesNotInventSeedWithoutStablePrefix(t *testing.T) {
	account := &Account{ID: 3901, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11}, OpenAIProxySessionAffinityKey: true,
	}}
	for _, body := range [][]byte{
		[]byte(`{}`),
		[]byte(`{"model":"gpt-6.1-sol"}`),
		[]byte(`{"model":"gpt-6.1-sol","messages":[]}`),
		[]byte(`{"model":5,"messages":"invalid"}`),
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		c.Set("api_key", &APIKey{ID: 100})
		ctx := withOpenAIRequestProxySession(c.Request.Context(), c, account, body)
		require.Nil(t, ctx.Value(openAIRequestProxySessionKey{}))
	}
}

func TestOpenAIRequestProxyAffinityReusesLegacyCodexThread(t *testing.T) {
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11, 22},
		OpenAIProxySessionAffinityKey: true,
	}}
	scope := func(t *testing.T, headers map[string]string, body, source string) any {
		t.Helper()
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		for name, value := range headers {
			c.Request.Header.Set(name, value)
		}
		c.Set("api_key", &APIKey{ID: 100})
		collector := requesttiming.New(time.Now(), int64(len(body)))
		ctx := requesttiming.With(c.Request.Context(), collector)
		resolved := withOpenAIRequestProxySession(ctx, c, account, []byte(body))
		collector.Finish(http.StatusOK, false)
		collector.WhenFinished(func(snapshot requesttiming.Snapshot) {
			require.Contains(t, snapshot.Events, "request_proxy_affinity_"+source)
		})
		return resolved.Value(openAIRequestProxySessionKey{})
	}

	explicit := scope(t, map[string]string{"thread-id": "thread-one"}, `{"input":"first turn"}`, "declared")
	require.NotNil(t, explicit)
	require.Equal(t, explicit, scope(t, map[string]string{openAIWSTurnMetadataHeader: `{"thread_id":"thread-one"}`},
		`{"input":"continued turn"}`, "codex_thread"))
	require.Equal(t, explicit, scope(t, nil,
		`{"client_metadata":{"x-codex-turn-metadata":"{\"thread_id\":\"thread-one\"}"},"input":"compacted history"}`, "codex_thread"))
	require.Equal(t, explicit, scope(t, map[string]string{openAIWSWindowIDHeader: "thread-one:1"},
		`{"input":"another turn"}`, "codex_thread"))
	require.NotEqual(t, explicit, scope(t, map[string]string{openAIWSTurnMetadataHeader: `{"thread_id":"thread-two"}`},
		`{"input":"first turn"}`, "codex_thread"))

	legacyThread := map[string]string{openAIWSTurnMetadataHeader: `{"thread_id":"thread-one"}`}
	require.Equal(t, explicit, scope(t, legacyThread, `{"prompt_cache_key":"cache-hint","input":"first turn"}`, "codex_thread"))
	require.Equal(t, explicit, scope(t, legacyThread, `{"prompt_cache_key":"other-hint","input":"follow up"}`, "codex_thread"))

	hintBody := `{"prompt_cache_key":"cache-hint","input":"first turn"}`
	hintScope := scope(t, nil, hintBody, "prompt_cache_key")
	require.NotNil(t, hintScope)
	require.Equal(t, hintScope, scope(t, map[string]string{
		openCodeSessionAffinityHeader: "cache-hint", openAIWSTurnMetadataHeader: `{"thread_id":"thread-one"}`,
	}, hintBody, "client_hint"))
	require.Nil(t, scope(t, nil, `{"prompt_cache_key":{"not":"a string"}}`, "missing"))
	require.Nil(t, scope(t, map[string]string{
		"thread-id": "thread-one", openAIWSTurnMetadataHeader: `{"thread_id":"thread-one"}`,
	}, `{"client_metadata":{"thread_id":"thread-two"},"input":"first turn"}`, "invalid"))
}

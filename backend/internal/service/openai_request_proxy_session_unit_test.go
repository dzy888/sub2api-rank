//go:build unit

package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

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

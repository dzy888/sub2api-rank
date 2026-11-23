package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIStickyIdentitySurvivesChangedTurnContent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headers  map[string]string
		metadata map[string]string
		envelope bool
	}{
		{name: "thread_header", headers: map[string]string{"thread-id": "stable-thread"}},
		{name: "underscore_thread_header", headers: map[string]string{"thread_id": "stable-thread"}},
		{name: "body_thread", metadata: map[string]string{"thread_id": "stable-thread"}},
		{name: "body_session", metadata: map[string]string{"session_id": "stable-thread"}},
		{name: "enveloped_body_thread", metadata: map[string]string{"thread_id": "stable-thread"}, envelope: true},
		{name: "turn_metadata_header", headers: map[string]string{"x-codex-turn-metadata": `{"thread_id":"stable-thread","request_kind":"turn"}`}},
		{name: "turn_metadata_body", metadata: map[string]string{"x-codex-turn-metadata": `{"thread_id":"stable-thread","request_kind":"turn"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStickyEscapeRebindFixture()
			f.recoverOriginalAccount()
			var hashes []string
			for _, input := range []string{"first request", "compacted continuation"} {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				for name, value := range tc.headers {
					c.Request.Header.Set(name, value)
				}
				request := map[string]any{"model": "gpt-5.1", "input": input}
				if tc.metadata != nil {
					request["client_metadata"] = tc.metadata
				}
				if tc.envelope {
					request = map[string]any{"type": "response.create", "response": request}
				}
				body, err := json.Marshal(request)
				require.NoError(t, err)
				hash := f.service.GenerateSessionHash(c, body)
				require.NotEmpty(t, hash)
				require.Equal(t, hash, c.GetString("openai_auth_connection_session_hash"))
				require.Empty(t, f.service.ExtractSessionID(c, body), "routing-only identity must not become an upstream prompt cache key")
				hashes = append(hashes, hash)
				if len(hashes) == 1 {
					require.NoError(t, f.service.BindStickySession(c.Request.Context(), f.request.GroupID, hash, f.accounts[0].ID))
					continue
				}
				f.request.SessionHash = hash
				selected, decision, err := f.scheduler.Select(c.Request.Context(), f.request)
				require.NoError(t, err)
				defer releaseStickyEscapeRebindSelection(selected)
				require.NotNil(t, selected)
				require.Equal(t, f.accounts[0].ID, selected.Account.ID)
				require.True(t, decision.StickySessionHit, "changed content must find the original account binding")
			}
			require.Equal(t, hashes[0], hashes[1])
		})
	}
}

func TestOpenAIStickyIdentityOverrideUpdatesLegacyAndConnectionAffinity(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	svc := &OpenAIGatewayService{}
	_ = svc.GenerateSessionHash(c, []byte(`{"model":"gpt-5.1","messages":[{"role":"user","content":"first turn"}]}`))
	oldLegacy := openAILegacySessionHashFromContext(c.Request.Context())
	newHash := DeriveSessionHashFromSeedWithContext(c, "claude-metadata-session")
	_, wantLegacy := deriveOpenAISessionHashes("claude-metadata-session")
	require.NotEqual(t, oldLegacy, wantLegacy)
	require.Equal(t, wantLegacy, openAILegacySessionHashFromContext(c.Request.Context()))
	require.Equal(t, newHash, c.GetString("openai_auth_connection_session_hash"))
}

func TestOpenAIStickyIdentityRejectsConflictingMetadata(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("thread-id", "header-thread")
	svc := &OpenAIGatewayService{}
	for _, body := range []string{
		`{"model":"gpt-5.1","input":"hello","client_metadata":{"thread_id":"different-thread"}}`,
		`{"model":"gpt-5.1","input":"hello","client_metadata":{"thread_id":123}}`,
	} {
		require.Equal(t, DeriveSessionHashFromSeed(deriveOpenAIContentSessionSeed([]byte(body))), svc.GenerateSessionHash(c, []byte(body)))
	}
	first := []byte(`{"client_metadata":{"thread_id":"thread-one"},"input":"same input"}`)
	second := []byte(`{"client_metadata":{"thread_id":"thread-two"},"input":"same input"}`)
	c.Request.Header.Del("thread-id")
	require.NotEqual(t, svc.GenerateSessionHash(c, first), svc.GenerateSessionHash(c, second), "different threads must not collapse onto the content fallback")
	c.Set("api_key", &APIKey{Group: &Group{Platform: PlatformGrok}})
	require.Equal(t, svc.GenerateSessionHash(c, first), svc.GenerateSessionHash(c, second), "Grok keeps its own affinity rules")
}

func TestOpenAIStickyIdentityDoesNotRewriteExplicitCacheSignal(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("thread-id", "stable-thread")
	svc := &OpenAIGatewayService{}
	body := []byte(`{"model":"gpt-5.1","prompt_cache_key":"client-cache","client_metadata":{"thread_id":"stable-thread"},"input":"hello"}`)
	require.Equal(t, DeriveSessionHashFromSeed("client-cache"), svc.GenerateSessionHash(c, body))
	require.Equal(t, "client-cache", svc.ExtractSessionID(c, body))
	c.Request.Header.Set("session-id", "existing-session")
	require.Equal(t, DeriveSessionHashFromSeed("existing-session"), svc.GenerateSessionHash(c, body))
}

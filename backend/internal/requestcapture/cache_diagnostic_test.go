package requestcapture

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestCacheDiagnosticRecordsMarkedRequestAndFinalWire(t *testing.T) {
	now := time.Now()
	dir := filepath.Join(t.TempDir(), "private")
	config := cacheDiagnosticConfig{Directory: dir, Marker: "SUB2API-CACHE-TEST123", APIKeyID: 17, AccountID: 39, Until: now.Add(time.Minute)}
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	d := newCacheDiagnostic(string(encoded), now)
	require.NotNil(t, d)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	req = req.WithContext(context.WithValue(req.Context(), ctxkey.ClientRequestID, "test-request"))
	req.Header.Set("Authorization", "Bearer example-credential")
	req.Header.Set("Session-Id", "client-session")
	ingress := []byte(`{"model":"gpt-6.1-sol","input":[{"role":"user","content":"hello SUB2API-CACHE-TEST123"}],"prompt_cache_key":"client-session"}`)
	otherRole := bytes.Replace(ingress, []byte(`"user"`), []byte(`"assistant"`), 1)
	require.Equal(t, req.Context(), d.begin(req, 17, otherRole, now))
	require.Equal(t, req.Context(), d.begin(req, 18, ingress, now))
	require.Equal(t, req.Context(), d.begin(req, 17, ingress, config.Until))
	ctx := d.begin(req, 17, ingress, now)
	upstream := bytes.ReplaceAll(ingress, []byte("client-session"), []byte("scoped-session"))
	wire, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.invalid/responses", bytes.NewReader(upstream))
	require.NoError(t, err)
	wire.Header.Set("session_id", "scoped-session")
	ObserveCacheDiagnosticRequest(wire, 40)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	ObserveCacheDiagnosticRequest(wire, 39)
	var record string
	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if entry.Name()[0] != '.' {
				record = filepath.Join(dir, entry.Name())
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	for filename, expected := range map[string][]byte{"ingress.json": ingress, "upstream.json": upstream} {
		p := filepath.Join(record, filename)
		got, err := os.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, got)
		info, err := os.Stat(p)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
	meta, err := os.ReadFile(filepath.Join(record, "metadata.json"))
	require.NoError(t, err)
	require.NotContains(t, string(meta), "example-credential")
	require.NotContains(t, string(meta), "client-session")
	require.NotContains(t, string(meta), "scoped-session")
	var decoded cacheDiagnosticMeta
	require.NoError(t, json.Unmarshal(meta, &decoded))
	require.Equal(t, "test-request", decoded.RequestID)
	require.Equal(t, decoded.Input.Fingerprints["session-id"], decoded.Input.Fingerprints["prompt_cache_key"])
	require.Equal(t, decoded.Upstream.Fingerprints["session_id"], decoded.Upstream.Fingerprints["prompt_cache_key"])
	require.NotEqual(t, decoded.Input.Fingerprints["prompt_cache_key"], decoded.Upstream.Fingerprints["prompt_cache_key"])
}

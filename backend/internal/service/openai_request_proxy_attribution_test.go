//go:build bps_request_integration

package service

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRequestProxySuccessTracesSelectedExit(t *testing.T) {
	observed := make(chan int, 1)
	svc, account := localBPSRequestProxyFixture(t, 2, func(id int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		observed <- id
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, bpsRequestCompletedWire)
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{2}
	body := []byte(`{"model":"gpt-5.4","input":"local fixture","stream":true}`)
	collector := requesttiming.New(time.Now(), int64(len(body)))
	ctx := requesttiming.With(t.Context(), collector)
	code, _, err := forwardLocalBPSRequest(ctx, svc, account, body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, 2, <-observed)
	collector.Finish(code, false)
	collector.WhenFinished(func(snapshot requesttiming.Snapshot) {
		require.Len(t, snapshot.Attempts, 1)
		require.Equal(t, int64(2), snapshot.Attempts[0].ProxyID)
		require.Equal(t, http.StatusOK, snapshot.Attempts[0].Status)
	})
}

// Exercise the actual Forward failure path: URL-only selection must override
// an unrelated legacy binding and must never persist the credential-bearing URL.
func TestOpenAIRequestProxyFailureAttributesSelectedExit(t *testing.T) {
	svc, account := localBPSRequestProxyFixture(t, 1, func(_ int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"local fixture limited"}}`)
	})
	oldID := int64(999)
	account.ProxyID = &oldID
	account.Proxy = &Proxy{ID: oldID, Name: "old-single", Protocol: "http", Host: "unused.invalid", Port: 8080}
	body := []byte(`{"model":"gpt-6-astra","input":"local fixture","stream":true}`)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	_, err := svc.Forward(t.Context(), c, account, body)
	require.Error(t, err)
	value, exists := c.Get(OpsUpstreamErrorsKey)
	require.True(t, exists)
	events := value.([]*OpsUpstreamErrorEvent)
	require.NotEmpty(t, events)
	for _, event := range events {
		if event.AccountID != account.ID {
			continue
		}
		require.NotNil(t, event.ProxyID)
		require.Equal(t, int64(1), *event.ProxyID)
		require.Equal(t, opsProxyNameUnnamed, event.ProxyName)
	}
	encoded, err := json.Marshal(events)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "old-single")
	require.NotContains(t, string(encoded), "127.0.0.1")
}

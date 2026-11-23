//go:build bps_request_integration

package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type requestProxyAccountTestRepo struct {
	AccountRepository
	account *Account
}
type requestProxyAccountTestUpstream struct{ HTTPUpstream }

func (u *requestProxyAccountTestUpstream) DoWithTLS(r *http.Request, p string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.HTTPUpstream.Do(r, p, id, n)
}

func (r *requestProxyAccountTestRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}
func (r *requestProxyAccountTestRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

func TestOpenAIRequestProxyAccountTestUsesSelectedExit(t *testing.T) {
	observed := make(chan int, 1)
	gateway, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		observed <- id
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, bpsRequestCompletedWire)
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{2}
	svc := &AccountTestService{accountRepo: &requestProxyAccountTestRepo{account: account}, openaiGatewayService: gateway, httpUpstream: &requestProxyAccountTestUpstream{gateway.httpUpstream}}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/admin/accounts/300/test", nil)
	err := svc.TestAccountConnection(c, account.ID, "gpt-5.4", "", "")
	require.NoError(t, err)
	require.Contains(t, rec.Body.String(), "test_complete")
	require.Equal(t, 2, <-observed)
}

func TestOpenAIRequestProxyImagesUsesSelectedExit(t *testing.T) {
	observed := make(chan int, 1)
	svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		observed <- id
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"image_generation_call\",\"result\":\"aW1hZ2U=\"}]}}\n\n")
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{2}
	body := []byte(`{"model":"gpt-image-1","prompt":"local fixture"}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)
	result, err := svc.ForwardImages(t.Context(), c, account, body, parsed, "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.ImageCount)
	require.Equal(t, 2, <-observed)
}

func TestOpenAIRequestProxyImagesEndpointFallbackKeepsLogicalExit(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			observed := make(chan requestProxyObservation, 2)
			svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				observed <- requestProxyObservation{id: id, path: r.URL.Path}
				if r.URL.Path == "/backend-api/codex/images/generations" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"error":{"message":"fixture endpoint unavailable"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"image_generation_call\",\"result\":\"aW1hZ2U=\"}],\"usage\":{\"input_tokens\":1,\"output_tokens\":20}}}\n\n")
			})
			account.Extra["openai_excel_bps"] = false
			account.Extra["openai_proxy_mode"] = "request"
			account.Extra["openai_proxy_ids"] = []int64{1, 2, 3}
			body := []byte(`{"model":"gpt-image-2","prompt":"local fallback fixture"}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			parsed, err := svc.ParseOpenAIImagesRequest(c, body)
			require.NoError(t, err)
			// Images must finish after a downstream disconnect, including an
			// endpoint fallback, so billing can retain completed output usage.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			result, err := svc.ForwardImages(ctx, c, account, body, parsed, "")
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, 1, result.ImageCount)
			require.Equal(t, 20, result.Usage.OutputTokens)
			direct, fallback := <-observed, <-observed
			require.Equal(t, "/backend-api/codex/images/generations", direct.path)
			require.Equal(t, "/backend-api/codex/responses", fallback.path)
			require.Equal(t, direct.id, fallback.id, "one Images operation must keep its selected exit across direct endpoint fallback")
		})
	}
}

func TestOpenAIRequestProxyForwardSameSessionDistributesSixThroughThree(t *testing.T) {
	arrived := make(chan int, 6)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	svc, account := localBPSRequestProxyFixture(t, 4, func(id int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		arrived <- id
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, bpsRequestCompletedWire)
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{1, 2, 3}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 6)
	for i := 0; i < 6; i++ {
		go func() {
			code, response, err := forwardLocalBPSRequest(ctx, svc, account, []byte(`{"model":"gpt-5.4","input":"test","stream":true,"prompt_cache_key":"same-session"}`))
			if err == nil && (code != 200 || !bytes.Contains([]byte(response), []byte("response.completed"))) {
				err = fmt.Errorf("incomplete OAuth response: status %d: %s", code, response)
			}
			done <- err
		}()
	}
	hits := map[int]int{}
	for i := 0; i < 6; i++ {
		select {
		case id := <-arrived:
			hits[id]++
		case <-ctx.Done():
			t.Fatal("six OAuth requests did not reach selected local proxies")
		}
	}
	once.Do(func() { close(release) })
	for i := 0; i < 6; i++ {
		require.NoError(t, <-done)
	}
	require.Equal(t, map[int]int{1: 2, 2: 2, 3: 2}, hits)
}

func TestOpenAIRequestProxyForwardSessionAffinityPinsAuthenticatedResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	observed := make(chan int64, 1)
	svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case observed <- int64(id):
		default:
			t.Error("response was forwarded more than once")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, bpsRequestCompletedWire)
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{1, 2, 3}
	account.Extra[OpenAIProxySessionAffinityKey] = true

	forward := func(apiKeyID int64, threadID, input, cacheHint string, legacy bool) int64 {
		t.Helper()
		body := []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":%q,"stream":true,"prompt_cache_key":%q}`, input, cacheHint))
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		if legacy {
			c.Request.Header.Set(openAIWSTurnMetadataHeader, fmt.Sprintf(`{"thread_id":%q}`, threadID))
		} else {
			c.Request.Header.Set("thread-id", threadID)
		}
		c.Set("api_key", &APIKey{ID: apiKeyID})
		requestAccount := *account
		requestAccount.Extra = cloneExcelBPSRecoveryExtra(account.Extra)
		requestAccount.Credentials = cloneExcelBPSRecoveryExtra(account.Credentials)
		result, err := svc.Forward(t.Context(), c, &requestAccount, body)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "response.completed")
		select {
		case id := <-observed:
			return id
		default:
			t.Fatal("response did not pass through a selected proxy")
			return 0
		}
	}

	first := forward(100, "thread-a", "first turn", "initial-hint", false)
	second := forward(100, "thread-a", "compacted continuation", "changing-hint", true)
	otherSession := forward(100, "thread-b", "independent session", "changing-hint", true)
	otherKey := forward(101, "thread-a", "same thread, another key", "changing-hint", true)
	nextTurn := forward(100, "thread-a", "another continuation", "third-hint", true)
	require.Equal(t, first, second)
	require.NotEqual(t, first, otherSession, "new sessions should spread over available exits")
	require.NotEqual(t, otherSession, otherKey, "API keys must not share a session binding")
	require.NotEqual(t, first, otherKey, "another API key must receive an independent binding")
	require.Equal(t, first, nextTurn)
}

func TestOpenAIRequestProxyNativeWebSocketPinsOneExitForItsSession(t *testing.T) {
	svc, account := localBPSRequestProxyFixture(t, 3, func(int, http.ResponseWriter, *http.Request) {
		t.Error("native WebSocket unexpectedly forwarded an HTTP request")
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra[OpenAIProxyModeKey] = OpenAIProxyModeRequest
	account.Extra[OpenAIProxyIDsKey] = []int64{1, 2, 3}
	account.Extra[OpenAIProxySessionAffinityKey] = true
	account.Extra["openai_passthrough"] = false
	account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	upstream := &openAIWSCaptureConn{events: [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_first","model":"gpt-5.4","usage":{"input_tokens":100,"output_tokens":2}}}`),
		[]byte(`{"type":"response.completed","response":{"id":"resp_second","model":"gpt-5.4","usage":{"input_tokens":104,"input_tokens_details":{"cached_tokens":100},"output_tokens":2}}}`),
	}}
	dialer := &wsSSETestDialer{conn: upstream}
	cfg := newOpenAIWSV2TestConfig()
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 3
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 3
	svc.cfg = cfg
	svc.cache = &stubGatewayCache{}
	svc.toolCorrector = NewCodexToolCorrector()
	svc.openaiWSResolver = NewOpenAIWSProtocolResolver(cfg)
	svc.openaiWSPool = newOpenAIWSConnPool(cfg)
	svc.openaiWSPool.setClientDialerForTest(dialer)
	t.Cleanup(svc.openaiWSPool.Close)

	expectedRoute := &Proxy{Protocol: "http", Host: "unused.invalid", Port: 8080}
	account.ProxyID, account.Proxy = new(int64(90)), expectedRoute
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		defer conn.CloseNow()
		_, first, err := conn.Read(r.Context())
		if err != nil {
			done <- err
			return
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		c.Set("api_key", &APIKey{ID: 100})
		done <- svc.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "test-token", first, nil)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), &coderws.DialOptions{
		HTTPHeader: http.Header{"Thread-Id": {"thread-one"}},
	})
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","input":"first"}`)))
	_, first, err := client.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(first), "resp_first")
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp_first","input":"next"}`)))
	_, second, err := client.Read(ctx)
	require.NoError(t, err)
	require.Contains(t, string(second), "resp_second")
	require.Contains(t, string(second), `"cached_tokens":100`)
	require.NoError(t, client.CloseNow())
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("WebSocket ingress did not release its connection")
	}
	dialer.mu.Lock()
	require.Len(t, dialer.proxyURLs, 1)
	require.NotEmpty(t, dialer.proxyURLs[0])
	require.NotEqual(t, expectedRoute.URL(), dialer.proxyURLs[0])
	dialer.mu.Unlock()
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	require.Len(t, upstream.writes, 2)
	require.Equal(t, "resp_first", upstream.writes[1]["previous_response_id"])
}

func TestOpenAIRequestProxyWSSSEReusesLeasePerSessionAndDistributesNewSessions(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
		t.Errorf("WebSocket routing unexpectedly sent HTTP through proxy %d", id)
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra[OpenAIProxyModeKey] = OpenAIProxyModeRequest
	account.Extra[OpenAIProxyIDsKey] = []int64{1, 2, 3}
	account.Extra[OpenAIProxySessionAffinityKey] = true
	account.Extra["openai_passthrough"] = false
	account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = OpenAIWSIngressModeCtxPool
	account.Extra["openai_oauth_ws_sse_acceleration"] = true

	cfg := newOpenAIWSV2TestConfig()
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 3
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 3
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	dialer := &wsSSETestDialer{}
	var responseID atomic.Int32
	var firstConn *openAIWSCaptureConn
	dialer.connFactory = func() openAIWSClientConn {
		responses := 1
		if responseID.Add(1) == 1 {
			responses = 2
		}
		events := make([][]byte, 0, responses*3)
		for i := 0; i < responses; i++ {
			id := fmt.Sprintf("resp_proxy_%d_%d", responseID.Load(), i)
			events = append(events,
				[]byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"model":"gpt-5.4"}}`, id)),
				[]byte(`{"type":"response.output_text.delta","delta":"ok"}`),
				[]byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-5.4","usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":90},"output_tokens":2}}}`, id)),
			)
		}
		conn := &openAIWSCaptureConn{events: events}
		if responses == 2 {
			firstConn = conn
		}
		return conn
	}
	pool.setClientDialerForTest(dialer)
	svc.cfg = cfg
	svc.openaiWSResolver = NewOpenAIWSProtocolResolver(cfg)
	svc.openaiWSPool = pool
	svc.cache = &stubGatewayCache{}
	svc.toolCorrector = NewCodexToolCorrector()

	forward := func(thread, cacheKey, input string) {
		t.Helper()
		body := []byte(fmt.Sprintf(`{"model":"gpt-5.1","stream":true,"prompt_cache_key":%q,"input":[{"role":"user","content":"stable prefix"}, {"role":"user","content":%q}]}`, cacheKey, input))
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		c.Request.Header.Set("thread-id", thread)
		SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
		c.Set("api_key", &APIKey{ID: 100})
		result, err := svc.Forward(t.Context(), c, account, body)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, result.OpenAIWSMode)
		require.Equal(t, 90, result.Usage.CacheReadInputTokens)
		require.Contains(t, rec.Body.String(), "response.completed")
		require.Equal(t, "oauth_http_sse_acceleration", c.GetString("openai_ws_transport_reason"))
	}

	forward("thread-a", "stable-cache-key", "first turn")
	forward("thread-a", "stable-cache-key", "continued turn")
	require.EqualValues(t, 1, dialer.calls.Load(), "same session should reuse its WS connection and leased exit")
	dialer.mu.Lock()
	firstExit := dialer.proxyURLs[0]
	dialer.mu.Unlock()
	require.NotEmpty(t, firstExit)

	forward("thread-b", "another-cache-key", "independent conversation")
	require.EqualValues(t, 2, dialer.calls.Load(), "a new session may establish a connection on another selected exit")
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	require.NotEqual(t, firstExit, dialer.proxyURLs[1])
	require.NotNil(t, firstConn)
	firstConn.mu.Lock()
	defer firstConn.mu.Unlock()
	require.Len(t, firstConn.writes, 2)
	require.NotEmpty(t, firstConn.writes[0]["prompt_cache_key"])
	require.Equal(t, firstConn.writes[0]["prompt_cache_key"], firstConn.writes[1]["prompt_cache_key"])
	firstInput := firstConn.writes[0]["input"].([]any)
	secondInput := firstConn.writes[1]["input"].([]any)
	require.Equal(t, firstInput[0], secondInput[0], "the reusable request prefix must survive consecutive sends")
}

func TestOpenAIRequestProxyWSSSEHandshakeFallsBackThroughLeasedExit(t *testing.T) {
	httpExits := make(chan string, 1)
	fixture, account := localBPSRequestProxyFixture(t, 2, func(_ int, w http.ResponseWriter, r *http.Request) {
		httpExits <- "http://" + r.Context().Value(http.LocalAddrContextKey).(net.Addr).String()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, bpsRequestCompletedWire)
	})
	account.Extra = wsSSETestAccount().Extra
	account.Extra[OpenAIProxyModeKey] = OpenAIProxyModeRequest
	account.Extra[OpenAIProxyIDsKey] = []int64{1, 2}
	account.Extra[OpenAIProxySessionAffinityKey] = true
	dialer := &wsSSETestDialer{status: http.StatusUpgradeRequired, err: errors.New("test handshake refusal")}
	svc := wsSSETestService(t, dialer, fixture.httpUpstream)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("Thread-Id", "fallback-thread")
	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
	c.Set("api_key", &APIKey{ID: 100})

	result, err := svc.Forward(t.Context(), c, account, []byte(wsSSETestRequest))
	require.NoError(t, err)
	require.False(t, result.OpenAIWSMode)
	require.Equal(t, "oauth_ws_sse_handshake_fallback", c.GetString("openai_ws_transport_reason"))
	select {
	case exit := <-httpExits:
		dialer.mu.Lock()
		defer dialer.mu.Unlock()
		require.Equal(t, []string{exit}, dialer.proxyURLs)
	default:
		t.Fatal("HTTP fallback did not use a selected proxy")
	}
}

func TestOpenAIRequestProxyForwardCancellationReleasesExitWhileOtherStreamsStayBusy(t *testing.T) {
	arrived := make(chan int, 4)
	svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\"}}\n\n")
		w.(http.Flusher).Flush()
		arrived <- id
		<-r.Context().Done()
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{1, 2, 3}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	type running struct {
		stop  context.CancelFunc
		done  chan error
		proxy int
	}
	start := func() running {
		child, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			_, _, err := forwardLocalBPSRequest(child, svc, account, []byte(`{"model":"gpt-5.4","input":"hold","stream":true}`))
			done <- err
		}()
		select {
		case id := <-arrived:
			return running{stop, done, id}
		case <-ctx.Done():
			stop()
			t.Fatal("OAuth stream did not start")
			return running{}
		}
	}
	a, b, c := start(), start(), start()
	t.Cleanup(a.stop)
	t.Cleanup(b.stop)
	t.Cleanup(c.stop)
	require.ElementsMatch(t, []int{1, 2, 3}, []int{a.proxy, b.proxy, c.proxy})
	a.stop()
	select {
	case <-a.done:
	case <-ctx.Done():
		t.Fatal("canceled OAuth stream did not release")
	}
	d := start()
	t.Cleanup(d.stop)
	require.Equal(t, a.proxy, d.proxy)
	for _, r := range []running{b, c, d} {
		r.stop()
		select {
		case <-r.done:
		case <-ctx.Done():
			t.Fatal("held OAuth stream did not finish")
		}
	}
}

func TestOpenAIRequestProxyForwardFinishedRequestReleasesExit(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprint("rejected=", rejected), func(t *testing.T) {
			arrived := make(chan int, 4)
			var mu sync.Mutex
			calls := 0
			svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				mu.Lock()
				calls++
				order := calls
				mu.Unlock()
				arrived <- id
				if order <= 2 {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"held\",\"status\":\"in_progress\"}}\n\n")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				if rejected && order == 3 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":"bad_request","message":"fixture rejected"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, bpsRequestCompletedWire)
			})
			account.Extra["openai_excel_bps"] = false
			account.Extra["openai_proxy_mode"] = "request"
			account.Extra["openai_proxy_ids"] = []int64{1, 2, 3}
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			body := []byte(`{"model":"gpt-5.4","input":"test","stream":true}`)
			done := make(chan error, 2)
			for i := 0; i < 2; i++ {
				go func() { _, _, err := forwardLocalBPSRequest(ctx, svc, account, body); done <- err }()
				select {
				case <-arrived:
				case <-ctx.Done():
					t.Fatal("held OAuth request did not start")
				}
			}
			code, _, err := forwardLocalBPSRequest(ctx, svc, account, body)
			if rejected {
				require.Error(t, err)
				require.Equal(t, http.StatusBadRequest, code)
			} else {
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, code)
			}
			finished := <-arrived
			code, _, err = forwardLocalBPSRequest(ctx, svc, account, body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, code)
			require.Equal(t, finished, <-arrived, "finished request must release its least-in-flight slot while other exits remain busy")
			cancel()
			for i := 0; i < 2; i++ {
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("held OAuth request did not finish")
				}
			}
		})
	}
}

func TestOpenAIRequestProxyCompatibilityUsesSelectedExit(t *testing.T) {
	for _, entry := range []string{"native", "passthrough", "chat", "messages"} {
		t.Run(entry, func(t *testing.T) {
			observed := make(chan int, 1)
			svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				observed <- id
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, bpsRequestCompletedWire)
			})
			account.Extra["openai_excel_bps"] = false
			account.Extra["openai_passthrough"] = entry == "passthrough"
			account.Extra["openai_proxy_mode"] = "request"
			account.Extra["openai_proxy_ids"] = []int64{2}
			body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"test"}],"input":"test","max_tokens":32,"stream":true}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/"+entry, bytes.NewReader(body))
			var err error
			switch entry {
			case "chat":
				_, err = svc.ForwardAsChatCompletions(t.Context(), c, account, body, "", "")
			case "messages":
				_, err = svc.ForwardAsAnthropic(t.Context(), c, account, body, "", "")
			default:
				_, err = svc.Forward(t.Context(), c, account, body)
			}
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, 2, <-observed)
		})
	}
}

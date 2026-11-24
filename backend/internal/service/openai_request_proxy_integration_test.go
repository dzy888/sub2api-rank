//go:build bps_request_integration

package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
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
	svc, account := localBPSRequestProxyFixture(t, 3, func(id int, w http.ResponseWriter, r *http.Request) {
		t.Errorf("selector-only fixture unexpectedly sent a business request to proxy %d", id)
	})
	account.Extra["openai_excel_bps"] = false
	account.Extra["openai_proxy_mode"] = "request"
	account.Extra["openai_proxy_ids"] = []int64{1, 2, 3}
	account.Extra[OpenAIProxySessionAffinityKey] = true

	acquire := func(apiKeyID int64, threadID, input string) int64 {
		body := []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":%q,"stream":true,"prompt_cache_key":"moving-hint"}`, input))
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		c.Request.Header.Set("thread-id", threadID)
		c.Set("api_key", &APIKey{ID: apiKeyID})
		ctx := withOpenAIRequestProxySession(c.Request.Context(), c, account, body)
		require.NotEmpty(t, ctx.Value(openAIRequestProxySessionKey{}))
		routed, release, err := svc.beginOpenAIRequestProxy(ctx, c, account)
		require.NoError(t, err)
		route, ok := openAIRequestProxyFromContext(routed, account.ID)
		require.True(t, ok)
		id := route.lease.ProxyID
		release()
		return id
	}

	first := acquire(100, "thread-a", "first turn")
	second := acquire(100, "thread-a", "compacted continuation")
	otherSession := acquire(100, "thread-b", "independent session")
	otherKey := acquire(101, "thread-a", "same thread, another key")
	nextTurn := acquire(100, "thread-a", "another continuation")
	require.Equal(t, first, second)
	require.NotEqual(t, first, otherSession, "new sessions should spread over available exits")
	require.NotEqual(t, otherSession, otherKey, "API keys must not share a session binding")
	require.NotEqual(t, first, otherKey, "another API key must receive an independent binding")
	require.Equal(t, first, nextTurn)
}

func TestOpenAIRequestProxyMockSharedSessionDoesNotMergeConcurrentRequests(t *testing.T) {
	// This exercises Forward and the local proxy boundary. Handler-level account
	// slot admission remains a separate account-scoped gate.
	const requestCount = 24

	for _, sessionAffinity := range []bool{false, true} {
		name := "per_request"
		if sessionAffinity {
			name = "session_affinity"
		}
		t.Run(name, func(t *testing.T) {
			type observedRequest struct {
				proxyID  int
				session  string
				threadID string
			}
			arrived := make(chan observedRequest, requestCount)
			release := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			var activeMu sync.Mutex
			active, maxActive := 0, 0

			svc, account := localBPSRequestProxyFixture(t, 3, func(proxyID int, w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				activeMu.Lock()
				active++
				if active > maxActive {
					maxActive = active
				}
				activeMu.Unlock()
				arrived <- observedRequest{proxyID: proxyID, session: r.Header.Get("session_id"), threadID: r.Header.Get("thread-id")}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				activeMu.Lock()
				active--
				activeMu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, bpsRequestCompletedWire)
			})
			account.Extra["openai_excel_bps"] = false
			account.Extra[OpenAIProxyModeKey] = OpenAIProxyModeRequest
			account.Extra[OpenAIProxyIDsKey] = []int64{1, 2, 3}
			if sessionAffinity {
				account.Extra[OpenAIProxySessionAffinityKey] = true
			}

			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			done := make(chan error, requestCount)
			for i := 0; i < requestCount; i++ {
				go func(turn int) {
					requestAccount := *account
					requestAccount.Extra = cloneExcelBPSRecoveryExtra(account.Extra)
					requestAccount.Credentials = cloneExcelBPSRecoveryExtra(account.Credentials)
					body := []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":"turn-%02d","stream":true,"prompt_cache_key":"shared-session"}`, turn))
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body)).WithContext(ctx)
					c.Request.Header.Set("session_id", "shared-session-id")
					c.Request.Header.Set("thread-id", "shared-thread-id")
					c.Set("api_key", &APIKey{ID: 77331})
					result, err := svc.Forward(ctx, c, &requestAccount, body)
					if err == nil && (result == nil || rec.Code != http.StatusOK) {
						err = fmt.Errorf("incomplete mock response: status %d", rec.Code)
					}
					done <- err
				}(i)
			}

			hits := make(map[int]int)
			sessionIDs := make(map[string]int)
			threadIDs := make(map[string]int)
			for i := 0; i < requestCount; i++ {
				select {
				case request := <-arrived:
					hits[request.proxyID]++
					sessionIDs[request.session]++
					threadIDs[request.threadID]++
				case <-ctx.Done():
					t.Fatalf("only %d of %d mock requests reached the local exits: %v", i, requestCount, ctx.Err())
				}
			}
			releaseOnce.Do(func() { close(release) })
			for i := 0; i < requestCount; i++ {
				require.NoError(t, <-done)
			}

			activeMu.Lock()
			observedMaxActive := maxActive
			activeMu.Unlock()
			require.Len(t, sessionIDs, 1, "all independent requests carried the same session_id")
			require.Len(t, threadIDs, 1, "all independent requests carried the same thread-id")
			require.Equal(t, requestCount, observedMaxActive, "the fake upstream must observe all requests concurrently")
			if sessionAffinity {
				require.Len(t, hits, 1, "session affinity pins the shared conversation to one exit")
			} else {
				require.Equal(t, map[int]int{1: 8, 2: 8, 3: 8}, hits, "per-request mode spreads the shared session across exits")
			}
		})
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

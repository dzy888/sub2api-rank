package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/mihomo"
	"github.com/Wei-Shaw/sub2api/internal/pkg/requesttiming"
	"github.com/gin-gonic/gin"
)

var errOpenAIRequestProxyUnavailable = errors.New("selected OpenAI request proxies are unavailable")

type openAIRequestProxyContextKey struct{}
type openAIRequestProxySessionKey struct{}
type openAIRequestProxyWeakAffinityKey struct{}

const openAIRequestProxyOpsKey = "openai_request_proxy_ops_route"

type openAIRequestProxyRoute struct {
	accountID   int64
	proxy       string
	lease       *mihomo.BPSLease
	mu          sync.Mutex
	bodies      []*openAIRequestProxyBody
	cancelSends []context.CancelFunc
	finished    bool
	once        sync.Once
}

func (r *openAIRequestProxyRoute) bind(req *http.Request) (*http.Request, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return nil, errOpenAIRequestProxyUnavailable
	}
	ctx, cancel := context.WithCancel(req.Context())
	r.cancelSends = append(r.cancelSends, cancel)
	return req.WithContext(ctx), nil
}

type openAIRequestProxyBody struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (b *openAIRequestProxyBody) Close() error {
	b.once.Do(func() { b.err = b.ReadCloser.Close() })
	return b.err
}
func (r *openAIRequestProxyRoute) track(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	b := &openAIRequestProxyBody{ReadCloser: resp.Body}
	resp.Body = b
	r.mu.Lock()
	r.bodies = append(r.bodies, b)
	closed := r.finished
	r.mu.Unlock()
	if closed {
		_ = b.Close()
	}
}
func (r *openAIRequestProxyRoute) finish() {
	r.once.Do(func() {
		r.mu.Lock()
		r.finished = true
		bodies := append([]*openAIRequestProxyBody(nil), r.bodies...)
		cancels := append([]context.CancelFunc(nil), r.cancelSends...)
		r.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		for _, b := range bodies {
			_ = b.Close()
		}
		r.lease.Release()
	})
}

// Account tests return before their caller consumes the stream. Keep the
// request scope alive until that final body ends, closes, or is canceled.
type openAIRequestProxyOwnedBody struct {
	io.ReadCloser
	finish func()
}

func (b *openAIRequestProxyOwnedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.finish()
	}
	return n, err
}
func (b *openAIRequestProxyOwnedBody) Close() error {
	err := b.ReadCloser.Close()
	b.finish()
	return err
}

// One logical HTTP turn owns its exit across payload repairs and continuations.
// The scope ends only after Forward finishes consuming its final response.
func (s *OpenAIGatewayService) beginOpenAIRequestProxy(ctx context.Context, c *gin.Context, account *Account) (context.Context, func(), error) {
	noop := func() {}
	// Every attempt replaces attribution, including a failed acquisition after
	// another account was tried on this Gin request.
	if c != nil {
		c.Set(openAIRequestProxyOpsKey, (*openAIRequestProxyRoute)(nil))
	}
	if account == nil || !account.OpenAIRequestProxyEnabled() {
		return ctx, noop, nil
	}
	if route, _ := ctx.Value(openAIRequestProxyContextKey{}).(*openAIRequestProxyRoute); route != nil {
		if route.accountID == account.ID {
			if c != nil {
				c.Set(openAIRequestProxyOpsKey, route)
			}
			return ctx, noop, nil
		}
		return ctx, noop, writeOpenAIRequestProxyError(c, "openai_proxy_unavailable", "Selected proxy scope does not match the account")
	}
	scope, _ := ctx.Value(openAIRequestProxySessionKey{}).(string)
	ids := account.OpenAIRequestProxyIDs()
	var lease *mihomo.BPSLease
	var err error
	if scope != "" && account.requestProxySessionAffinityEnabled() {
		lease, err = mihomo.AcquireBPSStaticSessionRequestLease(ctx, scope, account.ID, ids)
	} else {
		lease, err = mihomo.AcquireBPSStaticRequestLease(ctx, account.ID, ids)
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx, noop, ctx.Err()
		}
		return ctx, noop, writeOpenAIRequestProxyError(c, "openai_proxy_unavailable", "No selected proxy is ready; direct fallback is disabled")
	}
	route := &openAIRequestProxyRoute{accountID: account.ID, proxy: lease.ProxyURL, lease: lease}
	if c != nil {
		c.Set(openAIRequestProxyOpsKey, route)
	}
	release := route.finish
	stop := context.AfterFunc(ctx, release)
	return context.WithValue(ctx, openAIRequestProxyContextKey{}, route), func() { stop(); release() }, nil
}

func (a *Account) requestProxySessionAffinityEnabled() bool {
	return a.OpenAIRequestProxySessionAffinityEnabled()
}

// Read affinity signals before request transformations. Account scheduling
// also recognizes legacy Codex threads and stable input prefixes.
func withOpenAIRequestProxySession(ctx context.Context, c *gin.Context, account *Account, body []byte) context.Context {
	if !account.requestProxySessionAffinityEnabled() {
		return ctx
	}
	apiKeyID := getAPIKeyIDFromContext(c)
	if apiKeyID <= 0 {
		return ctx
	}
	identity := resolveOpenAIClientSessionIdentity(c, body)
	var value, source string
	switch identity.metadata.Status {
	case OpenAIClientSessionIdentityResolved:
		value, source = identity.identity.value, "declared"
	case OpenAIClientSessionIdentityMissing:
		// A caller-supplied routing header retains precedence. A legacy thread
		// is more specific than the cache key, which the caller can override.
		if hint := sanitizeSessionID(explicitOpenAIHeaderSessionID(c)); hint != "" {
			value, source = hint, "client_hint"
		} else if thread := sanitizeSessionID(resolveOpenAIWSClientThreadID(c, body)); thread != "" {
			value, source = thread, "codex_thread"
		} else if hint := sanitizeSessionID(openAIRequestPromptCacheKey(body)); hint != "" {
			value, source = hint, "prompt_cache_key"
		} else if seed := deriveOpenAIAnchoredContentSessionSeed(body); seed != "" {
			value, source = "content:"+seed, "content_prefix"
		} else {
			source = "missing"
		}
	default:
		source = "invalid"
	}
	requesttiming.Mark(ctx, "request_proxy_affinity_"+source)
	if value == "" {
		return ctx
	}
	scope := fmt.Sprintf("%d:%x", apiKeyID, sha256.Sum256([]byte(value)))
	if source == "content_prefix" || source == "prompt_cache_key" {
		ctx = context.WithValue(ctx, openAIRequestProxyWeakAffinityKey{}, true)
	}
	return context.WithValue(ctx, openAIRequestProxySessionKey{}, scope)
}

func openAIRequestProxyWSScope(ctx context.Context, account *Account) string {
	if ctx == nil || !account.OpenAIRequestProxySessionAffinityEnabled() || ctx.Value(openAIRequestProxyWeakAffinityKey{}) == true {
		return ""
	}
	route, ok := openAIRequestProxyFromContext(ctx, account.ID)
	if !ok || route.lease == nil || !route.lease.SessionBound {
		return ""
	}
	scope, _ := ctx.Value(openAIRequestProxySessionKey{}).(string)
	if scope == "" {
		return ""
	}
	return fmt.Sprintf("request-proxy:%d:%d:%x:%s", account.ID, route.lease.ProxyID, sha256.Sum256([]byte(route.proxy)), scope)
}

func writeOpenAIRequestProxyError(c *gin.Context, code, message string) error {
	if c != nil && c.Writer != nil && !c.Writer.Written() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"type": "upstream_error", "code": code, "message": message}})
	}
	return errOpenAIRequestProxyUnavailable
}

func openAIRequestProxyFromContext(ctx context.Context, accountID int64) (*openAIRequestProxyRoute, bool) {
	route, _ := ctx.Value(openAIRequestProxyContextKey{}).(*openAIRequestProxyRoute)
	return route, route != nil && route.accountID == accountID && route.proxy != ""
}

func validateOpenAIWSRequestProxy(ctx context.Context, account *Account, proxyURL string) error {
	route, _ := openAIRequestProxyFromContext(ctx, account.ID)
	if !account.OpenAIRequestProxyEnabled() && route == nil {
		return nil
	}
	if openAIRequestProxyWSScope(ctx, account) == "" || !slices.Contains(account.OpenAIRequestProxyIDs(), route.lease.ProxyID) ||
		!route.lease.RequestProxyCurrent() {
		return errOpenAIRequestProxyUnavailable
	}
	_, err := ValidateOpenAIRequestProxyEgress(ctx, account.ID, proxyURL)
	return err
}

// ValidateOpenAIRequestProxyEgress lets transport adapters preserve a lease
// created by the gateway without exposing a way to create or replace it.
// The boolean distinguishes an ordinary request from an active scoped route.
func ValidateOpenAIRequestProxyEgress(ctx context.Context, accountID int64, proxyURL string) (bool, error) {
	if ctx == nil {
		return false, nil
	}
	route, _ := ctx.Value(openAIRequestProxyContextKey{}).(*openAIRequestProxyRoute)
	if route == nil {
		return false, nil
	}
	route.mu.Lock()
	defer route.mu.Unlock()
	if route.finished || route.accountID != accountID || route.proxy == "" || route.proxy != proxyURL {
		return true, errOpenAIRequestProxyUnavailable
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return true, nil
}

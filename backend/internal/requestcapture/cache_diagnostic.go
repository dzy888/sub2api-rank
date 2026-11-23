package requestcapture

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const cacheDiagnosticBodyLimit = 32 << 20
const cacheDiagnosticSamples = 6

type cacheDiagnosticConfig struct {
	Directory string    `json:"directory"`
	Marker    string    `json:"marker"`
	APIKeyID  int64     `json:"api_key_id"`
	AccountID int64     `json:"account_id"`
	Until     time.Time `json:"until"`
}

type cacheDiagnostic struct {
	cacheDiagnosticConfig
	key   [32]byte
	count atomic.Int32
}

type cacheDiagnosticContextKey struct{}

type cacheDiagnosticSample struct {
	diagnostic *cacheDiagnostic
	body       []byte
	meta       cacheDiagnosticMeta
	claimed    atomic.Bool
}

type cacheDiagnosticMeta struct {
	RequestID  string               `json:"request_id"`
	CapturedAt time.Time            `json:"captured_at"`
	AccountID  int64                `json:"account_id"`
	Input      cacheDiagnosticStage `json:"input"`
	Upstream   cacheDiagnosticStage `json:"upstream"`
}

type cacheDiagnosticStage struct {
	Bytes        int               `json:"bytes"`
	SHA256       string            `json:"sha256"`
	Model        string            `json:"model"`
	Fingerprints map[string]string `json:"identity_fingerprints"`
}

var cacheDiagnosticMarker = regexp.MustCompile(`^SUB2API-CACHE-[A-Za-z0-9]{6,64}$`)

var openAICacheDiagnostic = sync.OnceValue(func() *cacheDiagnostic {
	return newCacheDiagnostic(os.Getenv("SUB2API_CACHE_DIAGNOSTIC"), time.Now())
})

func newCacheDiagnostic(raw string, now time.Time) *cacheDiagnostic {
	var cfg cacheDiagnosticConfig
	if raw == "" || len(raw) > 4096 || json.Unmarshal([]byte(raw), &cfg) != nil || !cacheDiagnosticMarker.MatchString(cfg.Marker) ||
		!filepath.IsAbs(cfg.Directory) || cfg.APIKeyID <= 0 || cfg.AccountID <= 0 ||
		!cfg.Until.After(now) || cfg.Until.Sub(now) > time.Hour {
		return nil
	}
	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		return nil
	}
	info, err := os.Lstat(cfg.Directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil
	}
	d := &cacheDiagnostic{cacheDiagnosticConfig: cfg}
	if _, err := rand.Read(d.key[:]); err != nil {
		return nil
	}
	return d
}

// BeginCacheDiagnostic admits only authenticated Responses reproductions with
// an operator-selected marker in user input. The body is detached before later
// request rewrites. This is independent of the error-only capture workflow.
func BeginCacheDiagnostic(req *http.Request, apiKeyID int64, body []byte) *http.Request {
	d := openAICacheDiagnostic()
	if d == nil {
		return req
	}
	if ctx := d.begin(req, apiKeyID, body, time.Now()); ctx != req.Context() {
		return req.WithContext(ctx)
	}
	return req
}

func (d *cacheDiagnostic) begin(req *http.Request, apiKeyID int64, body []byte, now time.Time) context.Context {
	ctx := req.Context()
	if d == nil || apiKeyID != d.APIKeyID || !now.Before(d.Until) || d.count.Load() >= cacheDiagnosticSamples ||
		len(body) > cacheDiagnosticBodyLimit || !bytes.Contains(body, []byte(d.Marker)) || req.Method != http.MethodPost {
		return ctx
	}
	if req.URL.Path != "/v1/responses" && req.URL.Path != "/openai/v1/responses" && req.URL.Path != "/responses" {
		return ctx
	}
	input := gjson.GetBytes(body, "input")
	match := input.Type == gjson.String && strings.Contains(input.Str, d.Marker)
	if input.IsArray() {
		input.ForEach(func(_, item gjson.Result) bool {
			if item.Get("role").Str == "user" {
				match = strings.Contains(item.Get("content").Raw, d.Marker)
			}
			return true
		})
	}
	requestID, _ := ctx.Value(ctxkey.ClientRequestID).(string)
	if !match || requestID == "" || !gjson.ValidBytes(body) {
		return ctx
	}
	for count := d.count.Load(); ; count = d.count.Load() {
		if count >= cacheDiagnosticSamples {
			return ctx
		}
		if d.count.CompareAndSwap(count, count+1) {
			break
		}
	}
	sample := &cacheDiagnosticSample{diagnostic: d, body: bytes.Clone(body), meta: cacheDiagnosticMeta{
		RequestID: requestID, CapturedAt: now.UTC(), AccountID: d.AccountID, Input: d.stage(body, req.Header),
	}}
	return context.WithValue(ctx, cacheDiagnosticContextKey{}, sample)
}

// ObserveCacheDiagnosticRequest takes a separate reader after egress rewriting.
// It leaves the original transport body and its retry behavior untouched.
func ObserveCacheDiagnosticRequest(req *http.Request, accountID int64) {
	sample, _ := req.Context().Value(cacheDiagnosticContextKey{}).(*cacheDiagnosticSample)
	if sample == nil || accountID != sample.diagnostic.AccountID || req.GetBody == nil ||
		req.ContentLength <= 0 || req.ContentLength > cacheDiagnosticBodyLimit ||
		!time.Now().Before(sample.diagnostic.Until) || !sample.claimed.CompareAndSwap(false, true) {
		return
	}
	body, err := req.GetBody()
	if err != nil {
		return
	}
	headers := sample.diagnostic.identities(req.Header)
	go sample.save(body, req.ContentLength, headers)
}

func (d *cacheDiagnostic) fingerprint(value string) string {
	digest := hmac.New(sha256.New, d.key[:])
	_, _ = io.WriteString(digest, value)
	return hex.EncodeToString(digest.Sum(nil)[:12])
}

func (d *cacheDiagnostic) identities(headers http.Header) map[string]string {
	values := make(map[string]string)
	for _, name := range []string{"session_id", "session-id", "conversation_id", "thread-id", "thread_id"} {
		if value := headers.Get(name); value != "" {
			values[name] = d.fingerprint(value)
		}
	}
	return values
}

func (d *cacheDiagnostic) stage(body []byte, headers http.Header) cacheDiagnosticStage {
	return d.describe(body, d.identities(headers))
}

func (d *cacheDiagnostic) describe(body []byte, identities map[string]string) cacheDiagnosticStage {
	for _, name := range []string{"prompt_cache_key", "client_metadata.session_id", "client_metadata.thread_id"} {
		if field := gjson.GetBytes(body, name); field.Type == gjson.String && field.Str != "" {
			identities[name] = d.fingerprint(field.Str)
		}
	}
	sum := sha256.Sum256(body)
	return cacheDiagnosticStage{Bytes: len(body), SHA256: hex.EncodeToString(sum[:]), Model: gjson.GetBytes(body, "model").Str, Fingerprints: identities}
}

func (s *cacheDiagnosticSample) save(reader io.ReadCloser, length int64, headers map[string]string) {
	defer reader.Close()
	body, err := io.ReadAll(io.LimitReader(reader, length+1))
	if err != nil || int64(len(body)) != length || !gjson.ValidBytes(body) {
		return
	}
	s.meta.Upstream = s.diagnostic.describe(body, headers)
	meta, err := json.Marshal(s.meta)
	if err != nil {
		return
	}
	name := uuid.NewString()
	temp := filepath.Join(s.diagnostic.Directory, "."+name)
	if err := os.Mkdir(temp, 0700); err != nil {
		return
	}
	defer os.RemoveAll(temp)
	if os.WriteFile(filepath.Join(temp, "ingress.json"), s.body, 0600) != nil ||
		os.WriteFile(filepath.Join(temp, "upstream.json"), body, 0600) != nil ||
		os.WriteFile(filepath.Join(temp, "metadata.json"), meta, 0600) != nil {
		return
	}
	if err := os.Rename(temp, filepath.Join(s.diagnostic.Directory, name)); err == nil {
		logger.L().Info("openai.cache_diagnostic_captured", zap.String("client_request_id", s.meta.RequestID), zap.Int64("account_id", s.meta.AccountID))
	}
}

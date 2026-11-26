package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// sessionSlotCacheStub 记录会话槽位的键，用于验证闸门按 (账号, 会话) 维度限流。
type sessionSlotCacheStub struct {
	helperConcurrencyCacheStub
	limitSeen  int
	acquireArg []string
	released   []string
}

func (s *sessionSlotCacheStub) AcquireSessionSlot(_ context.Context, accountID int64, scope string, maxConcurrency int, requestID string) (bool, error) {
	s.limitSeen = maxConcurrency
	s.acquireArg = append(s.acquireArg, scope)
	return true, nil
}

func (s *sessionSlotCacheStub) ReleaseSessionSlot(_ context.Context, _ int64, scope, _ string) error {
	s.released = append(s.released, scope)
	return nil
}

func (s *sessionSlotCacheStub) GetSessionConcurrency(context.Context, int64, string) (int, error) {
	return 0, nil
}

func (s *sessionSlotCacheStub) ListOpenSessionScopes(context.Context, int64, int) ([]string, error) {
	return nil, nil
}

func TestAcquireSessionSlotWithWaitTimeoutUsesConfiguredLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := &sessionSlotCacheStub{}
	helper := NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatClaude, 0)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	release, err := helper.AcquireSessionSlotWithWaitTimeout(c, 68, "scope-a", 26, time.Second, false, new(bool))
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Equal(t, 26, cache.limitSeen, "配置值必须传到缓存层")
	require.Equal(t, []string{"scope-a"}, cache.acquireArg)

	release()
	require.Equal(t, []string{"scope-a"}, cache.released, "释放必须回到同一会话键")
}

// overflowSessionStub 让第一个请求的会话恒满、其它会话可抢，用于验证溢出。
type overflowSessionStub struct {
	sessionSlotCacheStub
	fullScope string
	open      []string
}

func (s *overflowSessionStub) AcquireSessionSlot(_ context.Context, _ int64, scope string, _ int, _ string) (bool, error) {
	if scope == s.fullScope {
		return false, nil
	}
	return true, nil
}

func (s *overflowSessionStub) ListOpenSessionScopes(context.Context, int64, int) ([]string, error) {
	return s.open, nil
}

func TestAcquireSessionSlotOverflowsToIdleSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := &overflowSessionStub{fullScope: "hot", open: []string{"hot", "idle"}}
	helper := NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatClaude, 0)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	release, usedScope, err := helper.AcquireSessionSlotWithOverflowWaitTimeout(c, 1, "hot", 4, time.Second, false, new(bool))
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Equal(t, "idle", usedScope, "热点会话满时必须溢出到空闲会话，而不是原地排队")

	// 释放必须回到实际占用的会话，否则会把槽位泄漏在错误的会话上。
	release()
	require.Equal(t, []string{"idle"}, cache.released)
}

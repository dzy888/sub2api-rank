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

//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newHeartbeatTestCache(t *testing.T, slotTTLMinutes int) (*concurrencyCache, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cache, ok := NewConcurrencyCache(client, slotTTLMinutes, 60).(*concurrencyCache)
	require.True(t, ok, "NewConcurrencyCache must return *concurrencyCache")
	return cache, server
}

// 账号槽位的存活窗口必须被心跳窗口压住：否则漏释放的槽位会按 30 分钟一直
// 计入「当前并发」，管理端的并发数就会长期虚高。
func TestAccountSlotTTLIsCappedByHeartbeatWindow(t *testing.T) {
	longLived, _ := newHeartbeatTestCache(t, 30)
	require.Equal(t, accountSlotLiveTTLSeconds, longLived.accountSlotTTL())

	// 配置比心跳窗口更短时以配置为准。
	shortLived, _ := newHeartbeatTestCache(t, 1)
	require.Equal(t, 60, shortLived.accountSlotTTL())
}

func TestAccountSlotExpiresAfterLiveWindowWithoutHeartbeat(t *testing.T) {
	cache, _ := newHeartbeatTestCache(t, 30)
	ctx := context.Background()

	acquired, err := cache.AcquireAccountSlot(ctx, 7, 10, "req-stale")
	require.NoError(t, err)
	require.True(t, acquired)

	count, err := cache.GetAccountConcurrency(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// 超过存活窗口后，未续期的槽位必须消失。
	// 直接把 score 推到窗口之外，避免依赖 miniredis 对 Lua TIME 的 FastForward 支持。
	stale := float64(time.Now().Unix() - int64(accountSlotLiveTTLSeconds) - 10)
	require.NoError(t, cache.rdb.ZAdd(ctx, accountSlotKey(7), redis.Z{Score: stale, Member: "req-stale"}).Err())

	count, err = cache.GetAccountConcurrency(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestAccountSlotRefreshKeepsLiveRequestCounted(t *testing.T) {
	cache, _ := newHeartbeatTestCache(t, 30)
	ctx := context.Background()

	acquired, err := cache.AcquireAccountSlot(ctx, 8, 10, "req-live")
	require.NoError(t, err)
	require.True(t, acquired)

	// 心跳续期后即使原始 score 已经过期，仍在飞请求要继续计入。
	stale := float64(time.Now().Unix() - int64(accountSlotLiveTTLSeconds) - 10)
	require.NoError(t, cache.rdb.ZAdd(ctx, accountSlotKey(8), redis.Z{Score: stale, Member: "req-live"}).Err())
	require.NoError(t, cache.RefreshAccountSlot(ctx, 8, "req-live"))

	count, err := cache.GetAccountConcurrency(ctx, 8)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// 刷新不能复活已释放（或已过期）的槽位。
func TestAccountSlotRefreshDoesNotResurrectReleasedSlot(t *testing.T) {
	cache, _ := newHeartbeatTestCache(t, 30)
	ctx := context.Background()

	acquired, err := cache.AcquireAccountSlot(ctx, 9, 10, "req-released")
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, 9, "req-released"))

	require.NoError(t, cache.RefreshAccountSlot(ctx, 9, "req-released"))
	count, err := cache.GetAccountConcurrency(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

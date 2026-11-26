//go:build unit

package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type heartbeatRefresherStub struct {
	calls atomic.Int64
}

func (s *heartbeatRefresherStub) RefreshAccountSlot(context.Context, int64, string) error {
	s.calls.Add(1)
	return nil
}

func TestAccountSlotHeartbeatRefreshesUntilStopped(t *testing.T) {
	original := accountSlotHeartbeatInterval
	accountSlotHeartbeatInterval = 5 * time.Millisecond
	t.Cleanup(func() { accountSlotHeartbeatInterval = original })

	refresher := &heartbeatRefresherStub{}
	stop := startAccountSlotHeartbeat(refresher, 42, "req-1")

	require.Eventually(t, func() bool { return refresher.calls.Load() >= 2 }, time.Second, 2*time.Millisecond)
	stop()

	settled := refresher.calls.Load()
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, settled, refresher.calls.Load(), "停止后不应再续期")

	// 重复调用 stop 必须安全。
	stop()
}

func TestAccountSlotHeartbeatSkipsInvalidInput(t *testing.T) {
	refresher := &heartbeatRefresherStub{}
	for _, tc := range []struct {
		accountID int64
		requestID string
	}{
		{0, "req"},
		{1, ""},
	} {
		stop := startAccountSlotHeartbeat(refresher, tc.accountID, tc.requestID)
		time.Sleep(10 * time.Millisecond)
		stop()
	}
	require.Zero(t, refresher.calls.Load())
}

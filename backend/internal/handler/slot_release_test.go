//go:build unit

package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 回归：包装前必须保存原始账号释放函数。曾经在闭包里直接调用被重新赋值的
// accountRelease，导致 sync.Once.Do 内部递归调用自己而永久阻塞，账号槽位
// 与心跳都停不下来（表现为账号并发数一直不降）。
func TestCombineSlotReleasesIsIdempotentAndNotRecursive(t *testing.T) {
	var accountCalls, sessionCalls int

	done := make(chan struct{})
	go func() {
		defer close(done)
		release := combineSlotReleases(
			func() { accountCalls++ },
			func() { sessionCalls++ },
		)
		release()
		release() // 幂等
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("release deadlocked")
	}

	require.Equal(t, 1, accountCalls, "账号槽位必须释放且只释放一次")
	require.Equal(t, 1, sessionCalls, "会话槽位必须释放且只释放一次")
}

func TestCombineSlotReleasesHandlesMissingSlots(t *testing.T) {
	calls := 0
	combineSlotReleases(func() { calls++ }, nil)()
	require.Equal(t, 1, calls, "无会话槽位时应直接复用账号释放函数")

	calls = 0
	combineSlotReleases(nil, func() { calls++ })()
	require.Equal(t, 1, calls, "无账号槽位时仍应释放会话槽位")

	require.Nil(t, combineSlotReleases(nil, nil), "两者都为空时保持 nil，调用方按 nil 判断")
}

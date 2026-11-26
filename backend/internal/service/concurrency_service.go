package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// ConcurrencyCache 定义并发控制的缓存接口
// 使用有序集合存储槽位，按时间戳清理过期条目
type ConcurrencyCache interface {
	// 账号槽位管理
	// 键格式: concurrency:account:{accountID}（有序集合，成员为 requestID）
	AcquireAccountSlot(ctx context.Context, accountID int64, maxConcurrency int, requestID string) (bool, error)
	ReleaseAccountSlot(ctx context.Context, accountID int64, requestID string) error
	GetAccountConcurrency(ctx context.Context, accountID int64) (int, error)
	GetAccountConcurrencyBatch(ctx context.Context, accountIDs []int64) (map[int64]int, error)

	// 会话槽位管理见 SessionConcurrencyCache：不放进主接口，既有缓存桩无需全部实现。
	// 账号等待队列（账号级）
	IncrementAccountWaitCount(ctx context.Context, accountID int64, maxWait int) (bool, error)
	DecrementAccountWaitCount(ctx context.Context, accountID int64) error
	GetAccountWaitingCount(ctx context.Context, accountID int64) (int, error)

	// 用户槽位管理
	// 键格式: concurrency:user:{userID}（有序集合，成员为 requestID）
	AcquireUserSlot(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error)
	ReleaseUserSlot(ctx context.Context, userID int64, requestID string) error
	GetUserConcurrency(ctx context.Context, userID int64) (int, error)

	// 等待队列计数（每次入队都会刷新 TTL，避免长时间排队时计数提前过期）
	IncrementWaitCount(ctx context.Context, userID int64, maxWait int) (bool, error)
	DecrementWaitCount(ctx context.Context, userID int64) error

	// 批量负载查询（只读）
	GetAccountsLoadBatch(ctx context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error)
	GetUsersLoadBatch(ctx context.Context, users []UserWithConcurrency) (map[int64]*UserLoadInfo, error)

	// 清理过期槽位（后台任务）
	CleanupExpiredAccountSlots(ctx context.Context, accountID int64) error
	CleanupExpiredAccountSlotKeys(ctx context.Context) error

	// Startup reclaims expired slots only; peer prefixes and waiting counters remain valid.
	CleanupStaleProcessSlots(ctx context.Context, activeRequestPrefix string) error
}

// AccountSlotRefresher 是可选的账号槽位心跳能力。
// 账号槽位的存活窗口很短（见 repository 的 accountSlotLiveTTLSeconds），
// 在飞请求必须定期续期，否则会被当成过期槽位清掉。
type AccountSlotRefresher interface {
	RefreshAccountSlot(ctx context.Context, accountID int64, requestID string) error
}

// accountSlotHeartbeatInterval 必须显著小于账号槽位存活窗口（120s）。
// 声明为变量以便测试缩短间隔。
var accountSlotHeartbeatInterval = 30 * time.Second

const (
	accountSlotHeartbeatTimeout = 2 * time.Second
	// accountSlotHeartbeatMaxLifetime 是心跳的上限寿命：即使释放函数因故没被调用，
	// 心跳协程也会自行退出，不会永久驻留（槽位本身 120s 后自然过期）。
	accountSlotHeartbeatMaxLifetime = 30 * time.Minute
)

// startAccountSlotHeartbeat 在请求存活期间周期性刷新账号槽位的 score。
// 返回的函数会停止心跳并等待其退出，重复调用安全。
//
// parent 是请求自身的 context：它结束时（handler 返回）心跳必须立即停，
// 即使调用方漏调了释放函数。否则一条没被释放的槽位会被心跳一直续命，
// 在管理端表现为「并发数一直降不下来，但请求早就结束了」。
func startAccountSlotHeartbeat(parent context.Context, refresher AccountSlotRefresher, accountID int64, requestID string) func() {
	if refresher == nil || accountID <= 0 || requestID == "" {
		return func() {}
	}
	var parentDone <-chan struct{}
	if parent != nil {
		parentDone = parent.Done()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(accountSlotHeartbeatInterval)
		defer ticker.Stop()
		lifetime := time.NewTimer(accountSlotHeartbeatMaxLifetime)
		defer lifetime.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-parentDone:
				return
			case <-lifetime.C:
				return
			case <-ticker.C:
				opCtx, opCancel := context.WithTimeout(context.Background(), accountSlotHeartbeatTimeout)
				_ = refresher.RefreshAccountSlot(opCtx, accountID, requestID)
				opCancel()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

type APIKeyConcurrencyCache interface {
	TrackAPIKeySlot(ctx context.Context, apiKeyID int64, requestID string) error
	ReleaseAPIKeySlot(ctx context.Context, apiKeyID int64, requestID string) error
	GetAPIKeyConcurrencyBatch(ctx context.Context, apiKeyIDs []int64) (map[int64]int, error)
}

// OpenAIWSIngressLeaseCache owns the short-lived distributed lease used to
// bound live client WebSocket sessions. It is deliberately independent of the
// request-slot namespace: idle ingress connections do not occupy turn slots.
type OpenAIWSIngressLeaseCache interface {
	AcquireOpenAIWSIngressLease(ctx context.Context, apiKeyID int64, maxConnections int, leaseID string) (bool, error)
	RefreshOpenAIWSIngressLease(ctx context.Context, apiKeyID int64, leaseID string) (bool, error)
	ReleaseOpenAIWSIngressLease(ctx context.Context, apiKeyID int64, leaseID string) error
}

const (
	openAIWSIngressLeaseTTL             = 60 * time.Second
	openAIWSIngressLeaseRefreshInterval = 20 * time.Second
	openAIWSIngressLeaseOperationTO     = 2 * time.Second
)

var ErrOpenAIWSIngressLeaseLost = errors.New("openai websocket ingress lease lost")

// OpenAIWSIngressLease keeps a Redis-backed ingress lease alive and cancels
// its context if Redis cannot confirm ownership for a full lease lifetime.
// Call Release on every handler exit to reclaim capacity immediately.
type OpenAIWSIngressLease struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	cache    OpenAIWSIngressLeaseCache
	apiKeyID int64
	leaseID  string

	stopOnce    sync.Once
	stopCh      chan struct{}
	refreshDone chan struct{}
}

func (l *OpenAIWSIngressLease) Context() context.Context {
	if l == nil || l.ctx == nil {
		return context.Background()
	}
	return l.ctx
}

func (l *OpenAIWSIngressLease) Release() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() {
		if l.stopCh != nil {
			close(l.stopCh)
		}
		if l.cancel != nil {
			l.cancel(nil)
		}
		if l.refreshDone != nil {
			<-l.refreshDone
		}
		if l.cache == nil || l.apiKeyID <= 0 || l.leaseID == "" {
			return
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), openAIWSIngressLeaseOperationTO)
		defer releaseCancel()
		if err := l.cache.ReleaseOpenAIWSIngressLease(releaseCtx, l.apiKeyID, l.leaseID); err != nil {
			logger.L().Warn("openai_ws_ingress_lease_release_failed",
				zap.Int64("api_key_id", l.apiKeyID),
				zap.Error(err),
			)
		}
	})
}

func (l *OpenAIWSIngressLease) refreshLoop() {
	defer func() {
		if l != nil && l.refreshDone != nil {
			close(l.refreshDone)
		}
	}()
	if l == nil || l.cache == nil {
		return
	}
	ticker := time.NewTicker(openAIWSIngressLeaseRefreshInterval)
	defer ticker.Stop()
	lastConfirmedAt := time.Now()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-ticker.C:
			var lost bool
			lastConfirmedAt, lost = l.refresh(lastConfirmedAt)
			if lost {
				l.cancel(ErrOpenAIWSIngressLeaseLost)
				return
			}
		}
	}
}

// refresh confirms the lease is still owned. A missing member is an immediate
// lease loss; transient Redis errors are tolerated only for one full lease TTL.
func (l *OpenAIWSIngressLease) refresh(lastConfirmedAt time.Time) (time.Time, bool) {
	refreshCtx, refreshCancel := context.WithTimeout(context.Background(), openAIWSIngressLeaseOperationTO)
	owned, err := l.cache.RefreshOpenAIWSIngressLease(refreshCtx, l.apiKeyID, l.leaseID)
	refreshCancel()
	if err == nil && owned {
		return time.Now(), false
	}
	if err == nil {
		err = ErrOpenAIWSIngressLeaseLost
	}
	elapsed := time.Since(lastConfirmedAt)
	logger.L().Warn("openai_ws_ingress_lease_refresh_failed",
		zap.Int64("api_key_id", l.apiKeyID),
		zap.Duration("unconfirmed_for", elapsed),
		zap.Error(err),
	)
	if errors.Is(err, ErrOpenAIWSIngressLeaseLost) || elapsed >= openAIWSIngressLeaseTTL {
		logger.L().Error("openai_ws_ingress_lease_lost",
			zap.Int64("api_key_id", l.apiKeyID),
			zap.Duration("unconfirmed_for", elapsed),
			zap.Error(err),
		)
		return lastConfirmedAt, true
	}
	return lastConfirmedAt, false
}

var (
	requestIDPrefix  = initRequestIDPrefix()
	requestIDCounter atomic.Uint64
)

func initRequestIDPrefix() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err == nil {
		return "r" + strconv.FormatUint(binary.BigEndian.Uint64(b), 36)
	}
	fallback := uint64(time.Now().UnixNano()) ^ (uint64(os.Getpid()) << 16)
	return "r" + strconv.FormatUint(fallback, 36)
}

func RequestIDPrefix() string {
	return requestIDPrefix
}

func generateRequestID() string {
	seq := requestIDCounter.Add(1)
	return requestIDPrefix + "-" + strconv.FormatUint(seq, 36)
}

func (s *ConcurrencyService) CleanupStaleProcessSlots(ctx context.Context) error {
	if s == nil || s.cache == nil {
		return nil
	}
	return s.cache.CleanupStaleProcessSlots(ctx, RequestIDPrefix())
}

const (
	// 默认等待队列额外槽位
	defaultExtraWaitSlots = 20

	defaultAccountLoadBatchCacheTTL = 200 * time.Millisecond
	accountLoadBatchFetchTimeout    = 3 * time.Second
	maxAccountLoadBatchCacheEntries = 256
	apiKeyConcurrencyFetchTimeout   = 3 * time.Second
	apiKeySlotTrackTimeout          = 2 * time.Second
)

// SessionConcurrencyCache 是「账号 + 会话」维度的可选槽位能力。
// 上游按 session 计路由级并发配额（route_rate_limit_exceeded，实测约 26），
// 收敛成少量会话后必须按会话限额。未实现该接口的缓存退化为不限。
// 键格式: concurrency:session:{accountID}:{scope}（有序集合，成员为 requestID）
type SessionConcurrencyCache interface {
	AcquireSessionSlot(ctx context.Context, accountID int64, scope string, maxConcurrency int, requestID string) (bool, error)
	ReleaseSessionSlot(ctx context.Context, accountID int64, scope, requestID string) error
	GetSessionConcurrency(ctx context.Context, accountID int64, scope string) (int, error)
	// ListOpenSessionScopes 返回该账号下仍有占用的会话 scope，用于会话溢出：
	// 本会话已满时优先挪到别的空闲会话，而不是在原会话上死等。
	ListOpenSessionScopes(ctx context.Context, accountID int64, limit int) ([]string, error)
}

// ConcurrencyService 管理账号和用户的并发限制。
type ConcurrencyService struct {
	cache             ConcurrencyCache
	performancePauses PerformancePauseReader

	accountLoadCacheTTL atomic.Int64
	accountLoadCacheMu  sync.RWMutex
	accountLoadCache    map[string]cachedAccountLoadBatch
	accountLoadGroup    singleflight.Group

	// apiKeyQueuePolicy is the immutable process policy for key-level waiting.
	apiKeyQueuePolicy atomic.Pointer[APIKeyQueuePolicy]
	apiKeyQueueStop   *apiKeyQueueStopState
}

type cachedAccountLoadBatch struct {
	loadMap   map[int64]*AccountLoadInfo
	expiresAt time.Time
}

// NewConcurrencyService 创建并发控制服务。
func NewConcurrencyService(cache ConcurrencyCache) *ConcurrencyService {
	svc := &ConcurrencyService{
		cache:            cache,
		accountLoadCache: make(map[string]cachedAccountLoadBatch),
		apiKeyQueueStop:  newAPIKeyQueueStopState(),
	}
	svc.SetAccountLoadBatchCacheTTL(defaultAccountLoadBatchCacheTTL)
	return svc
}

// AcquireOpenAIWSIngressLease atomically reserves one live ingress connection
// for an API key. A non-positive limit explicitly disables this protection.
func (s *ConcurrencyService) AcquireOpenAIWSIngressLease(ctx context.Context, apiKeyID int64, maxConnections int) (*OpenAIWSIngressLease, bool, error) {
	if maxConnections <= 0 {
		return nil, true, nil
	}
	if s == nil || s.cache == nil || apiKeyID <= 0 {
		return nil, false, errors.New("openai websocket ingress lease cache is unavailable")
	}
	cache, ok := s.cache.(OpenAIWSIngressLeaseCache)
	if !ok {
		return nil, false, errors.New("openai websocket ingress lease cache is unsupported")
	}
	leaseID := generateRequestID()
	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	acquireCtx, acquireCancel := context.WithTimeout(baseCtx, openAIWSIngressLeaseOperationTO)
	acquired, err := cache.AcquireOpenAIWSIngressLease(acquireCtx, apiKeyID, maxConnections, leaseID)
	acquireCancel()
	if err != nil || !acquired {
		return nil, acquired, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	leaseCtx, leaseCancel := context.WithCancelCause(ctx)
	lease := &OpenAIWSIngressLease{
		ctx:         leaseCtx,
		cancel:      leaseCancel,
		cache:       cache,
		apiKeyID:    apiKeyID,
		leaseID:     leaseID,
		stopCh:      make(chan struct{}),
		refreshDone: make(chan struct{}),
	}
	go lease.refreshLoop()
	return lease, true, nil
}

// SetAccountLoadBatchCacheTTL 设置账号负载批量读取的极短 TTL 缓存；非正数表示禁用缓存。
func (s *ConcurrencyService) SetAccountLoadBatchCacheTTL(ttl time.Duration) {
	if s == nil {
		return
	}
	s.accountLoadCacheTTL.Store(int64(ttl))
	if ttl <= 0 {
		s.accountLoadCacheMu.Lock()
		s.accountLoadCache = make(map[string]cachedAccountLoadBatch)
		s.accountLoadCacheMu.Unlock()
	}
}

// AcquireResult represents the result of acquiring a concurrency slot
type AcquireResult struct {
	Acquired    bool
	ReleaseFunc func() // Must be called when done (typically via defer)
	// RequestID is the exact Redis member owned by this result. Live transfer
	// uses it to move the reservation atomically instead of counting it twice.
	RequestID string
}

type AccountWithConcurrency struct {
	ID             int64
	MaxConcurrency int
}

type UserWithConcurrency struct {
	ID             int64
	MaxConcurrency int
}

type AccountLoadInfo struct {
	AccountID          int64
	CurrentConcurrency int
	WaitingCount       int
	LoadRate           int // 0-100+ (percent)
}

type UserLoadInfo struct {
	UserID             int64
	CurrentConcurrency int
	WaitingCount       int
	LoadRate           int // 0-100+ (percent)
}

// AcquireAccountSlot attempts to acquire a concurrency slot for an account.
// If the account is at max concurrency, it waits until a slot is available or timeout.
// Returns a release function that MUST be called when the request completes.
func (s *ConcurrencyService) AcquireAccountSlot(ctx context.Context, accountID int64, maxConcurrency int) (*AcquireResult, error) {
	// Recheck the independent live authority even for stale/sticky candidates.
	if s.performancePauses != nil {
		paused, err := s.performancePauses.PerformancePaused(ctx, []int64{accountID})
		if err != nil {
			return nil, err
		}
		if paused[accountID] {
			return &AcquireResult{Acquired: false}, nil
		}
	}
	// If maxConcurrency is 0 or negative, no limit
	// A stale scheduler snapshot may still carry the concurrency from before an
	// AI action. Recheck the shared authority before admitting a new slot; never
	// enlarge a caller's stricter bound or interrupt an already-held request.
	if reader, ok := s.performancePauses.(interface {
		AIOpsRuntime(context.Context, []int64) (map[int64]AIOpsRuntime, error)
	}); ok {
		runtime, err := reader.AIOpsRuntime(ctx, []int64{accountID})
		if err != nil {
			return nil, err
		}
		if current, exists := runtime[accountID]; exists {
			if current.Paused {
				return &AcquireResult{Acquired: false}, nil
			}
			if current.Concurrency > 0 && (maxConcurrency <= 0 || current.Concurrency < maxConcurrency) {
				maxConcurrency = current.Concurrency
			}
		}
	}
	if maxConcurrency <= 0 {
		return &AcquireResult{
			Acquired:    true,
			ReleaseFunc: func() {}, // no-op
		}, nil
	}

	// Generate unique request ID for this slot
	requestID := generateRequestID()

	acquired, err := s.cache.AcquireAccountSlot(ctx, accountID, maxConcurrency, requestID)
	if err != nil {
		return nil, err
	}

	if acquired {
		// 账号槽位存活窗口很短，靠心跳续期；请求结束时停止心跳并释放槽位。
		stopHeartbeat := func() {}
		if refresher, ok := s.cache.(AccountSlotRefresher); ok {
			stopHeartbeat = startAccountSlotHeartbeat(ctx, refresher, accountID, requestID)
		}
		return &AcquireResult{
			Acquired:  true,
			RequestID: requestID,
			ReleaseFunc: func() {
				stopHeartbeat()
				bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := s.cache.ReleaseAccountSlot(bgCtx, accountID, requestID); err != nil {
					logger.LegacyPrintf("service.concurrency", "Warning: failed to release account slot for %d (req=%s): %v", accountID, requestID, err)
				}
			},
		}, nil
	}

	return &AcquireResult{
		Acquired:    false,
		ReleaseFunc: nil,
	}, nil
}

// AcquireSessionSlot 获取某个会话在一个账号上的槽位。
// 与账号槽位相同的 TTL/清理语义，只是键多了一维 scope。
func (s *ConcurrencyService) AcquireSessionSlot(ctx context.Context, accountID int64, scope string, maxConcurrency int) (*AcquireResult, error) {
	return s.acquireSessionSlotOnScope(ctx, accountID, scope, maxConcurrency)
}

// AcquireSessionSlotWithOverflow 先在期望会话上取槽；该会话已满时，再从同账号的
// 其它会话里挑一个仍有空位的继续尝试，成功则返回实际落到的 scope。
//
// 目的：会话闸门不能只看「我这个会话满没满」。同一个账号上别的会话可能还空着，
// 死等原会话会让并发卡在个别热点会话上（一个槽排队、其它槽空闲）。溢出只在
// 存在空闲时发生，不会突破各会话自己的上限。
//
// 返回的 scope 可能与入参不同，调用方必须用返回值做后续粘性处理。
func (s *ConcurrencyService) AcquireSessionSlotWithOverflow(ctx context.Context, accountID int64, scope string, maxConcurrency int, overflowLimit int) (*AcquireResult, string, error) {
	if maxConcurrency <= 0 || accountID <= 0 || strings.TrimSpace(scope) == "" {
		return &AcquireResult{Acquired: true, ReleaseFunc: func() {}}, scope, nil
	}
	sessionCache, ok := s.cache.(SessionConcurrencyCache)
	if !ok {
		return &AcquireResult{Acquired: true, ReleaseFunc: func() {}}, scope, nil
	}

	if result, err := s.acquireSessionSlotOnScope(ctx, accountID, scope, maxConcurrency); err != nil || result.Acquired {
		return result, scope, err
	}

	open, err := sessionCache.ListOpenSessionScopes(ctx, accountID, overflowLimit)
	if err != nil {
		// 枚举失败不算请求失败：退化为只在原会话上排队。
		return &AcquireResult{Acquired: false}, scope, nil
	}
	for _, candidate := range open {
		if candidate == scope {
			continue
		}
		result, acquireErr := s.acquireSessionSlotOnScope(ctx, accountID, candidate, maxConcurrency)
		if acquireErr != nil {
			continue
		}
		if result.Acquired {
			return result, candidate, nil
		}
	}
	return &AcquireResult{Acquired: false}, scope, nil
}

func (s *ConcurrencyService) acquireSessionSlotOnScope(ctx context.Context, accountID int64, scope string, maxConcurrency int) (*AcquireResult, error) {
	if maxConcurrency <= 0 || accountID <= 0 || strings.TrimSpace(scope) == "" {
		return &AcquireResult{Acquired: true, ReleaseFunc: func() {}}, nil
	}
	sessionCache, ok := s.cache.(SessionConcurrencyCache)
	if !ok {
		return &AcquireResult{Acquired: true, ReleaseFunc: func() {}}, nil
	}
	requestID := generateRequestID()
	acquired, err := sessionCache.AcquireSessionSlot(ctx, accountID, scope, maxConcurrency, requestID)
	if err != nil {
		return nil, err
	}
	if acquired {
		return &AcquireResult{
			Acquired:  true,
			RequestID: requestID,
			ReleaseFunc: func() {
				bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := sessionCache.ReleaseSessionSlot(bgCtx, accountID, scope, requestID); err != nil {
					logger.LegacyPrintf("service.concurrency", "Warning: failed to release session slot for %d/%s (req=%s): %v", accountID, scope, requestID, err)
				}
			},
		}, nil
	}
	return &AcquireResult{Acquired: false}, nil
}

// AcquireUserSlot attempts to acquire a concurrency slot for a user.
// If the user is at max concurrency, it waits until a slot is available or timeout.
// Returns a release function that MUST be called when the request completes.
func (s *ConcurrencyService) AcquireUserSlot(ctx context.Context, userID int64, maxConcurrency int) (*AcquireResult, error) {
	if maxConcurrency <= 0 {
		return s.trackUnlimitedUserSlot(ctx, userID), nil
	}

	// Generate unique request ID for this slot
	requestID := generateRequestID()

	acquired, err := s.cache.AcquireUserSlot(ctx, userID, maxConcurrency, requestID)
	if err != nil {
		return nil, err
	}

	if acquired {
		return &AcquireResult{
			Acquired:  true,
			RequestID: requestID,
			ReleaseFunc: func() {
				bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := s.cache.ReleaseUserSlot(bgCtx, userID, requestID); err != nil {
					logger.LegacyPrintf("service.concurrency", "Warning: failed to release user slot for %d (req=%s): %v", userID, requestID, err)
				}
			},
		}, nil
	}

	return &AcquireResult{
		Acquired:    false,
		ReleaseFunc: nil,
	}, nil
}

// UserSlotTracker records unlimited users in the existing concurrency set.
// It has no admission authority and must never reject a business request.
type UserSlotTracker interface {
	TrackUserSlot(context.Context, int64, string) error
	ReleaseTrackedUserSlot(context.Context, int64, string) error
}

func (s *ConcurrencyService) trackUnlimitedUserSlot(ctx context.Context, userID int64) *AcquireResult {
	result := &AcquireResult{Acquired: true, ReleaseFunc: func() {}}
	if s == nil || s.cache == nil || userID <= 0 {
		return result
	}
	tracker, ok := s.cache.(UserSlotTracker)
	if !ok {
		return result
	}
	requestID := generateRequestID()
	trackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	err := tracker.TrackUserSlot(trackCtx, userID, requestID)
	cancel()
	if err != nil {
		logger.LegacyPrintf("service.concurrency", "Warning: failed to track user slot for %d: %v", userID, err)
	}
	// Release even after a timeout: Redis may have accepted the write.
	var once sync.Once
	result.RequestID = requestID
	result.ReleaseFunc = func() {
		once.Do(func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := tracker.ReleaseTrackedUserSlot(releaseCtx, userID, requestID); err != nil {
				logger.LegacyPrintf("service.concurrency", "Warning: failed to release tracked user slot for %d: %v", userID, err)
			}
		})
	}
	return result
}

// APIKeySlotAdmissionCache adds atomic admission to the shared statistics set.
// A cache lacking admission support cannot admit a key with a configured limit.
type APIKeySlotAdmissionCache interface {
	APIKeyConcurrencyCache
	AcquireAPIKeySlot(context.Context, int64, int, string) (bool, error)
}

// AcquireAPIKeySlot never waits for capacity. The caller owns ReleaseFunc,
// including after cancellation: WS turns retain their slots while draining.
// HTTP callers bind release to their request context at the helper boundary.
func (s *ConcurrencyService) AcquireAPIKeySlot(ctx context.Context, apiKeyID int64, maxConcurrency int) (*AcquireResult, error) {
	if maxConcurrency == 0 {
		return &AcquireResult{Acquired: true, ReleaseFunc: s.TrackAPIKeySlot(context.WithoutCancel(ctx), apiKeyID)}, nil
	}
	if s == nil || s.cache == nil || apiKeyID <= 0 || maxConcurrency < 0 {
		return nil, fmt.Errorf("API key concurrency admission unavailable")
	}
	cache, ok := s.cache.(APIKeySlotAdmissionCache)
	if !ok {
		return nil, fmt.Errorf("API key concurrency admission unavailable")
	}
	requestID := generateRequestID()
	leaseCache, ok := s.cache.(APIKeySlotLeaseCache)
	owner, ownerOK := apiKeyAdmissionOwnerFromContext(ctx)
	if !ok || leaseCache.APIKeySlotTTL() <= 2*time.Second || leaseCache.APIKeySlotRefreshInterval() <= 0 || !ownerOK {
		return nil, fmt.Errorf("API key concurrency requires a renewable lease and cancellation owner")
	}
	started := time.Now()
	acquireCtx, cancel := context.WithTimeout(ctx, apiKeySlotTrackTimeout)
	acquired, err := cache.AcquireAPIKeySlot(acquireCtx, apiKeyID, maxConcurrency, requestID)
	cancel()
	if err != nil {
		// The write may have succeeded even if its response was lost.
		releaseAPIKeySlot(cache, apiKeyID, requestID)
		return nil, fmt.Errorf("acquire API key %d concurrency slot: %w", apiKeyID, err)
	}
	if !acquired {
		return &AcquireResult{Acquired: false}, nil
	}
	return &AcquireResult{Acquired: true, ReleaseFunc: keepEnforcedAPIKeySlot(owner, leaseCache, apiKeyID, requestID, started)}, nil
}

// TrackAPIKeySlot records one active request slot for an API key without
// applying key-level concurrency limits. It is fail-open: Redis errors are
// logged without blocking the request; even ambiguous writes retain cleanup.
func (s *ConcurrencyService) TrackAPIKeySlot(ctx context.Context, apiKeyID int64) func() {
	_, release, _ := s.TrackAPIKeySlotOwned(ctx, apiKeyID)
	return release
}

// TrackAPIKeySlotOwned returns the exact stats member identity plus release and
// stop-only pause handles. The Live transfer needs the identity to move the
// stats member atomically into the Live lease without an orphan renewal worker.
func (s *ConcurrencyService) TrackAPIKeySlotOwned(ctx context.Context, apiKeyID int64) (string, func(), func()) {
	if s == nil || s.cache == nil || apiKeyID <= 0 {
		return "", func() {}, func() {}
	}
	cache, ok := s.cache.(APIKeyConcurrencyCache)
	if !ok {
		return "", func() {}, func() {}
	}

	requestID := generateRequestID()
	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	trackCtx, cancel := context.WithTimeout(baseCtx, apiKeySlotTrackTimeout)
	err := cache.TrackAPIKeySlot(trackCtx, apiKeyID, requestID)
	cancel()
	if err != nil {
		logger.LegacyPrintf("service.concurrency", "Warning: failed to track api key slot for %d (req=%s): %v", apiKeyID, requestID, err)
	}

	release, pause := keepAPIKeySlotState(ctx, cache, apiKeyID, requestID)
	return requestID, release, pause
}

// GetAPIKeyConcurrencyBatch gets real-time active request counts for API keys.
// Stats are best-effort: missing Redis support or Redis errors return zeroes.
func (s *ConcurrencyService) GetAPIKeyConcurrencyBatch(ctx context.Context, apiKeyIDs []int64) (map[int64]int, error) {
	result := zeroAPIKeyConcurrencyMap(apiKeyIDs)
	if len(apiKeyIDs) == 0 {
		return result, nil
	}
	if s == nil || s.cache == nil {
		return result, nil
	}
	cache, ok := s.cache.(APIKeyConcurrencyCache)
	if !ok {
		return result, nil
	}

	redisCtx, cancel := context.WithTimeout(context.Background(), apiKeyConcurrencyFetchTimeout)
	defer cancel()

	counts, err := cache.GetAPIKeyConcurrencyBatch(redisCtx, apiKeyIDs)
	if err != nil {
		logger.LegacyPrintf("service.concurrency", "Warning: get api key concurrency batch failed: %v", err)
		return result, nil
	}
	for _, apiKeyID := range apiKeyIDs {
		result[apiKeyID] = counts[apiKeyID]
	}
	return result, nil
}

func zeroAPIKeyConcurrencyMap(apiKeyIDs []int64) map[int64]int {
	result := make(map[int64]int, len(apiKeyIDs))
	for _, apiKeyID := range apiKeyIDs {
		result[apiKeyID] = 0
	}
	return result
}

// ============================================
// Wait Queue Count Methods
// ============================================

// IncrementWaitCount attempts to increment the wait queue counter for a user.
// Returns true if successful, false if the wait queue is full.
// maxWait should be user.Concurrency + defaultExtraWaitSlots
func (s *ConcurrencyService) IncrementWaitCount(ctx context.Context, userID int64, maxWait int) (bool, error) {
	if s.cache == nil {
		// Redis not available, allow request
		return true, nil
	}

	result, err := s.cache.IncrementWaitCount(ctx, userID, maxWait)
	if err != nil {
		// On error, allow the request to proceed (fail open)
		logger.LegacyPrintf("service.concurrency", "Warning: increment wait count failed for user %d: %v", userID, err)
		return true, nil
	}
	return result, nil
}

// DecrementWaitCount decrements the wait queue counter for a user.
// Should be called when a request completes or exits the wait queue.
func (s *ConcurrencyService) DecrementWaitCount(ctx context.Context, userID int64) {
	if s.cache == nil {
		return
	}

	// Use background context to ensure decrement even if original context is cancelled
	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.cache.DecrementWaitCount(bgCtx, userID); err != nil {
		logger.LegacyPrintf("service.concurrency", "Warning: decrement wait count failed for user %d: %v", userID, err)
	}
}

// IncrementAccountWaitCount increments the wait queue counter for an account.
func (s *ConcurrencyService) IncrementAccountWaitCount(ctx context.Context, accountID int64, maxWait int) (bool, error) {
	if s.cache == nil {
		return true, nil
	}

	result, err := s.cache.IncrementAccountWaitCount(ctx, accountID, maxWait)
	if err != nil {
		logger.LegacyPrintf("service.concurrency", "Warning: increment wait count failed for account %d: %v", accountID, err)
		return true, nil
	}
	return result, nil
}

// DecrementAccountWaitCount decrements the wait queue counter for an account.
func (s *ConcurrencyService) DecrementAccountWaitCount(ctx context.Context, accountID int64) {
	if s.cache == nil {
		return
	}

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.cache.DecrementAccountWaitCount(bgCtx, accountID); err != nil {
		logger.LegacyPrintf("service.concurrency", "Warning: decrement wait count failed for account %d: %v", accountID, err)
	}
}

// GetAccountWaitingCount gets current wait queue count for an account.
func (s *ConcurrencyService) GetAccountWaitingCount(ctx context.Context, accountID int64) (int, error) {
	if s.cache == nil {
		return 0, nil
	}
	return s.cache.GetAccountWaitingCount(ctx, accountID)
}

// CalculateMaxWait calculates the maximum wait queue size for a user
// maxWait = userConcurrency + defaultExtraWaitSlots
func CalculateMaxWait(userConcurrency int) int {
	if userConcurrency <= 0 {
		userConcurrency = 1
	}
	return userConcurrency + defaultExtraWaitSlots
}

// GetAccountsLoadBatch 批量获取账号负载信息。
func (s *ConcurrencyService) GetAccountsLoadBatch(ctx context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	return s.getAccountsLoadBatch(ctx, accounts, true)
}

// GetAccountsLoadBatchFresh 绕过极短 TTL 缓存，用于抢槽失败后的实时刷新兜底。
func (s *ConcurrencyService) GetAccountsLoadBatchFresh(ctx context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	return s.getAccountsLoadBatch(ctx, accounts, false)
}

func (s *ConcurrencyService) getAccountsLoadBatch(ctx context.Context, accounts []AccountWithConcurrency, allowCache bool) (map[int64]*AccountLoadInfo, error) {
	if len(accounts) == 0 {
		return map[int64]*AccountLoadInfo{}, nil
	}
	if s.cache == nil {
		return map[int64]*AccountLoadInfo{}, nil
	}

	ttl := time.Duration(s.accountLoadCacheTTL.Load())
	if !allowCache || ttl <= 0 {
		return s.fetchAccountsLoadBatch(ctx, accounts)
	}

	key := accountLoadBatchCacheKey(accounts)
	if cached, ok := s.getCachedAccountLoadBatch(key, time.Now()); ok {
		return cached, nil
	}

	value, err, _ := s.accountLoadGroup.Do(key, func() (any, error) {
		now := time.Now()
		if cached, ok := s.getCachedAccountLoadBatch(key, now); ok {
			return cached, nil
		}
		loadMap, fetchErr := s.fetchAccountsLoadBatch(ctx, accounts)
		if fetchErr != nil {
			return nil, fetchErr
		}
		cached := cloneAccountLoadMap(loadMap)
		s.storeCachedAccountLoadBatch(key, cached, now.Add(ttl))
		return cached, nil
	})
	if err != nil {
		return nil, err
	}
	loadMap, _ := value.(map[int64]*AccountLoadInfo)
	if loadMap == nil {
		return map[int64]*AccountLoadInfo{}, nil
	}
	return loadMap, nil
}

func (s *ConcurrencyService) fetchAccountsLoadBatch(ctx context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	if s.cache == nil {
		return map[int64]*AccountLoadInfo{}, nil
	}
	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	redisCtx, cancel := context.WithTimeout(baseCtx, accountLoadBatchFetchTimeout)
	defer cancel()
	return s.cache.GetAccountsLoadBatch(redisCtx, accounts)
}

func (s *ConcurrencyService) getCachedAccountLoadBatch(key string, now time.Time) (map[int64]*AccountLoadInfo, bool) {
	s.accountLoadCacheMu.RLock()
	cached, ok := s.accountLoadCache[key]
	s.accountLoadCacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	if !now.Before(cached.expiresAt) {
		s.accountLoadCacheMu.Lock()
		if current, exists := s.accountLoadCache[key]; exists && !now.Before(current.expiresAt) {
			delete(s.accountLoadCache, key)
		}
		s.accountLoadCacheMu.Unlock()
		return nil, false
	}
	return cached.loadMap, true
}

func (s *ConcurrencyService) storeCachedAccountLoadBatch(key string, loadMap map[int64]*AccountLoadInfo, expiresAt time.Time) {
	s.accountLoadCacheMu.Lock()
	if s.accountLoadCache == nil {
		s.accountLoadCache = make(map[string]cachedAccountLoadBatch)
	}
	if len(s.accountLoadCache) >= maxAccountLoadBatchCacheEntries {
		now := time.Now()
		for cacheKey, cached := range s.accountLoadCache {
			if !now.Before(cached.expiresAt) {
				delete(s.accountLoadCache, cacheKey)
			}
		}
		for len(s.accountLoadCache) >= maxAccountLoadBatchCacheEntries {
			for cacheKey := range s.accountLoadCache {
				delete(s.accountLoadCache, cacheKey)
				break
			}
		}
	}
	s.accountLoadCache[key] = cachedAccountLoadBatch{
		loadMap:   loadMap,
		expiresAt: expiresAt,
	}
	s.accountLoadCacheMu.Unlock()
}

func accountLoadBatchCacheKey(accounts []AccountWithConcurrency) string {
	hash := sha256.New()
	var buf [16]byte
	for _, account := range accounts {
		binary.LittleEndian.PutUint64(buf[:8], uint64(account.ID))
		binary.LittleEndian.PutUint64(buf[8:], uint64(int64(account.MaxConcurrency)))
		_, _ = hash.Write(buf[:])
	}
	sum := hash.Sum(nil)
	return strconv.Itoa(len(accounts)) + ":" + hex.EncodeToString(sum)
}

func cloneAccountLoadMap(loadMap map[int64]*AccountLoadInfo) map[int64]*AccountLoadInfo {
	if len(loadMap) == 0 {
		return map[int64]*AccountLoadInfo{}
	}
	clone := make(map[int64]*AccountLoadInfo, len(loadMap))
	for accountID, loadInfo := range loadMap {
		if loadInfo == nil {
			clone[accountID] = nil
			continue
		}
		copied := *loadInfo
		clone[accountID] = &copied
	}
	return clone
}

// GetUsersLoadBatch returns load info for multiple users.
func (s *ConcurrencyService) GetUsersLoadBatch(ctx context.Context, users []UserWithConcurrency) (map[int64]*UserLoadInfo, error) {
	if s.cache == nil {
		return map[int64]*UserLoadInfo{}, nil
	}
	return s.cache.GetUsersLoadBatch(ctx, users)
}

// CleanupExpiredAccountSlots removes expired slots for one account (background task).
func (s *ConcurrencyService) CleanupExpiredAccountSlots(ctx context.Context, accountID int64) error {
	if s.cache == nil {
		return nil
	}
	return s.cache.CleanupExpiredAccountSlots(ctx, accountID)
}

// StartSlotCleanupWorker starts a background cleanup worker for expired account slots.
func (s *ConcurrencyService) StartSlotCleanupWorker(_ AccountRepository, interval time.Duration) {
	if s == nil || s.cache == nil || interval <= 0 {
		return
	}

	stopCtx := s.apiKeyQueueStopContext()
	if stopCtx == nil {
		stopCtx = context.Background()
	}
	runCleanup := func() {
		if stopCtx.Err() != nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(stopCtx, 5*time.Second)
		err := s.cache.CleanupExpiredAccountSlotKeys(cleanupCtx)
		cancel()
		if err != nil && stopCtx.Err() == nil {
			logger.LegacyPrintf("service.concurrency", "Warning: cleanup expired account slots failed: %v", err)
			return
		}
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		runCleanup()
		for {
			select {
			case <-stopCtx.Done():
				return
			case <-ticker.C:
				runCleanup()
			}
		}
	}()
}

// GetAccountConcurrencyBatch gets current concurrency counts for multiple accounts.
// Uses a detached context with timeout to prevent HTTP request cancellation from
// causing the entire batch to fail (which would show all concurrency as 0).
func (s *ConcurrencyService) GetAccountConcurrencyBatch(ctx context.Context, accountIDs []int64) (map[int64]int, error) {
	if len(accountIDs) == 0 {
		return map[int64]int{}, nil
	}
	if s.cache == nil {
		result := make(map[int64]int, len(accountIDs))
		for _, accountID := range accountIDs {
			result[accountID] = 0
		}
		return result, nil
	}

	// Use a detached context so that a cancelled HTTP request doesn't cause
	// the Redis pipeline to fail and return all-zero concurrency counts.
	redisCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return s.cache.GetAccountConcurrencyBatch(redisCtx, accountIDs)
}

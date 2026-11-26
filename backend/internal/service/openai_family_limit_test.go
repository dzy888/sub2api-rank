//go:build unit

package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func familyLimitHeaders(pairs ...string) http.Header {
	headers := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		headers.Set(pairs[i], pairs[i+1])
	}
	return headers
}

func TestParseCodexLimitFamilies(t *testing.T) {
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "31.5",
		"x-codex-primary-reset-after-seconds", "3600",
		"x-codex-other-primary-used-percent", "72",
		"x-codex-other-primary-reset-after-seconds", "1200",
		"x-codex-other-secondary-used-percent", "12",
		// 非家族头必须被忽略。
		"x-codex-turn-state", "not-a-family",
		"x-codex-window-id", "thread:1",
	)

	families := parseCodexLimitFamilies(headers)
	require.Len(t, families, 2)

	require.Contains(t, families, codexDefaultLimitFamily)
	require.Equal(t, 31.5, *families[codexDefaultLimitFamily].PrimaryUsedPercent)
	require.Equal(t, 3600, *families[codexDefaultLimitFamily].PrimaryResetAfter)

	require.Contains(t, families, "codex-other")
	other := families["codex-other"]
	require.Equal(t, 72.0, *other.PrimaryUsedPercent)
	require.Equal(t, 12.0, *other.SecondaryUsedPercent)
	require.Equal(t, 1200, *other.PrimaryResetAfter)
}

func TestParseCodexLimitFamiliesIgnoresMalformedValues(t *testing.T) {
	headers := familyLimitHeaders(
		"x-codex-other-primary-used-percent", "not-a-number",
		"x-codex-other-secondary-reset-after-seconds", "120",
	)
	families := parseCodexLimitFamilies(headers)
	require.Len(t, families, 1)
	require.Nil(t, families["codex-other"].PrimaryUsedPercent)
	require.Equal(t, 120, *families["codex-other"].SecondaryResetAfter)
}

func TestExhaustedCodexLimitFamilies(t *testing.T) {
	now := time.Now()
	snapshot := &OpenAICodexUsageSnapshot{Families: map[string]CodexLimitFamily{
		codexDefaultLimitFamily: {PrimaryUsedPercent: f64(90), PrimaryResetAfter: iptr(600)},
		"codex-other":           {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(1800)},
	}}

	exhausted := exhaustedCodexLimitFamilies(snapshot, now)
	require.Len(t, exhausted, 1)
	require.Contains(t, exhausted, "codex-other")
	require.WithinDuration(t, now.Add(30*time.Minute), exhausted["codex-other"], 2*time.Second)
	require.True(t, hasNonDefaultExhaustedCodexFamily(snapshot, now))

	// 默认家族耗尽时不属于「非默认家族耗尽」。
	defaultOnly := &OpenAICodexUsageSnapshot{Families: map[string]CodexLimitFamily{
		codexDefaultLimitFamily: {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(600)},
	}}
	require.False(t, hasNonDefaultExhaustedCodexFamily(defaultOnly, now))
}

func TestOpenAIFamilyModelTrackerLearnsFromExhaustion(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	now := time.Now()
	snapshot := &OpenAICodexUsageSnapshot{Families: map[string]CodexLimitFamily{
		codexDefaultLimitFamily: {PrimaryUsedPercent: f64(40), PrimaryResetAfter: iptr(600)},
		"codex-other":           {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(1800)},
	}}

	families := openAIFamilyModels.LearnExhaustedFamilies(7, "gpt-6.1-sol", snapshot, now)
	require.Equal(t, map[string][]string{"codex-other": {"gpt-6.1-sol"}}, families)
	require.Equal(t, "codex-other", openAIFamilyModels.FamilyForModel(7, "gpt-6.1-sol"))
	require.Empty(t, openAIFamilyModels.FamilyForModel(7, "gpt-6.2-sol"))

	// 再学一个同家族模型后，两个模型都应被返回。
	families = openAIFamilyModels.LearnExhaustedFamilies(7, "gpt-5.6-sol", snapshot, now)
	require.Equal(t, []string{"gpt-5.6-sol", "gpt-6.1-sol"}, families["codex-other"])
}

func TestOpenAIFamilyModelTrackerKeepsFirstMapping(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	now := time.Now()
	other := &OpenAICodexUsageSnapshot{Families: map[string]CodexLimitFamily{
		codexDefaultLimitFamily: {PrimaryUsedPercent: f64(10)},
		"codex-other":           {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(60)},
	}}
	_ = openAIFamilyModels.LearnExhaustedFamilies(9, "gpt-6.1-sol", other, now)

	// 归属已确认后，默认家族耗尽不会改写它。
	def := &OpenAICodexUsageSnapshot{Families: map[string]CodexLimitFamily{
		codexDefaultLimitFamily: {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(60)},
	}}
	require.Empty(t, openAIFamilyModels.LearnExhaustedFamilies(9, "gpt-6.1-sol", def, now),
		"默认家族不参与家族级归因")
	require.Equal(t, "codex-other", openAIFamilyModels.FamilyForModel(9, "gpt-6.1-sol"))
}

func TestOpenAIFamilyModelTrackerLearnsFromUsageDelta(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	// 第一次只建立基线样本。
	openAIFamilyModels.Observe(11, "", parseCodexLimitFamilies(familyLimitHeaders(
		"x-codex-primary-used-percent", "10",
		"x-codex-other-primary-used-percent", "20",
	)))
	// 第二次 codex-other 上涨，归属应学到这里。
	openAIFamilyModels.Observe(11, "gpt-6.2-sol", parseCodexLimitFamilies(familyLimitHeaders(
		"x-codex-primary-used-percent", "10",
		"x-codex-other-primary-used-percent", "24",
	)))
	require.Equal(t, "codex-other", openAIFamilyModels.FamilyForModel(11, "gpt-6.2-sol"))
}

func TestOpenAIFamilyModelTrackerSingleFamilyIsUnambiguous(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	openAIFamilyModels.Observe(12, "gpt-7", parseCodexLimitFamilies(familyLimitHeaders(
		"x-codex-other-primary-used-percent", "5",
	)))
	require.Equal(t, "codex-other", openAIFamilyModels.FamilyForModel(12, "gpt-7"))
}

type familyCooldownRepoStub struct {
	*modelNotFoundAccountRepoStub
	rateLimitedResets []time.Time
}

func (r *familyCooldownRepoStub) SetRateLimited(_ context.Context, _ int64, resetAt time.Time) error {
	r.rateLimitedResets = append(r.rateLimitedResets, resetAt)
	return nil
}

func familyCooldownAccount() *Account {
	return &Account{
		ID:       4242,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Status:   StatusActive,
		Extra:    map[string]any{},
		Credentials: map[string]any{
			"access_token":       "synthetic-access",
			"chatgpt_account_id": "synthetic-workspace",
			"refresh_token":      "synthetic-refresh",
			"expires_at":         time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		},
	}
}

func TestHandle429CooldownScopesNonDefaultFamilyToItsModels(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	ctx := withTempUnschedulableModel(context.Background(), []string{"gpt-6.1-sol"})
	repo := &familyCooldownRepoStub{modelNotFoundAccountRepoStub: &modelNotFoundAccountRepoStub{}}
	svc := &RateLimitService{accountRepo: repo}
	account := familyCooldownAccount()
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "30",
		"x-codex-primary-reset-after-seconds", "3600",
		"x-codex-other-primary-used-percent", "100",
		"x-codex-other-primary-reset-after-seconds", "1800",
	)

	svc.handle429Cooldown(ctx, account, headers, nil)

	require.Empty(t, repo.rateLimitedResets, "非默认家族耗尽不应阻塞整个账号")
	require.Len(t, repo.modelRateLimitCalls, 1)
	call := repo.modelRateLimitCalls[0]
	require.Equal(t, account.ID, call.accountID)
	require.Equal(t, "gpt-6.1-sol", call.scope)
	require.Contains(t, call.reason, "codex-other")
	require.WithinDuration(t, time.Now().Add(30*time.Minute), call.resetAt, 5*time.Second)

	// 当前实例的账号快照必须立刻生效，否则同进程内还会继续选中该模型。
	require.True(t, account.isRateLimitActiveForKey("gpt-6.1-sol"))
	require.False(t, account.isRateLimitActiveForKey("gpt-6.2-sol"))
}

func TestHandle429CooldownCoolsEachExhaustedFamilySeparately(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	now := time.Now()
	_ = openAIFamilyModels.LearnExhaustedFamilies(4242, "model-a", &OpenAICodexUsageSnapshot{
		Families: map[string]CodexLimitFamily{
			"codex-other": {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(600)},
		},
	}, now)
	_ = openAIFamilyModels.LearnExhaustedFamilies(4242, "model-b", &OpenAICodexUsageSnapshot{
		Families: map[string]CodexLimitFamily{
			"codex-mini": {PrimaryUsedPercent: f64(100), PrimaryResetAfter: iptr(1200)},
		},
	}, now)

	repo := &familyCooldownRepoStub{modelNotFoundAccountRepoStub: &modelNotFoundAccountRepoStub{}}
	svc := &RateLimitService{accountRepo: repo}
	account := familyCooldownAccount()
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "10",
		"x-codex-other-primary-used-percent", "100",
		"x-codex-other-primary-reset-after-seconds", "600",
		"x-codex-mini-primary-used-percent", "100",
		"x-codex-mini-primary-reset-after-seconds", "1200",
	)

	svc.handle429Cooldown(context.Background(), account, headers, nil)

	require.Empty(t, repo.rateLimitedResets)
	require.Len(t, repo.modelRateLimitCalls, 2)
	byScope := map[string]modelNotFoundRateLimitCall{}
	for _, call := range repo.modelRateLimitCalls {
		byScope[call.scope] = call
	}
	require.Contains(t, byScope, "model-a")
	require.Contains(t, byScope, "model-b")
	require.Contains(t, byScope["model-a"].reason, "codex-other")
	require.Contains(t, byScope["model-b"].reason, "codex-mini")
	require.WithinDuration(t, time.Now().Add(10*time.Minute), byScope["model-a"].resetAt, 5*time.Second)
	require.WithinDuration(t, time.Now().Add(20*time.Minute), byScope["model-b"].resetAt, 5*time.Second)
}

func TestHandle429CooldownKeepsAccountScopeForDefaultFamily(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	ctx := withTempUnschedulableModel(context.Background(), []string{"gpt-6.1-sol"})
	repo := &familyCooldownRepoStub{modelNotFoundAccountRepoStub: &modelNotFoundAccountRepoStub{}}
	svc := &RateLimitService{accountRepo: repo}
	account := familyCooldownAccount()
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "100",
		"x-codex-primary-reset-after-seconds", "3600",
	)

	svc.handle429Cooldown(ctx, account, headers, nil)

	require.Len(t, repo.rateLimitedResets, 1, "默认家族耗尽仍按整号冷却")
	require.Empty(t, repo.modelRateLimitCalls)
	require.WithinDuration(t, time.Now().Add(time.Hour), repo.rateLimitedResets[0], 5*time.Second)
}

func TestHandle429CooldownFallsBackWhenFamilyHasNoReset(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	ctx := withTempUnschedulableModel(context.Background(), []string{"gpt-6.1-sol"})
	repo := &familyCooldownRepoStub{modelNotFoundAccountRepoStub: &modelNotFoundAccountRepoStub{}}
	svc := &RateLimitService{accountRepo: repo}
	account := familyCooldownAccount()
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "30",
		"x-codex-other-primary-used-percent", "100",
	)

	svc.handle429Cooldown(ctx, account, headers, nil)

	// 没有重置时间就给不出可解释的窗口，交回秒级兜底（整号短冷却）。
	require.Empty(t, repo.modelRateLimitCalls)
	require.Len(t, repo.rateLimitedResets, 1)
}

// retryingFamilyBlocker 模拟「同账号 429 重试窗口仍然生效」。
type retryingFamilyBlocker struct {
	retry          bool
	schedulingStop int
}

func (b *retryingFamilyBlocker) BlockAccountScheduling(*Account, time.Time, string) {
	b.schedulingStop++
}

func (b *retryingFamilyBlocker) ClearAccountSchedulingBlock(int64) {}

func (b *retryingFamilyBlocker) ShouldRetryOpenAIOAuth429(*Account, http.Header, []byte) bool {
	return b.retry
}

func TestHandle429AppliesFamilyScopeInsideRetryWindow(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	ctx := withTempUnschedulableModel(context.Background(), []string{"gpt-6.1-sol"})
	repo := &familyCooldownRepoStub{modelNotFoundAccountRepoStub: &modelNotFoundAccountRepoStub{}}
	blocker := &retryingFamilyBlocker{retry: true}
	svc := &RateLimitService{accountRepo: repo, runtimeBlocker: blocker}
	account := familyCooldownAccount()
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "30",
		"x-codex-primary-reset-after-seconds", "3600",
		"x-codex-other-primary-used-percent", "100",
		"x-codex-other-primary-reset-after-seconds", "1800",
	)

	svc.handle429(ctx, account, headers, nil)

	require.Len(t, repo.modelRateLimitCalls, 1, "重试窗口内也必须落模型级冷却")
	require.Equal(t, "gpt-6.1-sol", repo.modelRateLimitCalls[0].scope)
	require.Zero(t, blocker.schedulingStop, "模型级冷却不得阻塞整号")
}

func TestHandle429KeepsRetryWindowEarlyReturnForDefaultFamily(t *testing.T) {
	resetOpenAIFamilyModelTracker()
	ctx := withTempUnschedulableModel(context.Background(), []string{"gpt-6.1-sol"})
	repo := &familyCooldownRepoStub{modelNotFoundAccountRepoStub: &modelNotFoundAccountRepoStub{}}
	blocker := &retryingFamilyBlocker{retry: true}
	svc := &RateLimitService{accountRepo: repo, runtimeBlocker: blocker}
	account := familyCooldownAccount()
	headers := familyLimitHeaders(
		"x-codex-primary-used-percent", "100",
		"x-codex-primary-reset-after-seconds", "3600",
	)

	svc.handle429(ctx, account, headers, nil)

	require.Empty(t, repo.modelRateLimitCalls, "默认家族在重试窗口内仍交给重试恢复")
	require.Empty(t, repo.rateLimitedResets)
	require.Zero(t, blocker.schedulingStop)
}

func f64(v float64) *float64 { return &v }
func iptr(v int) *int        { return &v }

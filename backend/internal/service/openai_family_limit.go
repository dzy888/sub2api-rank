package service

import (
	"context"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// Codex 的限流额度按 limit_id「家族」计，而不是按账号或单个模型计。
// 默认家族的响应头不带 limit_id 段（x-codex-primary-*），其它家族带
// （x-<limit_id>-primary-*）。codex 源码 parse_all_rate_limits 会遍历全部
// 家族；这里保持同一口径，否则非默认家族耗尽时我们完全看不见。
const (
	codexDefaultLimitFamily = "codex"
	// openAIFamilyUsageSampleRetention 限制「按涨幅推断模型家族」的历史样本窗口，
	// 避免窗口重置后旧快照与新窗口读数比较而产生负数或虚假涨幅。
	openAIFamilyUsageSampleRetention = 30 * time.Minute
)

// codexLimitFamilyHeaderPattern 匹配 x-<limit_id>-{primary,secondary}-<field>。
// 默认家族的 limit_id 就是 codex（x-codex-primary-used-percent）。
// 其它头（x-codex-turn-state、x-codex-window-id 等）后缀不匹配，不会误判。
var codexLimitFamilyHeaderPattern = regexp.MustCompile(
	`^x-(.+)-(primary|secondary)-(used-percent|reset-after-seconds|window-minutes|over-secondary-limit-percent)$`,
)

// CodexLimitFamily 是某个 limit_id 家族在最近一次响应头里的读数。
type CodexLimitFamily struct {
	PrimaryUsedPercent   *float64
	PrimaryResetAfter    *int
	PrimaryWindowMinutes *int
	SecondaryUsedPercent *float64
	SecondaryResetAfter  *int
	SecondaryWindowMin   *int
	PrimaryOverSecondary *float64
}

// parseCodexLimitFamilies 扫描全部 x-<limit_id>-* 家族头。
// 与 ParseCodexRateLimitHeaders 里的默认家族解析互不影响：默认家族会同时出现在
// 旧字段和 Families["codex"] 中，调用方按需要选择口径。
func parseCodexLimitFamilies(headers http.Header) map[string]CodexLimitFamily {
	if len(headers) == 0 {
		return nil
	}
	families := make(map[string]CodexLimitFamily)
	for key, values := range headers {
		if len(values) == 0 {
			continue
		}
		match := codexLimitFamilyHeaderPattern.FindStringSubmatch(strings.ToLower(strings.TrimSpace(key)))
		if match == nil {
			continue
		}
		family := strings.TrimSpace(match[1])
		if family == "" {
			continue
		}
		raw := strings.TrimSpace(values[0])
		if raw == "" {
			continue
		}
		entry := families[family]
		switch match[3] {
		case "used-percent":
			value, ok := codexHeaderFloat(raw)
			if !ok {
				continue
			}
			if match[2] == "primary" {
				entry.PrimaryUsedPercent = value
			} else {
				entry.SecondaryUsedPercent = value
			}
		case "reset-after-seconds":
			value, ok := codexHeaderInt(raw)
			if !ok {
				continue
			}
			if match[2] == "primary" {
				entry.PrimaryResetAfter = value
			} else {
				entry.SecondaryResetAfter = value
			}
		case "window-minutes":
			value, ok := codexHeaderInt(raw)
			if !ok {
				continue
			}
			if match[2] == "primary" {
				entry.PrimaryWindowMinutes = value
			} else {
				entry.SecondaryWindowMin = value
			}
		case "over-secondary-limit-percent":
			if match[2] != "primary" {
				continue
			}
			value, ok := codexHeaderFloat(raw)
			if !ok {
				continue
			}
			entry.PrimaryOverSecondary = value
		default:
			continue
		}
		families[family] = entry
	}
	if len(families) == 0 {
		return nil
	}
	return families
}

func codexHeaderFloat(raw string) (*float64, bool) {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, false
	}
	return &value, true
}

func codexHeaderInt(raw string) (*int, bool) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return nil, false
	}
	return &value, true
}

// usedPercent 返回该家族最吃紧的已用百分比；没有任何读数时返回 0。
func (f CodexLimitFamily) usedPercent() float64 {
	used := 0.0
	if f.PrimaryUsedPercent != nil {
		used = math.Max(used, *f.PrimaryUsedPercent)
	}
	if f.SecondaryUsedPercent != nil {
		used = math.Max(used, *f.SecondaryUsedPercent)
	}
	return used
}

// exhaustedReset 报告该家族是否已耗尽（任一窗口 >= 100），并给出可用的重置时间。
// ok 为 true 但 at 为零值时表示「已耗尽但上游没给重置时间」。
func (f CodexLimitFamily) exhaustedReset(now time.Time) (time.Time, bool) {
	primary := f.PrimaryUsedPercent != nil && *f.PrimaryUsedPercent >= 100
	secondary := f.SecondaryUsedPercent != nil && *f.SecondaryUsedPercent >= 100
	switch {
	case primary && f.PrimaryResetAfter != nil:
		return now.Add(time.Duration(*f.PrimaryResetAfter) * time.Second), true
	case secondary && f.SecondaryResetAfter != nil:
		return now.Add(time.Duration(*f.SecondaryResetAfter) * time.Second), true
	case primary || secondary:
		return time.Time{}, true
	}
	return time.Time{}, false
}

// exhaustedCodexLimitFamilies 返回所有已耗尽的家族及其重置时间。
func exhaustedCodexLimitFamilies(snapshot *OpenAICodexUsageSnapshot, now time.Time) map[string]time.Time {
	if snapshot == nil || len(snapshot.Families) == 0 {
		return nil
	}
	exhausted := make(map[string]time.Time)
	for family, entry := range snapshot.Families {
		if resetAt, ok := entry.exhaustedReset(now); ok {
			exhausted[family] = resetAt
		}
	}
	if len(exhausted) == 0 {
		return nil
	}
	return exhausted
}

// hasNonDefaultExhaustedCodexFamily 报告是否存在「非默认家族」耗尽。
// 默认家族承载所有未显式归属的模型，它耗尽时仍按整号冷却，故这里只看其它家族。
func hasNonDefaultExhaustedCodexFamily(snapshot *OpenAICodexUsageSnapshot, now time.Time) bool {
	for family := range exhaustedCodexLimitFamilies(snapshot, now) {
		if family != codexDefaultLimitFamily {
			return true
		}
	}
	return false
}

// codexLimitFamiliesExtra 把家族读数压成可写入 account.Extra 的 JSON 友好结构，
// 便于在管理端 / 日志里核对「哪个家族在烧额度」。
//
// 只有默认家族时返回 nil：非 window 字段在快照写入路径里走 display 分支，
// 一旦有值，被合并丢弃的旧包也会产生一次 UpdateExtra。默认家族的读数已经被
// codex_primary_*/codex_secondary_* 覆盖，没必要再复制一份，更没必要因此
// 让每个旧包都多一次写库。
func codexLimitFamiliesExtra(families map[string]CodexLimitFamily) map[string]any {
	if len(families) == 0 {
		return nil
	}
	nonDefault := false
	for name := range families {
		if name != codexDefaultLimitFamily {
			nonDefault = true
			break
		}
	}
	if !nonDefault {
		return nil
	}
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make(map[string]any, len(families))
	for _, name := range names {
		entry := families[name]
		item := map[string]any{"used_percent": entry.usedPercent()}
		if entry.PrimaryUsedPercent != nil {
			item["primary_used_percent"] = *entry.PrimaryUsedPercent
		}
		if entry.SecondaryUsedPercent != nil {
			item["secondary_used_percent"] = *entry.SecondaryUsedPercent
		}
		if entry.PrimaryResetAfter != nil {
			item["primary_reset_after_seconds"] = *entry.PrimaryResetAfter
		}
		if entry.SecondaryResetAfter != nil {
			item["secondary_reset_after_seconds"] = *entry.SecondaryResetAfter
		}
		out[name] = item
	}
	return out
}

// openAIFamilyUsageSample 是某个家族最近一次读到的额度占用，用于计算涨幅。
type openAIFamilyUsageSample struct {
	used float64
	at   time.Time
}

// openAIFamilyModelTracker 学习「出站模型 -> limit_id 家族」的归属。
//
// 归属无法从请求里直接得知：上游只在响应头里给家族额度，不告诉我们本次请求属于
// 哪个家族。可观测的信号有两个：
//  1. 家族 used_percent 的涨幅（本次请求消耗了谁的额度）；
//  2. 429 时哪个家族到达 100%（更强，且不需要历史样本）。
//
// 学习是单调的：一旦确认归属就不再改写，避免并发请求互相踩踏导致抖动。
// 未学习到的模型返回空家族，调用方应退化为现有账号级行为。
type openAIFamilyModelTracker struct {
	mu          sync.Mutex
	usage       map[int64]map[string]openAIFamilyUsageSample
	modelFamily map[int64]map[string]string
}

var openAIFamilyModels = newOpenAIFamilyModelTracker()

func newOpenAIFamilyModelTracker() *openAIFamilyModelTracker {
	return &openAIFamilyModelTracker{
		usage:       make(map[int64]map[string]openAIFamilyUsageSample),
		modelFamily: make(map[int64]map[string]string),
	}
}

// resetOpenAIFamilyModelTracker 仅供测试清理全局学习状态。
func resetOpenAIFamilyModelTracker() {
	openAIFamilyModels = newOpenAIFamilyModelTracker()
}

func (t *openAIFamilyModelTracker) accountState(accountID int64) (map[string]openAIFamilyUsageSample, map[string]string) {
	usage, ok := t.usage[accountID]
	if !ok {
		usage = make(map[string]openAIFamilyUsageSample)
		t.usage[accountID] = usage
	}
	models, ok := t.modelFamily[accountID]
	if !ok {
		models = make(map[string]string)
		t.modelFamily[accountID] = models
	}
	return usage, models
}

// Observe 记录一次家族额度读数，并尝试为 model 推断归属。
func (t *openAIFamilyModelTracker) Observe(accountID int64, model string, families map[string]CodexLimitFamily) {
	if t == nil || accountID <= 0 || len(families) == 0 {
		return
	}
	now := time.Now()
	model = strings.TrimSpace(model)

	t.mu.Lock()
	defer t.mu.Unlock()

	usage, models := t.accountState(accountID)

	bestFamily, bestDelta := "", 0.0
	for family, entry := range families {
		current := entry.usedPercent()
		previous, ok := usage[family]
		delta := 0.0
		if ok && now.Sub(previous.at) <= openAIFamilyUsageSampleRetention {
			delta = current - previous.used
		}
		if delta > bestDelta {
			bestFamily, bestDelta = family, delta
		}
		usage[family] = openAIFamilyUsageSample{used: current, at: now}
	}

	if model == "" {
		return
	}
	if _, known := models[model]; known {
		return
	}
	switch {
	case bestFamily != "" && bestDelta > 0:
		models[model] = bestFamily
	case len(families) == 1:
		// 只有一个家族有读数时归属是确定的。
		for family := range families {
			models[model] = family
		}
	}
}

// LearnExhaustedFamilies 在 429 时按「哪个非默认家族到达 100%」确定归属，
// 返回 家族 -> 应冷却的模型列表。
//
// 默认家族的归属信息量最低（未显式归属的模型都算它，无法据此隔离），因此不参与。
// 未知模型只归属到已用百分比最高的那个非默认家族，避免同一个模型被同时归到多个
// 家族而在两个窗口之间反复解冻。
func (t *openAIFamilyModelTracker) LearnExhaustedFamilies(accountID int64, model string, snapshot *OpenAICodexUsageSnapshot, now time.Time) map[string][]string {
	if t == nil || accountID <= 0 || snapshot == nil {
		return nil
	}
	exhausted := exhaustedCodexLimitFamilies(snapshot, now)
	if len(exhausted) == 0 {
		return nil
	}
	result := make(map[string][]string, len(exhausted))
	bestFamily, bestUsed := "", -1.0
	for family := range exhausted {
		if family == codexDefaultLimitFamily {
			continue
		}
		used := snapshot.Families[family].usedPercent()
		if used > bestUsed {
			bestFamily, bestUsed = family, used
		}
		if models := t.ModelsForFamily(accountID, family); len(models) > 0 {
			result[family] = models
		}
	}
	if bestFamily == "" {
		return nil
	}

	model = strings.TrimSpace(model)
	if model != "" && t.FamilyForModel(accountID, model) == "" {
		t.mu.Lock()
		_, models := t.accountState(accountID)
		models[model] = bestFamily
		t.mu.Unlock()
		result[bestFamily] = appendUniqueModel(result[bestFamily], model)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func appendUniqueModel(models []string, model string) []string {
	for _, existing := range models {
		if existing == model {
			return models
		}
	}
	models = append(models, model)
	sort.Strings(models)
	return models
}

// FamilyForModel 返回已学习到的家族归属；未学习到返回空串。
func (t *openAIFamilyModelTracker) FamilyForModel(accountID int64, model string) string {
	if t == nil || accountID <= 0 {
		return ""
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.modelFamily[accountID][model]
}

// ModelsForFamily 返回已归属到该家族的全部模型。
func (t *openAIFamilyModelTracker) ModelsForFamily(accountID int64, family string) []string {
	if t == nil || accountID <= 0 || family == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	models := make([]string, 0, 4)
	for model, mapped := range t.modelFamily[accountID] {
		if mapped == family {
			models = append(models, model)
		}
	}
	sort.Strings(models)
	return models
}

// openAIFamilyModelFromContext 取出 ctxkey.Model 上的请求模型，供成功响应侧
// 学习家族归属。取不到时返回空串（只更新家族读数样本，不写归属）。
func openAIFamilyModelFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	model, _ := ctx.Value(ctxkey.Model).(string)
	return strings.TrimSpace(model)
}

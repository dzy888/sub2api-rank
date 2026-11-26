package service

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// codexFingerprintIDsContextKey 是暂存在 gin context 的收敛 ID 集合键。
// 由 Forward（非透传）或 forwardOpenAIPassthrough（透传）解析后写入，请求
// 构造器读取用于出站头改写——请求体与出站头必须共享同一份 IDs，保证
// turn_id 等随机字段一致。
const codexFingerprintIDsContextKey = "codex_fingerprint_ids"

// stageCodexFingerprintIDs 将本 attempt 解析出的收敛 ID 暂存到 gin context。
// 必须无条件覆写（含 nil）：failover 从收敛账号切到 off 账号时，上一账号的
// IDs 不得残留并被误应用到新账号的出站头（typed-nil 由应用侧 nil 守卫吸收）。
func stageCodexFingerprintIDs(c *gin.Context, ids *codexFingerprintIDs) {
	if c != nil {
		c.Set(codexFingerprintIDsContextKey, ids)
	}
}

func stagedCodexFingerprintIDs(c *gin.Context, account *Account) *codexFingerprintIDs {
	if c == nil || account == nil || !account.UsesOpenAICodexProtocol() {
		return nil
	}
	value, ok := c.Get(codexFingerprintIDsContextKey)
	if !ok {
		return nil
	}
	ids, ok := value.(*codexFingerprintIDs)
	if !ok || ids == nil || ids.accountID != account.ID {
		return nil
	}
	return ids
}

// applyStagedCodexFingerprintHeaders 读取 context 暂存的收敛 ID 并改写出站头。
// 非透传与透传两个请求构造器共用本函数，防止应用语义漂移。仅解析该
// snapshot 的 OAuth 账号可读取，避免 stale context 跨账号 failover 泄漏。
func applyStagedCodexFingerprintHeaders(c *gin.Context, account *Account, h http.Header) {
	applyCodexFingerprintHeaders(h, stagedCodexFingerprintIDs(c, account))
}

func applyStagedCodexFingerprintClientMetadata(c *gin.Context, account *Account, reqBody map[string]any) bool {
	return applyCodexFingerprintClientMetadata(reqBody, stagedCodexFingerprintIDs(c, account))
}

// codexFingerprintMode 控制 OAuth 账号出站请求的设备指纹收敛强度。
// 多人共享同一 OAuth 账号时，每个用户的 Codex 客户端会携带各自不同的
// installation_id / session_id / thread_id，上游据此判定设备数和会话数。
// 收敛模式将这些标识改写为账号级恒定值，减少上游可见的设备/会话指纹。
type codexFingerprintMode string

const (
	// codexFingerprintOff 不做任何收敛，原样透传客户端标识。
	// 这是默认值：收敛是显式 opt-in 的（见 GetCodexFingerprintMode）。
	codexFingerprintOff codexFingerprintMode = "off"
	// codexFingerprintDevice 仅收敛 installation_id 为账号级恒定值。
	// 上游看到 1 台设备 + 多会话（每用户各自的 session）。
	codexFingerprintDevice codexFingerprintMode = "device"
	// codexFingerprintSession 收敛 installation_id + session_id，
	// thread_id 按客户端原始 session-id 确定性派生（每个真实 Codex 会话一个独立线程）。
	// 上游看到 1 台设备 + 1 会话 + N 线程，最接近正常用户 spawn 子代理的模式。
	codexFingerprintSession codexFingerprintMode = "session"
	// codexFingerprintFull 收敛所有标识：installation_id + session_id + thread_id。
	// 上游看到 1 台设备 + 1 会话 + 1 线程，最激进。
	codexFingerprintFull codexFingerprintMode = "full"
	// codexFingerprintSubagent 把账号上所有流量收进同一个会话：
	// installation_id 与 session_id 收敛为账号级恒定值，每个客户端会话派生成一个
	// 子线程（thread_id），并按 Codex 的子代理形态标注
	// x-openai-subagent: collab_spawn / x-codex-parent-thread-id: <根线程>，
	// 以及在 turn metadata 中声明 thread_source=subagent、subagent_kind=thread_spawn。
	// 上游看到的是 1 台设备 + 1 会话 + N 个由同一会话派生的子线程——这正是真实
	// Codex 用户并发开子代理的形状，几十个并发请求因此不会被当作多个独立会话。
	codexFingerprintSubagent codexFingerprintMode = "subagent"
)

const (
	// codexSubagentThreadsPerSession 对齐真实 Codex 的单会话子代理并发上限：
	// codex-rs 的 DEFAULT_MULTI_AGENT_V2_MAX_CONCURRENT_THREADS_PER_SESSION = 4
	// （旧版 agent_max_threads = 6）。会话下超过这个数量的并发线程是异常特征。
	codexSubagentThreadsPerSession = 4
	// codexLegacyThreadsPerSession 是 V1 时代的单会话线程上限
	// （codex-rs config/mod.rs:281 DEFAULT_AGENT_MAX_THREADS = 6）。源码里两档
	// 都合法，兜底会话按较宽的 V1 形状摊开以容纳无会话身份的批量并发。
	codexLegacyThreadsPerSession = 6
	codexFingerprintModeExtraKey = "codex_fingerprint_mode"
	codexFingerprintSeedExtraKey = "codex_fingerprint_seed"
)

func canonicalCodexFingerprintSeed(value any) (string, bool) {
	raw, ok := value.(string)
	if !ok {
		return "", false
	}
	trimmed := strings.TrimSpace(raw)
	parsed, err := uuid.Parse(trimmed)
	if err != nil || parsed == uuid.Nil || trimmed != parsed.String() {
		return "", false
	}
	return trimmed, true
}

func newCodexFingerprintSeed() string {
	return uuid.NewString()
}

func stripCodexFingerprintSeed(extra map[string]any) map[string]any {
	if extra == nil {
		return nil
	}
	stripped := maps.Clone(extra)
	delete(stripped, codexFingerprintSeedExtraKey)
	return stripped
}

func codexFingerprintModeFromExtra(extra map[string]any) codexFingerprintMode {
	if extra == nil {
		return codexFingerprintOff
	}
	raw, _ := extra[codexFingerprintModeExtraKey].(string)
	switch codexFingerprintMode(strings.TrimSpace(raw)) {
	case codexFingerprintOff, codexFingerprintDevice, codexFingerprintSession, codexFingerprintFull, codexFingerprintSubagent:
		return codexFingerprintMode(strings.TrimSpace(raw))
	default:
		return codexFingerprintOff
	}
}

func codexFingerprintModeRequiresSeed(mode codexFingerprintMode) bool {
	switch mode {
	case codexFingerprintDevice, codexFingerprintSession, codexFingerprintFull, codexFingerprintSubagent:
		return true
	default:
		return false
	}
}

func codexFingerprintSeed(extra map[string]any) (string, bool) {
	if extra == nil {
		return "", false
	}
	return canonicalCodexFingerprintSeed(extra[codexFingerprintSeedExtraKey])
}

func prepareCodexFingerprintExtraForCreate(platform, accountType string, extra map[string]any) map[string]any {
	prepared := stripCodexFingerprintSeed(extra)
	if platform != PlatformOpenAI || (accountType != AccountTypeOAuth && accountType != AccountTypeSetupToken) || !codexFingerprintModeRequiresSeed(codexFingerprintModeFromExtra(prepared)) {
		return prepared
	}
	if prepared == nil {
		prepared = make(map[string]any, 1)
	}
	prepared[codexFingerprintSeedExtraKey] = newCodexFingerprintSeed()
	return prepared
}

func prepareCodexFingerprintExtraForUpdate(account *Account, extra map[string]any) map[string]any {
	prepared := stripCodexFingerprintSeed(extra)
	if account == nil || !account.IsOpenAIOAuthLike() {
		return prepared
	}
	if seed, ok := codexFingerprintSeed(account.Extra); ok {
		if prepared == nil {
			prepared = make(map[string]any, 1)
		}
		prepared[codexFingerprintSeedExtraKey] = seed
		return prepared
	}
	if codexFingerprintModeRequiresSeed(codexFingerprintModeFromExtra(prepared)) {
		if prepared == nil {
			prepared = make(map[string]any, 1)
		}
		prepared[codexFingerprintSeedExtraKey] = newCodexFingerprintSeed()
	}
	return prepared
}

func sanitizedCodexFingerprintExtraUpdates(updates map[string]any) map[string]any {
	if updates == nil {
		return nil
	}
	sanitized := maps.Clone(updates)
	delete(sanitized, codexFingerprintSeedExtraKey)
	return sanitized
}

// ShouldEnsureCodexFingerprintSeedForExtraUpdates reports whether a JSONB key-level
// extra update is enabling Codex fingerprint convergence and therefore must atomically
// preserve or create the system-managed per-account seed in the repository update.
func ShouldEnsureCodexFingerprintSeedForExtraUpdates(updates map[string]any) bool {
	if updates == nil {
		return false
	}
	return codexFingerprintModeRequiresSeed(codexFingerprintModeFromExtra(updates))
}

// GetCodexFingerprintMode 从账号 extra JSON 读取指纹收敛模式。
//
// **收敛是显式 opt-in**：未设置、空值或非法值一律按 off 处理，只有管理员
// 明确配置 device / session / full 才收敛。
//
// 历史：v0.1.175（#5553）把缺省值当作 session，导致升级后存量 OAuth 账号
// （普遍没有这个 extra 键）的每个非透传请求都被静默改写 installation /
// session / thread / turn / window 五类标识；#5555、#5556、#5582 报告的额度
// 缩水都卡在该版本边界，并有"回退 v0.1.173 即恢复"与"新账号开收敛后降额"
// 的 A/B 实测。上游的配额判定策略不可观测，因此这里取兼容安全的一侧：
// 不显式 opt-in 就保持 v0.1.175 之前的客户端身份（#5610）。
func (a *Account) GetCodexFingerprintMode() codexFingerprintMode {
	if a == nil || !a.IsOpenAIOAuthLike() {
		return codexFingerprintOff
	}
	return codexFingerprintModeFromExtra(a.Extra)
}

// deriveStableUUIDv4 从种子确定性派生一个 UUIDv4 格式的字符串。
// 同一种子永远返回同一值。
func deriveStableUUIDv4(seed string) string {
	h := sha256.Sum256([]byte(seed))
	b := h[:16]
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 1
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16])
}

// resolveConvergedInstallationID 返回账号级恒定的 installation_id。
// 优先使用管理员配置的真实 device_id，无则从系统管理的账号随机种子确定性派生。
func resolveConvergedInstallationID(account *Account, seed string) string {
	if account == nil {
		return ""
	}
	if deviceID := account.GetOpenAIDeviceID(); deviceID != "" {
		return deviceID
	}
	if seed == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-install-id:v2:" + seed)
}

// resolveConvergedSessionIDForClient 按客户端会话派生 session_id：
// 同一客户端会话在账号上始终对应同一个上游会话，不同客户端会话各自独立。
// 无客户端身份时退回账号级恒定值。
func resolveConvergedSessionIDForClient(seed, clientSessionID string) string {
	if seed == "" {
		return ""
	}
	if clientSessionID == "" {
		return resolveConvergedSessionID(seed)
	}
	return deriveStableUUIDv4("sub2api:codex-client-session-id:v1:" + seed + ":" + clientSessionID)
}

// resolveConvergedSessionID 返回账号级恒定的 session_id。
func resolveConvergedSessionID(seed string) string {
	if seed == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-session-id:v2:" + seed)
}

// resolveConvergedThreadID 按客户端原始 session-id 确定性派生 thread_id。
// 每个真实 Codex 会话（不同客户端启动实例）获得一个独立线程，
// 模拟正常用户 spawn 子代理或开多窗口的模式。
func resolveConvergedThreadID(seed, clientSessionID string) string {
	if seed == "" || clientSessionID == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-thread-id:v2:" + seed + ":" + clientSessionID)
}

// codexFingerprintIDs 收敛后的完整 ID 集合。
// 由 resolveCodexFingerprintIDs 一次性生成，同一个实例在头改写和体改写之间共享，
// 确保所有载体中的 turn_id 等随机字段一致。体改写时还会补记原始
// client_metadata.session_id，用于识别 root prompt_cache_key 的默认值。
type codexFingerprintIDs struct {
	accountID      int64
	mode           codexFingerprintMode
	installationID string
	sessionID      string
	threadID       string
	turnID         string
	windowID       string
	// windowNumber 是真实客户端的上下文窗口序号：初始 0，每次压缩 +1。
	// 网关无状态，按“本请求是否携带 previous_response_id / 压缩标记”推导。
	windowNumber                  uint64
	turnStartedAtUnixMs           int64
	originalBodySessionID         string
	originalBodySessionIDCaptured bool
	// messagesBridge 记录客户端原始 prompt_cache_key 是否属于 anthropic-* 兼容入口。
	// transform 会先删掉该字段，事后无法再判定，只能在解析阶段记下来。
	messagesBridge bool
	// subagent 模式专用：本会话的根线程与派生形态。
	parentThreadID string
	subagentKind   string
	threadSource   string
	// isRootTurn 表示本请求是该会话的首个 turn（root 形态）。
	// 真实 Codex 的 root turn 不带 x-openai-subagent，thread_id 等于 session_id，
	// thread metadata 的 thread_source 是 "user"；只有真正 spawn 出来的并行
	// 子代理才带 collab_spawn + parent_thread_id + thread_source="subagent"。
	isRootTurn bool
}

// resolveCodexFingerprintIDs 按收敛模式计算出站 ID 集合。
// clientSessionID 是客户端原始的 session-id 头值（连字符形式），用于 session 模式下
// 的 thread_id 派生——每个真实 Codex 会话得到一个独立线程。
// 返回 nil 表示 off 模式，不需要改写。
// 注意：包含随机生成的 turn_id，调用方必须只调用一次并共享结果给头改写和体改写。
func resolveCodexFingerprintIDs(account *Account, clientSessionID string, mode codexFingerprintMode) *codexFingerprintIDs {
	if account == nil || mode == codexFingerprintOff {
		return nil
	}
	seed, ok := codexFingerprintSeed(account.Extra)
	if !ok {
		return nil
	}

	ids := &codexFingerprintIDs{
		accountID:           account.ID,
		mode:                mode,
		turnStartedAtUnixMs: time.Now().UnixMilli(),
	}

	ids.installationID = resolveConvergedInstallationID(account, seed)
	if ids.installationID == "" {
		return nil
	}

	switch mode {
	case codexFingerprintDevice:
		return ids

	case codexFingerprintSession:
		ids.sessionID = resolveConvergedSessionID(seed)
		ids.threadID = resolveConvergedThreadID(seed, clientSessionID)
		if ids.threadID == "" {
			ids.threadID = ids.sessionID
		}
		ids.turnID = uuid.Must(uuid.NewV7()).String()
		ids.windowID = codexWindowID(ids.threadID, ids.windowNumber)
		return ids

	case codexFingerprintFull:
		ids.sessionID = resolveConvergedSessionID(seed)
		ids.threadID = ids.sessionID
		ids.turnID = uuid.Must(uuid.NewV7()).String()
		ids.windowID = codexWindowID(ids.threadID, ids.windowNumber)
		return ids

	case codexFingerprintSubagent:
		// 每个客户端会话收敛成一个 session；root turn 的 thread_id 等于 session_id，
		// 与该会话中并发的兄弟 turn 才另派生线程。
		//
		// 依据 codex-rs：
		//   - session/session.rs:1007 "session_id is equal to the root thread's ID"
		//   - config/mod.rs:282 单会话并发线程上限 4（含 root，故兄弟至多 3 个）
		//   - responses_metadata.rs:485 root 不带 x-openai-subagent，thread_source 为 user
		ids.sessionID = resolveConvergedSessionIDForClient(seed, clientSessionID)
		ids.turnID = uuid.Must(uuid.NewV7()).String()
		// 只有客户端真实声明过会话才算该会话的 root turn：客户端自己会复用同一
		// session 连发多个 turn，与真实 CLI 一致。
		//
		// 内容兜底会话虽然也有 session_id，但它代表的是「同一段对话」而不是一个
		// 已协商好身份的客户端会话；若按 root 处理，该会话内所有并发会共用同一个
		// thread_id，等于没有分流。因此兜底会话一律走兄弟线程槽位。
		ids.isRootTurn = isRealClientFingerprintSession(clientSessionID)
		if ids.isRootTurn {
			ids.threadID = ids.sessionID
			ids.threadSource = openAICodexThreadSourceUser
		} else {
			ids.parentThreadID = ids.sessionID
			ids.threadID = resolveConvergedSubagentThreadIDForSlot(seed, clientSessionID, nextCodexSubagentSlot())
			ids.subagentKind = openAICodexSubagentKindThreadSpawn
			ids.threadSource = openAICodexThreadSourceSubagent
		}
		ids.windowID = codexWindowID(ids.threadID, ids.windowNumber)
		return ids
	}

	return nil
}

// codexWindowID 按真实客户端的形态拼出 window id：`{thread_id}:{window_number}`。
// 见 codex-rs session/mod.rs current_window_from_state。
func codexWindowID(threadID string, windowNumber uint64) string {
	if threadID == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", threadID, windowNumber)
}

// resolveCodexWindowNumber 推导本次请求应声明的上下文窗口序号。
// 真实客户端从 0 开始，每压缩一次上下文 +1；网关没有本地窗口状态，因此按请求
// 携带的续接信号近似：没有 previous_response_id 与压缩标记的独立 turn 视为首个窗口。
func resolveCodexWindowNumber(clientHeaders http.Header, body []byte) uint64 {
	if clientHeaders == nil && len(body) == 0 {
		return 0
	}
	if clientHeaders != nil {
		if raw := strings.TrimSpace(clientHeaders.Get(openAIWSWindowIDHeader)); raw != "" {
			if _, number, ok := strings.Cut(raw, ":"); ok {
				if parsed, err := strconv.ParseUint(strings.TrimSpace(number), 10, 64); err == nil {
					return parsed
				}
			}
		}
	}
	if len(body) > 0 {
		if gjson.GetBytes(body, "previous_response_id").String() != "" {
			return 1
		}
		if HasCompactionTriggerInInput(body) {
			return 1
		}
	}
	return 0
}

// resolveConvergedRootThreadID 返回会话的根线程：子代理的 parent_thread_id。
// 与 session_id 一样是账号级恒定值，让上游看到稳定的“父会话”。
func resolveConvergedRootThreadID(seed string) string {
	if seed == "" {
		return ""
	}
	return deriveStableUUIDv4("sub2api:codex-root-thread-id:v1:" + seed)
}

// resolveConvergedSubagentThreadID 为每个客户端会话派生一个子线程。
//
// 有客户端身份时确定性派生，同一会话的后续 turn 落在同一线程。
//
// 没有身份时（网关自行合成的并发请求）不能用「每个请求一个新线程」：真实 Codex
// 一个会话同时最多只有 codexSubagentThreadsPerSession 个线程（V2 默认 4，旧版 6，
// 见 codex-rs DEFAULT_MULTI_AGENT_V2_MAX_CONCURRENT_THREADS_PER_SESSION），
// 会话下冒出几十个线程本身就是异常特征。这里把并发请求均匀摊到固定的若干个子线程上，
// 形状与该上限内正常 spawn 出来的子代理一致。
func resolveConvergedSubagentThreadID(seed, clientSessionID string) string {
	return resolveConvergedSubagentThreadIDForSlot(seed, clientSessionID, 0)
}

// codexSubagentSlot 返回本请求应落在的子线程槽位。
// 无客户端身份时按进程内计数器轮转，使同一批并发在固定子线程集合上均匀分布，
// 而不是每个请求各开一个新线程。
var codexSubagentSlotCounter atomic.Uint64

func nextCodexSubagentSlot() uint64 {
	return codexSubagentSlotCounter.Add(1) - 1
}

// resolveConvergedSubagentThreadIDForSlot 与上者相同，但允许调用方指定槽位，
// 供需要把同批并发摊到固定子线程集合的场景使用。
func resolveConvergedSubagentThreadIDForSlot(seed, clientSessionID string, slot uint64) string {
	if seed == "" {
		return ""
	}
	// 有身份的会话每个客户端会话独占一个兄弟线程（每个真实 CLI 会话一个线程）。
	if isRealClientFingerprintSession(clientSessionID) {
		return deriveStableUUIDv4("sub2api:codex-subagent-thread-id:v1:" + seed + ":" + clientSessionID)
	}
	// 内容合成的兜底会话没有真实客户端身份，只有一条「会话」却要承接全部无头并发：
	// 按 V1 的 6 线程形状摊开（root 占 1，兄弟 5 个），否则全部并发会压在同一批
	// 3 个线程上，直接撞单会话路由配额。
	return deriveStableUUIDv4(fmt.Sprintf("sub2api:codex-subagent-thread-slot:v1:%s:%d",
		seed, slot%(codexLegacyThreadsPerSession-1)))
}

// isRealClientFingerprintSession 判断会话身份是否来自客户端声明的头。
// 内容兜底身份统一带 content: 前缀（见 resolveClientFingerprintSessionID）。
func isRealClientFingerprintSession(sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	return sessionID != "" && !strings.HasPrefix(sessionID, "content:")
}

// extractClientSessionID 从请求头中提取客户端原始的会话标识。
// 优先取 session-id（连字符形式，Codex CLI 标准），回退到 session_id（下划线形式）。
// 返回的值尚未被 isolateOpenAISessionID 改写，是客户端的真实标识。
func extractClientSessionID(h http.Header) string {
	if v := strings.TrimSpace(h.Get("session-id")); v != "" {
		return v
	}
	return strings.TrimSpace(h.Get("session_id"))
}

// resolveCodexFingerprintIDsFromRequest 从客户端原始请求头中提取 session-id，
// 结合账号配置一次性解析收敛 ID 集合。调用方应将返回的 ids 同时传给
// applyCodexFingerprintHeaders 和 applyCodexFingerprintClientMetadata。
func resolveCodexFingerprintIDsFromRequest(account *Account, clientHeaders http.Header) *codexFingerprintIDs {
	return resolveCodexFingerprintIDsFromRequestWithBody(account, clientHeaders, nil)
}

// resolveClientFingerprintSessionID 解析用于收敛的上游会话身份。
//
// 只有客户端显式声明了 session 头才算真实会话身份。没有声明的请求（SDK、
// python-requests、裸 Go http 客户端）若直接落到无身份的兜底分支，会全部收敛成
// 同一个上游 session 的少数固定子线程槽位——实测一个账号上 4144 个无身份请求
// 被塞进 1 个 session 的 3 个槽位，全部撞上游的单会话路由配额
// （route_rate_limit_exceeded），加 slot_count 也不起作用。
//
// 因此退回内容种子：同一段对话的后续 turn 首条用户消息不变，仍落在同一会话；
// 不同对话自然分散到不同会话。派生前缀与真实头值区分开，避免与客户端真值撞车。
func resolveClientFingerprintSessionID(clientHeaders http.Header, body []byte) string {
	if clientHeaders != nil {
		if v := extractClientSessionID(clientHeaders); v != "" {
			return v
		}
	}
	if seed := deriveOpenAIAnchoredContentSessionSeed(body); seed != "" {
		return "content:" + deriveStableUUIDv4(seed)
	}
	return ""
}

// resolveCodexFingerprintIDsFromRequestWithBody 在解析收敛 ID 的同时按请求体推导
// 上下文窗口序号；body 为 nil 时退化为仅看头。
func resolveCodexFingerprintIDsFromRequestWithBody(account *Account, clientHeaders http.Header, body []byte) *codexFingerprintIDs {
	if account == nil {
		return nil
	}
	mode := account.GetCodexFingerprintMode()
	if mode == codexFingerprintOff {
		return nil
	}
	clientSessionID := resolveClientFingerprintSessionID(clientHeaders, body)
	ids := resolveCodexFingerprintIDs(account, clientSessionID, mode)
	if ids != nil {
		ids.windowNumber = resolveCodexWindowNumber(clientHeaders, body)
		ids.windowID = codexWindowID(ids.threadID, ids.windowNumber)
		if len(body) > 0 {
			ids.messagesBridge = isOpenAICompatMessagesBridgePromptCacheKey(
				gjson.GetBytes(body, "prompt_cache_key").String())
		}
	}
	return ids
}

// applyCodexFingerprintHeaders 按预计算的收敛 ID 改写出站 HTTP 头中的设备指纹。
// 在 buildUpstreamRequest 的白名单透传之后、enforceCodexIdentityHeaders 之前调用。
func applyCodexFingerprintHeaders(h http.Header, ids *codexFingerprintIDs) {
	if h == nil || ids == nil {
		return
	}

	// 所有非 off 模式都收敛 installation_id
	h.Set("x-codex-installation-id", ids.installationID)

	if ids.mode == codexFingerprintDevice {
		rewriteCodexTurnMetadataFields(h, map[string]any{
			"installation_id": ids.installationID,
		})
		return
	}

	// session / full / subagent 模式：改写所有相关头
	h.Set("x-codex-window-id", ids.windowID)
	h.Set("x-client-request-id", ids.threadID)
	// 连字符形式和下划线形式都改写，保证一致
	h.Set("session-id", ids.sessionID)
	h.Set("session_id", ids.sessionID)
	h.Set("thread-id", ids.threadID)

	fields := map[string]any{
		"installation_id":         ids.installationID,
		"session_id":              ids.sessionID,
		"thread_id":               ids.threadID,
		"turn_id":                 ids.turnID,
		"window_id":               ids.windowID,
		"window_number":           ids.windowNumber,
		"turn_started_at_unix_ms": ids.turnStartedAtUnixMs,
	}
	if ids.mode == codexFingerprintSubagent {
		// 只有兄弟 turn 才自称子代理：真实 Codex 的 root turn 不带
		// x-openai-subagent 与 parent_thread-id，thread_source 是 user
		// （responses_metadata.rs:485、tui/app_server_session.rs:2966）。
		if !ids.isRootTurn {
			h.Set(openAICodexSubagentHeader, openAICodexSubagentThreadSpawnValue)
			if ids.parentThreadID != "" {
				h.Set(openAICodexParentThreadIDHeader, ids.parentThreadID)
				fields["parent_thread_id"] = ids.parentThreadID
			}
			if ids.subagentKind != "" {
				fields["subagent_kind"] = ids.subagentKind
			}
		} else {
			// 防御：从另一账号/模式切过来时可能残留子代理头。
			h.Del(openAICodexSubagentHeader)
			h.Del(openAICodexParentThreadIDHeader)
		}
		if ids.threadSource != "" {
			fields["thread_source"] = ids.threadSource
		}
		// sandbox_mode 由客户端每次 turn 必写（sandbox_tags.rs:50），收敛模式下
		// 客户端若没带则补齐，避免出站稳定缺一个字段。
		if _, exists := fields["sandbox_mode"]; !exists {
			fields["sandbox_mode"] = openAICodexSandboxModeWorkspaceWrite
		}
	}
	// 真实 Codex 的每次 turn 都显式声明 request_kind=turn（见 codex-rs
	// turn_metadata.rs 的 to_responses_metadata），因此 x-codex-turn-metadata
	// 头从不为空。客户端没带过这个头时（网关合成的并发请求）在这里补齐，
	// 避免出站少一个官方客户端每次都会带的字段。
	if _, exists := fields["request_kind"]; !exists {
		fields["request_kind"] = openAICodexRequestKindTurn
	}
	rewriteCodexTurnMetadataFields(h, fields)
}

// rewriteCodexTurnMetadataFields 解析 x-codex-turn-metadata 头中的 JSON，
// 替换指定字段后回写。合法对象保留未指定字段（如 sandbox、thread_source）；
// 非法/非对象值重建为最小合法 metadata，避免 flat 与 embedded identity 分裂。
func rewriteCodexTurnMetadataFields(h http.Header, fields map[string]any) {
	if h == nil {
		return
	}
	var metadata map[string]any
	raw := strings.TrimSpace(h.Get("x-codex-turn-metadata"))
	if err := json.Unmarshal([]byte(raw), &metadata); err != nil || metadata == nil {
		metadata = make(map[string]any, len(fields))
	}
	for k, v := range fields {
		metadata[k] = v
	}
	rebuilt, err := marshalCodexTurnMetadata(metadata)
	if err != nil {
		return
	}
	h.Set("x-codex-turn-metadata", string(rebuilt))
}

// applyCodexFingerprintClientMetadata 按预计算的收敛 ID 改写请求体中的 client_metadata。
// 使用与头改写相同的 ids 实例，确保 turn_id 等随机字段一致。
func applyCodexFingerprintClientMetadata(reqBody map[string]any, ids *codexFingerprintIDs) bool {
	if reqBody == nil || ids == nil {
		return false
	}

	captureCodexFingerprintOriginalBodySessionID(ids, reqBody["client_metadata"])
	existing, _ := reqBody["client_metadata"].(map[string]any)
	if existing == nil {
		existing = make(map[string]any)
	}

	modified := false
	if applyCodexFingerprintToClientMetadataMap(existing, ids) {
		reqBody["client_metadata"] = existing
		modified = true
	}
	if applyCodexFingerprintPromptCacheKey(reqBody, ids) {
		modified = true
	}
	return modified
}

// applyCodexFingerprintToClientMetadataMap 是 client_metadata 改写的共享核心，
// map 版（非透传，body 已解码）与 raw 字节版（透传热路径）都经由它，保证两条
// 路径的收敛语义永不漂移。
func applyCodexFingerprintToClientMetadataMap(existing map[string]any, ids *codexFingerprintIDs) bool {
	if existing == nil || ids == nil {
		return false
	}

	modified := false

	if ids.installationID != "" {
		existing["x-codex-installation-id"] = ids.installationID
		modified = true
	}

	if ids.mode == codexFingerprintDevice {
		rewriteClientMetadataEmbeddedTurnMetadata(existing, map[string]any{
			"installation_id": ids.installationID,
		})
		return modified
	}

	// session / full / subagent 模式
	existing["session_id"] = ids.sessionID
	existing["thread_id"] = ids.threadID
	existing["turn_id"] = ids.turnID
	existing["x-codex-window-id"] = ids.windowID

	fields := map[string]any{
		"installation_id":         ids.installationID,
		"session_id":              ids.sessionID,
		"thread_id":               ids.threadID,
		"turn_id":                 ids.turnID,
		"window_id":               ids.windowID,
		"window_number":           ids.windowNumber,
		"turn_started_at_unix_ms": ids.turnStartedAtUnixMs,
	}
	if ids.mode == codexFingerprintSubagent {
		// 与出站头保持同一份身份：flat client_metadata 与内嵌 x-codex-turn-metadata
		// 必须一致。root turn 不带子代理字段，兄弟 turn 才带父线程与子代理类型。
		if !ids.isRootTurn {
			existing[openAICodexSubagentHeader] = openAICodexSubagentThreadSpawnValue
			if ids.parentThreadID != "" {
				existing[openAICodexParentThreadIDHeader] = ids.parentThreadID
				fields["parent_thread_id"] = ids.parentThreadID
			}
			if ids.subagentKind != "" {
				fields["subagent_kind"] = ids.subagentKind
			}
		} else {
			delete(existing, openAICodexSubagentHeader)
			delete(existing, openAICodexParentThreadIDHeader)
		}
		if ids.threadSource != "" {
			fields["thread_source"] = ids.threadSource
		}
		if _, exists := fields["sandbox_mode"]; !exists {
			fields["sandbox_mode"] = openAICodexSandboxModeWorkspaceWrite
		}
	}
	rewriteClientMetadataEmbeddedTurnMetadata(existing, fields)
	return true
}

func captureCodexFingerprintOriginalBodySessionID(ids *codexFingerprintIDs, clientMetadata any) {
	if ids == nil || ids.originalBodySessionIDCaptured {
		return
	}
	ids.originalBodySessionIDCaptured = true
	if clientMetadata == nil {
		return
	}
	switch metadata := clientMetadata.(type) {
	case map[string]any:
		if sessionID, ok := metadata["session_id"].(string); ok {
			ids.originalBodySessionID = strings.TrimSpace(sessionID)
		}
	case map[string]string:
		ids.originalBodySessionID = strings.TrimSpace(metadata["session_id"])
	}
}

func captureCodexFingerprintOriginalBodySessionIDRaw(ids *codexFingerprintIDs, value gjson.Result) {
	if ids == nil || ids.originalBodySessionIDCaptured {
		return
	}
	ids.originalBodySessionIDCaptured = true
	if value.Exists() && value.Type == gjson.String {
		ids.originalBodySessionID = strings.TrimSpace(value.String())
	}
}

func shouldRewriteCodexFingerprintPromptCacheKey(ids *codexFingerprintIDs, promptCacheKey string) bool {
	if ids == nil || !ids.originalBodySessionIDCaptured || ids.originalBodySessionID == "" || ids.sessionID == "" {
		return false
	}
	// subagent 模式同样让 prompt_cache_key 跟随收敛后的 session：子代理共享父
	// 会话的缓存命名空间，正是真实 Codex 的行为。
	if ids.mode != codexFingerprintSession && ids.mode != codexFingerprintFull && ids.mode != codexFingerprintSubagent {
		return false
	}
	return promptCacheKey == ids.originalBodySessionID
}

func applyCodexFingerprintPromptCacheKey(reqBody map[string]any, ids *codexFingerprintIDs) bool {
	if reqBody == nil || ids == nil || ids.sessionID == "" {
		return false
	}
	// messages bridge（Claude Code 兼容入口）的 prompt_cache_key 是 anthropic-*
	// 专用命名空间，上游不接受也由 bridge 自行维护，这里不补也不改。
	if ids.messagesBridge || isOpenAICompatMessagesBridgeRequestBody(reqBody) {
		return false
	}
	// 字段完全缺失时补上：Codex 的 PromptCacheKey::SessionDerived 恒发该字段且值
	// 就是 session（client.rs prompt_cache_key / session_derived_cache_key），
	// 缺席本身就是一处可见差异。客户端显式写过的值（含空串）保持既有语义。
	if _, exists := reqBody["prompt_cache_key"]; !exists {
		reqBody["prompt_cache_key"] = ids.sessionID
		return true
	}
	promptCacheKey, ok := reqBody["prompt_cache_key"].(string)
	if !ok || !shouldRewriteCodexFingerprintPromptCacheKey(ids, promptCacheKey) {
		return false
	}
	if promptCacheKey == ids.sessionID {
		return false
	}
	reqBody["prompt_cache_key"] = ids.sessionID
	return true
}

// applyCodexFingerprintClientMetadataRaw 在原始 JSON 字节上改写 client_metadata，
// 供透传路径使用——透传是热路径，禁止对可能高达数十 MB 的 body 做全量
// Unmarshal（见 forwardOpenAIPassthrough 的轻量提取注释）。实现为：gjson 提取
// client_metadata 小对象单独解码，经共享核心改写后 sjson 一次性拼回，body
// 其余字节原样保留；root prompt_cache_key 仅在可证明是 body session 默认值时
// 做标量改写。语义与 applyCodexFingerprintClientMetadata 逐点一致（含
// "非对象值整体替换为收敛集合"的行为）。
func applyCodexFingerprintClientMetadataRaw(body []byte, ids *codexFingerprintIDs) ([]byte, bool, error) {
	if len(body) == 0 || ids == nil {
		return body, false, nil
	}
	// 非 JSON 对象的 body（数组/标量/畸形）没有 client_metadata 语义，
	// sjson 在这类根上写字段会改写整体结构，直接放行保持原样。
	root := gjson.ParseBytes(body)
	if !root.IsObject() {
		captureCodexFingerprintOriginalBodySessionIDRaw(ids, gjson.Result{})
		return body, false, nil
	}

	existing := map[string]any{}
	if cm := gjson.GetBytes(body, "client_metadata"); cm.IsObject() {
		captureCodexFingerprintOriginalBodySessionIDRaw(ids, gjson.GetBytes(body, "client_metadata.session_id"))
		if err := json.Unmarshal([]byte(cm.Raw), &existing); err != nil {
			return body, false, fmt.Errorf("decode client_metadata for fingerprint: %w", err)
		}
	} else {
		captureCodexFingerprintOriginalBodySessionIDRaw(ids, gjson.Result{})
	}

	next := body
	modified := false
	if applyCodexFingerprintToClientMetadataMap(existing, ids) {
		raw, err := json.Marshal(existing)
		if err != nil {
			return body, false, fmt.Errorf("encode converged client_metadata: %w", err)
		}
		var setErr error
		next, setErr = sjson.SetRawBytes(body, "client_metadata", raw)
		if setErr != nil {
			return body, false, fmt.Errorf("splice converged client_metadata: %w", setErr)
		}
		modified = true
	}
	promptCacheKey := gjson.GetBytes(body, "prompt_cache_key")
	injectKey := !promptCacheKey.Exists() && ids.sessionID != ""
	rewriteKey := promptCacheKey.Exists() && promptCacheKey.Type == gjson.String &&
		strings.TrimSpace(promptCacheKey.String()) != "" &&
		shouldRewriteCodexFingerprintPromptCacheKey(ids, promptCacheKey.String())
	if injectKey || rewriteKey {
		rewritten, err := sjson.SetBytes(next, "prompt_cache_key", ids.sessionID)
		if err != nil {
			return body, false, fmt.Errorf("splice converged prompt_cache_key: %w", err)
		}
		next = rewritten
		modified = true
	}
	return next, modified, nil
}

// rewriteClientMetadataEmbeddedTurnMetadata 改写 client_metadata 中内嵌的
// x-codex-turn-metadata JSON 字符串里的指定字段。非法/非对象值会重建，
// 避免 flat client_metadata 与 embedded metadata 暴露两套身份。
//
// 键不存在时同样重建：Codex 只要 request_kind 存在就必发这个嵌套键
// （responses_metadata.rs has_turn_metadata / client_metadata:392-396），
// 而本网关恒写 request_kind=turn，缺了它就是明显的客户端形态差异。
func rewriteClientMetadataEmbeddedTurnMetadata(clientMetadata map[string]any, fields map[string]any) {
	if clientMetadata == nil {
		return
	}
	var metadata map[string]any
	if raw, ok := clientMetadata["x-codex-turn-metadata"].(string); ok && raw != "" {
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			metadata = nil
		}
	}
	if metadata == nil {
		metadata = make(map[string]any, len(fields))
	}
	for k, v := range fields {
		metadata[k] = v
	}
	if rebuilt, err := marshalCodexTurnMetadata(metadata); err == nil {
		clientMetadata["x-codex-turn-metadata"] = string(rebuilt)
	}
}

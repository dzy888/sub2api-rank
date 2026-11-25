package service

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/google/uuid"
)

// codexUpstreamMinVersion 上游 /backend-api/codex 接受的最低 version 头：
// 若请求携带 version 且低于该值，上游直接 404（issue #3901，2026-07 实测）。
const codexUpstreamMinVersion = "0.144.0"

// codexClientVersionMaxLen 官方版本号均为短 ASCII 串，远低于此上限。
const codexClientVersionMaxLen = 64

// codexClientVersionPattern 允许 0.146.0 与 0.147.0-alpha.4 两类官方形态。
var codexClientVersionPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+){1,3}(-[0-9A-Za-z.]+)?$`)

// NormalizeCodexClientVersion 校验并归一化 Codex 客户端版本号，非法值返回空串。
// 该值会被拼进出站 User-Agent 与 version 头，必须拒绝任意字节，避免管理员误填或
// 自动同步拿到异常值时把不可控内容透给上游。
func NormalizeCodexClientVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" || len(version) > codexClientVersionMaxLen || !codexClientVersionPattern.MatchString(version) {
		return ""
	}
	return version
}

// buildCodexCLIUserAgent 按版本号拼出规范 Codex CLI User-Agent。
// UA 形态只在 codexCLIUserAgentSuffix 一处定义，避免多处拼装漂移。
func buildCodexCLIUserAgent(version string) string {
	if version = NormalizeCodexClientVersion(version); version == "" {
		return codexCLIUserAgent
	}
	return openai.CodexDefaultOriginator + "/" + version + codexCLIUserAgentSuffix
}

// codexIdentityEnforcement 控制 enforceCodexIdentityHeaders 是否强制统一出站身份，
// 由 gateway.disable_codex_identity_enforcement 在服务构造时取反发布。
// 默认开启：上游会按客户端身份进行风控分层，命中风险身份的请求会拿到
// HTTP 200 + 流内 server_is_overloaded，本次请求即失败；强制统一出口可确保没有
// 请求带着第三方或陈旧身份出站。关闭后退回「仅按最终 UA 配对 originator」的收口语义。
var codexIdentityEnforcement = func() *atomic.Bool {
	v := &atomic.Bool{}
	v.Store(true)
	return v
}()

// SetCodexIdentityEnforcementEnabled 发布 Codex 出站身份强制统一开关。
// enforceCodexIdentityHeaders 是所有出站路径共用的纯函数收口点，无法在热路径注入配置，
// 故由持有配置的服务在构造时发布进程级快照。
func SetCodexIdentityEnforcementEnabled(enabled bool) {
	codexIdentityEnforcement.Store(enabled)
}

// codexCanonicalUserAgentResolver 返回当前生效的规范 Codex User-Agent（后台设置 / 自动同步版本号）。
// 由 SettingService 在装配时注入；解析器内部自带 TTL 缓存，热路径不触库。
type codexCanonicalUserAgentResolver func() string

var (
	codexCanonicalUAMu       sync.RWMutex
	codexCanonicalUAResolver codexCanonicalUserAgentResolver
)

// SetCodexCanonicalUserAgentResolver 注入规范 User-Agent 解析器。
// 未注入或解析结果非法时回退到编译期常量 codexCLIUserAgent。
func SetCodexCanonicalUserAgentResolver(resolver func() string) {
	codexCanonicalUAMu.Lock()
	defer codexCanonicalUAMu.Unlock()
	codexCanonicalUAResolver = resolver
}

// CodexCanonicalUserAgent 返回当前生效的规范 Codex User-Agent。
// 取值走与推理相同的解析链：面板 UA 指纹 + 面板/自动同步版本号 + 编译期兜底。
// 供无账号句柄的出站路径（OAuth 换 Token / 刷新）使用。
func CodexCanonicalUserAgent() string {
	return resolveCodexOutboundIdentity("").userAgent
}

// CodexCanonicalAuthIdentity 返回凭据面（auth.openai.com：换 Token / 刷新 / whoami）
// 出站请求的身份对：规范 User-Agent 与配套 originator，与推理解析链同源。
// 凭据面不发 version 头——真实 Codex 客户端在该面只携带 originator 与 User-Agent
// （codex-rs login/default_client.rs 的 default_headers()），version 门槛
// （issue #3901）只存在于 /backend-api/codex 推理面。
func CodexCanonicalAuthIdentity() (userAgent, originator string) {
	identity := resolveCodexOutboundIdentity("")
	return identity.userAgent, identity.originator
}

// ApplyCodexCanonicalAuthIdentity 为凭据面出站请求写入身份对（不含 version）。
func ApplyCodexCanonicalAuthIdentity(h http.Header) {
	if h == nil {
		return
	}
	userAgent, originator := CodexCanonicalAuthIdentity()
	h.Set("user-agent", userAgent)
	h.Set("originator", originator)
}

// CodexCanonicalClientVersion 返回当前生效的 Codex 客户端版本号。
func CodexCanonicalClientVersion() string {
	return resolveCodexOutboundIdentity("").version
}

// codexCanonicalUserAgent 返回出站规范 User-Agent。
func codexCanonicalUserAgent() string {
	codexCanonicalUAMu.RLock()
	resolver := codexCanonicalUAResolver
	codexCanonicalUAMu.RUnlock()
	if resolver != nil {
		if ua := resolver(); strings.TrimSpace(ua) != "" {
			return ua
		}
	}
	return codexCLIUserAgent
}

// codexOutboundIdentity 出站身份三元组，三者必须同源自洽：
// originator 与 User-Agent 首段配套（否则上游 404，issue #3901），
// version 等于 User-Agent 的版本段且不低于上游门槛。
type codexOutboundIdentity struct {
	userAgent  string
	originator string
	version    string
}

// resolveCodexOutboundIdentity 由候选 User-Agent 推导自洽的出站身份。
// candidateUA 为空时使用规范 User-Agent；推导不出官方身份时整体回退为规范 CLI 身份。
//
// 候选 UA（面板 / 账号级的管理员显式配置）只贡献客户端名与 OS / 架构 / 终端指纹，
// 其自带的版本段一律用当前生效版本重建：一条填写于某个历史版本的 UA 否则会把出站身份
// 永久钉死在陈旧版本上，绕过版本自动同步，落回上游身份风控更严格的一侧。
// 需要固定版本请填「Codex 客户端版本号」并关闭自动同步。
func resolveCodexOutboundIdentity(candidateUA string) codexOutboundIdentity {
	canonical := codexCanonicalUserAgent()
	if _, _, ok := openai.PairCodexClientIdentity(canonical); !ok {
		canonical = codexCLIUserAgent
	}
	ua := candidateUA
	if strings.TrimSpace(ua) == "" {
		ua = canonical
	}
	originator, pairedUA, ok := openai.PairCodexClientIdentity(ua)
	if !ok {
		if originator, pairedUA, ok = openai.PairCodexClientIdentity(canonical); !ok {
			originator, pairedUA = openai.CodexCLIOriginator, codexCLIUserAgent
		}
	}
	// 旧配置或账号级 UA 可能仍保存 codex-tui。该身份是 sub2api 默认出口曾经
	// 使用的风险指纹，必须与 UA 首段一起归一为 CLI，不能只替换 originator。
	originator, pairedUA, _ = openai.NormalizeCodexClientIdentityToCLI(originator, pairedUA)
	// 生效版本只有一个来源：规范身份（面板版本号 → 自动同步值 → 内置常量，见
	// SettingService.GetOpenAICodexClientVersion）。UA 与 version 头由此同源派生。
	version := codexClientVersionFromUA(canonical)
	if rebuilt := openai.SetCodexUserAgentVersion(pairedUA, version); rebuilt != "" {
		pairedUA = rebuilt
	}
	return codexOutboundIdentity{userAgent: pairedUA, originator: originator, version: version}
}

// codexDeviceProfile 是 UA 尾部「(OS OS版本; arch) terminal」的设备指纹。
// 真实 Codex 客户端从运行环境采集这一段，稳定、合法，且与版本段无关。
type codexDeviceProfile struct {
	os       string
	osVer    string
	arch     string
	terminal string
}

func (p codexDeviceProfile) String() string {
	return fmt.Sprintf(" (%s %s; %s) %s", p.os, p.osVer, p.arch, p.terminal)
}

// codexDeviceProfiles 是官方 Codex CLI 真实运行环境的常见组合。
// 网关原先对每个账号都发同一段 Ubuntu/xterm，账号一多，这段固定串本身就成了
// 「同一台机器上跑着一堆 Codex」的指纹。按账号确定性取一份，既跨账号分散，
// 又保证同一账号长期自洽。
//
// 权重贴近真实取样：Linux 服务器/容器占多数，macOS 次之，Windows 少量；
// 终端一律写真实 CLI 会检测到的取值，不写浏览器或图形终端品牌。
var codexDeviceProfiles = [...]codexDeviceProfile{
	{"Ubuntu", "24.4.0", "x86_64", "xterm-256color"},
	{"Ubuntu", "22.4.0", "x86_64", "xterm-256color"},
	{"Ubuntu", "24.4.0", "x86_64", "tmux"},
	{"Debian GNU/Linux", "12", "x86_64", "xterm-256color"},
	{"Linux", "6.8.0", "x86_64", "screen"},
	{"Mac OS X", "15.1.0", "arm64", "iTerm.app/3.5.14"},
	{"Mac OS X", "14.7.0", "arm64", "Apple_Terminal/455"},
	{"Windows", "11", "x86_64", "WindowsTerminal"},
}

// resolveCodexDeviceSuffix 按账号种子确定性选择一份设备指纹。
// 无种子（未开收敛的账号）时返回空串，由调用方保留 canonical UA 原样。
func resolveCodexDeviceSuffix(account *Account) string {
	if account == nil {
		return ""
	}
	seed, ok := codexFingerprintSeed(account.Extra)
	if !ok {
		return ""
	}
	sum := sha256.Sum256([]byte("sub2api:codex-device-profile:v1:" + seed))
	return codexDeviceProfiles[int(sum[0])%len(codexDeviceProfiles)].String()
}

// applyCodexAccountDeviceIdentity 只替换 UA 的设备指纹段，originator 与版本段
// 保持 enforceCodexIdentityHeaders 已经统一好的值，三者仍同源自洽。
func applyCodexAccountDeviceIdentity(h http.Header, account *Account) {
	suffix := resolveCodexDeviceSuffix(account)
	if h == nil || suffix == "" {
		return
	}
	ua := strings.TrimSpace(h.Get("user-agent"))
	slash := strings.IndexByte(ua, '/')
	if slash <= 0 {
		return
	}
	rest := ua[slash:]
	// 截断到第一个空格：`{client}/{version}` 之后的部分全部是环境指纹。
	if space := strings.IndexByte(rest, ' '); space >= 0 {
		rest = rest[:space]
	}
	h.Set("user-agent", ua[:slash]+rest+suffix)
}

// codexClientVersionFromUA 取 UA 的版本段作为生效版本；
// 非法或低于上游门槛（低于则上游 404，issue #3901）时回退编译期常量。
func codexClientVersionFromUA(ua string) string {
	version := NormalizeCodexClientVersion(openai.CodexUserAgentVersion(ua))
	if version == "" || CompareVersions(version, codexUpstreamMinVersion) < 0 {
		return codexCLIVersion
	}
	return version
}

// Latest Codex HTTP envelope markers. A real Codex client always declares the
// response fields it can omit; relays that only imitate the older handshake are
// easy to separate from the official client on these two headers alone.
const (
	openAICodexExcludeHeader             = "x-codex-exclude"
	openAICodexExcludeValue              = "tools,usage.attribution"
	openAICodexExcludeStreamFieldsHeader = "x-codex-exclude-stream-fields"
	openAICodexExcludeStreamFieldsValue  = "delta.sequence_number,reasoning.added.encrypted_content"
	openAICodexParentThreadIDHeader      = "x-codex-parent-thread-id"
	openAICodexSubagentHeader            = "x-openai-subagent"
	openAICodexSubagentThreadSpawnValue  = "collab_spawn"
	openAICodexThreadSourceSubagent      = "subagent"
	openAICodexThreadSourceUser          = "user"
	// 沙箱档位取值来自 codex-rs core/src/sandbox_tags.rs:95，每次 turn 必带。
	openAICodexSandboxModeWorkspaceWrite = "workspace-write"
	openAICodexSubagentKindThreadSpawn   = "thread_spawn"
	openAICodexRequestKindTurn           = "turn"
	openAICodexWindowIDSuffix            = ":0"
)

// applyCodexOutboundEnvelope makes the session identity triple self-consistent
// on the wire. A real Codex client always sends session-id, thread-id,
// x-client-request-id and x-codex-window-id together, and the turn metadata
// carries the same values. Builders that only forwarded whichever identity the
// caller happened to send produced mismatched pairs that upstream can correlate
// cheaply. The fingerprint stage runs first, so a converged identity wins; this
// only fills and aligns.
func applyCodexOutboundEnvelope(h http.Header) {
	if h == nil {
		return
	}
	// The builder-owned underscore header is authoritative: it carries the
	// API-key/account-scoped session. The hyphen form is the client's own value
	// and is only a fallback.
	sessionID := firstNonEmptyHeaderValue(h, "session_id", "session-id", "conversation_id")
	threadID := firstNonEmptyHeaderValue(h, "thread-id")
	if threadID == "" {
		threadID = codexTurnMetadataThreadID(h.Get("x-codex-turn-metadata"))
	}
	if threadID == "" {
		threadID = codexWindowIDThreadID(h.Get("x-codex-window-id"))
	}
	if sessionID == "" {
		sessionID = threadID
	}
	if threadID == "" {
		threadID = sessionID
	}
	if sessionID == "" {
		sessionID = uuid.NewString()
		threadID = sessionID
	}
	windowID := h.Get("x-codex-window-id")
	if codexWindowIDThreadID(windowID) != threadID {
		windowID = threadID + openAICodexWindowIDSuffix
	}
	h.Set("session-id", sessionID)
	h.Set("session_id", sessionID)
	h.Set("thread-id", threadID)
	h.Set("x-client-request-id", threadID)
	h.Set("x-codex-window-id", windowID)
	h.Set(openAICodexExcludeHeader, openAICodexExcludeValue)
	h.Set(openAICodexExcludeStreamFieldsHeader, openAICodexExcludeStreamFieldsValue)
	rewriteCodexTurnMetadataFields(h, map[string]any{
		"session_id": sessionID,
		"thread_id":  threadID,
		"window_id":  windowID,
	})
}

func firstNonEmptyHeaderValue(h http.Header, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(h.Get(name)); value != "" {
			return value
		}
	}
	return ""
}

func codexWindowIDThreadID(window string) string {
	window = strings.TrimSpace(window)
	if window == "" {
		return ""
	}
	thread, _, _ := strings.Cut(window, ":")
	return strings.TrimSpace(thread)
}

// applyOpenAIAPIKeyIdentityHeaders gives every OpenAI API-key account the same
// canonical client identity as OAuth forwarding. Incoming library/browser UAs
// do not select the outbound identity. Version follows the existing resolver.
// Apply before provider-specific headers and explicit account header overrides;
// those remain escape hatches for compatible upstreams with a different contract.
// Unlike a Codex probe, this does not add beta flags or session identifiers.
func applyOpenAIAPIKeyIdentityHeaders(h http.Header, account *Account, overrideUA string) {
	if h == nil || account == nil || !account.IsOpenAIApiKey() || !codexIdentityEnforcement.Load() {
		return
	}
	identity := resolveCodexOutboundIdentity(overrideUA)
	// Remove noncanonical key variants too, so a copied lower-case header cannot
	// coexist with the normalized identity on the wire.
	for key := range h {
		switch strings.ToLower(key) {
		case "user-agent", "originator", "version":
			delete(h, key)
		}
	}
	h.Set("User-Agent", identity.userAgent)
	h.Set("Originator", identity.originator)
	h.Set("Version", identity.version)
}

// ensureCodexIdentityHeaders 补齐 OAuth（ChatGPT 内部接口）出站请求所需的 Codex 身份头。
// 已有 User-Agent 与 version 保持不变，交给紧随其后的 enforceCodexIdentityHeaders 收口。
func ensureCodexIdentityHeaders(h http.Header) {
	if h == nil {
		return
	}
	identity := resolveCodexOutboundIdentity("")
	if strings.TrimSpace(h.Get("user-agent")) == "" {
		h.Set("user-agent", identity.userAgent)
	}
	if strings.TrimSpace(h.Get("originator")) == "" {
		h.Set("originator", identity.originator)
	}
	if strings.TrimSpace(h.Get("version")) == "" {
		h.Set("version", identity.version)
	}
	h.Set("OpenAI-Beta", "responses=experimental")
}

// applyOpenAICodexProbeHeaders 为合成探测请求补齐 Codex 身份和引擎指纹。
func applyOpenAICodexProbeHeaders(h http.Header) {
	if h == nil {
		return
	}
	ensureCodexIdentityHeaders(h)
	h.Set("X-Codex-Window-ID", uuid.NewString())
}

// enforceCodexIdentityHeaders 收口 OAuth（ChatGPT 内部接口）出站请求的客户端身份头。
// 见 enforceCodexIdentityHeadersWithUA；无账号级自定义 User-Agent 时使用本函数。
func enforceCodexIdentityHeaders(h http.Header) {
	enforceCodexIdentityHeadersWithUA(h, "")
}

// enforceCodexIdentityHeadersWithUA 强制统一 OAuth 出站身份：User-Agent / originator / version
// 一律改写为网关的规范身份，客户端自报身份不参与构造。上游会按客户端身份进行风控分层，
// 命中风险身份的请求会拿到 HTTP 200 + 流内 server_is_overloaded；统一出口可确保没有请求带着
// 第三方或陈旧身份出站，也天然满足 originator 与 UA 首段配套的上游校验（issue #3901）。
//
// overrideUA 是账号级自定义 User-Agent：管理员的显式配置仍然生效，但只贡献客户端名与
// OS / 架构 / 终端指纹——版本段与 originator 都由规范身份重建，不允许出现自相矛盾或陈旧的身份。
//
// 强制统一被 gateway.disable_codex_identity_enforcement 关闭时，退回「按最终 User-Agent 配对
// originator + version 门槛校正」的收口语义，供上游策略变动时回滚。
//
// 仅对携带 originator 的请求生效：compat 桥接等非 ChatGPT 内部接口路径会显式删除 originator，
// 不应被补回。需要从缺失身份头恢复的调用方应先调用 ensureCodexIdentityHeaders。
// 必须在所有 User-Agent 改写之后调用。
func enforceCodexIdentityHeadersWithUA(h http.Header, overrideUA string) {
	if h == nil || h.Get("originator") == "" {
		return
	}
	if !codexIdentityEnforcement.Load() {
		pairCodexIdentityHeaders(h)
		return
	}
	identity := resolveCodexOutboundIdentity(overrideUA)
	h.Set("user-agent", identity.userAgent)
	h.Set("originator", identity.originator)
	h.Set("version", identity.version)
}

// pairCodexIdentityHeaders 是关闭强制统一后的兜底收口：保留客户端真实身份，
// 仅保证 originator 与最终 User-Agent 首段配套、version 不低于上游门槛（issue #3901）。
func pairCodexIdentityHeaders(h http.Header) {
	originator, pairedUA, ok := openai.PairCodexClientIdentity(h.Get("user-agent"))
	if !ok {
		identity := resolveCodexOutboundIdentity("")
		originator, pairedUA = identity.originator, identity.userAgent
		h.Set("version", identity.version)
	}
	h.Set("user-agent", pairedUA)
	h.Set("originator", originator)
	if v := strings.TrimSpace(h.Get("version")); v != "" && CompareVersions(v, codexUpstreamMinVersion) < 0 {
		h.Set("version", resolveCodexOutboundIdentity("").version)
	}
}

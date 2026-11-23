package mihomo

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"
)

// Background membership refresh runs every 15 seconds. Allow one refresh
// window plus bounded account lookup/probe work, then fail closed on DB errors.
const BPSStaticRequestMembershipMaxAge = 30 * time.Second
const bpsRequestMaxSessions = 16384

type bpsRequestSessionKey struct {
	accountID int64
	scope     string
}

// BPSStaticProxy is a credential-bearing background snapshot. It must never be
// logged. Expiry is checked again on every acquisition, not just on refresh.
type BPSStaticProxy struct {
	ID        int64
	Name      string
	URL       string
	ExpiresAt *time.Time
}
type bpsRequestAccount struct {
	active map[string]int
	last   string
}
type bpsRequestSession struct {
	node     string
	active   int
	lastUsed time.Time
}
type bpsRequestPool struct {
	refreshedAt time.Time
	records     map[int64]BPSStaticProxy
	groups      map[int64][]int64
	accounts    map[int64]*bpsRequestAccount
	sessions    map[bpsRequestSessionKey]*bpsRequestSession
	warmCursor  int
}

// SetBPSStaticProxyRecords refreshes the shared active membership once; account
// allowlists are applied during acquisition and cannot replace this pool.
func SetBPSStaticProxyRecords(records []BPSStaticProxy) {
	next := make(map[int64]BPSStaticProxy, len(records))
	urls := make(map[string]string, len(records))
	for _, p := range records {
		p.URL = strings.TrimSpace(p.URL)
		if p.ID <= 0 || p.URL == "" {
			continue
		}
		if p.ExpiresAt != nil {
			expiry := *p.ExpiresAt
			p.ExpiresAt = &expiry
		}
		next[p.ID] = p
		urls[bpsStaticNodeKey(p.URL)] = p.URL
	}
	m := bpsStaticManager
	m.bpsMu.Lock()
	defer m.bpsMu.Unlock()
	m.bpsRequests.records = next
	now := time.Now()
	m.bpsRequests.refreshedAt = now
	for key, binding := range m.bpsRequests.sessions {
		if binding.active == 0 && (now.Sub(binding.lastUsed) >= bpsSessionIdleTTL || urls[binding.node] == "") {
			delete(m.bpsRequests.sessions, key)
		}
	}
	m.bpsStatic = urls
}

// SetBPSStaticRequestGroups registers background warming requirements before
// any user traffic. A removed configuration cannot alter an in-flight lease.
func SetBPSStaticRequestGroups(groups map[int64][]int64) {
	next := make(map[int64][]int64, len(groups))
	for id, ids := range groups {
		if id > 0 {
			next[id] = append([]int64(nil), ids...)
		}
	}
	m := bpsStaticManager
	m.bpsMu.Lock()
	defer m.bpsMu.Unlock()
	m.bpsRequests.groups = next
	for id, a := range m.bpsRequests.accounts {
		if _, ok := next[id]; !ok && len(a.active) == 0 {
			delete(m.bpsRequests.accounts, id)
		}
	}
}

// AcquireBPSStaticRequestLease atomically chooses and reserves the least busy
// healthy selected exit for this account. Equal loads rotate fairly. It never
// binds a conversation, probes on the request path, or uses an unselected exit.
func AcquireBPSStaticRequestLease(ctx context.Context, accountID int64, proxyIDs []int64, excludedProxyURLs ...string) (*BPSLease, error) {
	return bpsStaticManager.acquireBPSStaticRequestLease(ctx, accountID, proxyIDs, "", excludedProxyURLs)
}

// AcquireBPSStaticSessionRequestLease prefers the session's healthy exit
// inside the same allowlist and load pool used by ordinary request leases.
func AcquireBPSStaticSessionRequestLease(ctx context.Context, scope string, accountID int64, proxyIDs []int64) (*BPSLease, error) {
	return bpsStaticManager.acquireBPSStaticRequestLease(ctx, accountID, proxyIDs, scope, nil)
}

func (m *Manager) acquireBPSStaticRequestLease(ctx context.Context, accountID int64, proxyIDs []int64, scope string, excludedProxyURLs []string) (*BPSLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.bpsMu.Lock()
	defer m.bpsMu.Unlock()
	excluded := make(map[string]bool, len(excludedProxyURLs))
	for _, raw := range excludedProxyURLs {
		excluded[bpsStaticNodeKey(raw)] = true
	}
	now := time.Now()
	if m.bpsRequests.refreshedAt.IsZero() || !now.Before(m.bpsRequests.refreshedAt.Add(BPSStaticRequestMembershipMaxAge)) {
		return nil, bpsAcquisitionError(bpsSelectionError("selected_pool_stale", "selected BPS proxy membership requires a successful refresh"), 0)
	}
	nodes := make([]string, 0, len(proxyIDs))
	seen := map[string]bool{}
	selected := make(map[string]BPSStaticProxy)
	for _, id := range proxyIDs {
		p, ok := m.bpsRequests.records[id]
		if !ok || id <= 0 || (p.ExpiresAt != nil && !p.ExpiresAt.After(now)) {
			continue
		}
		node := bpsStaticNodeKey(p.URL)
		h := m.bpsHealthAtLocked(node, now)
		if excluded[node] || seen[node] || m.bpsStatic[node] != p.URL || !now.Before(h.verifiedUntil) || m.bpsCoolingLocked(node, now) {
			continue
		}
		seen[node] = true
		selected[node] = p
		nodes = append(nodes, node)
	}
	if accountID <= 0 || len(nodes) == 0 {
		return nil, bpsAcquisitionError(bpsSelectionError("selected_pool_unavailable", "selected BPS proxies have no healthy exit"), 0)
	}
	if m.bpsRequests.accounts == nil {
		m.bpsRequests.accounts = make(map[int64]*bpsRequestAccount)
	}
	a := m.bpsRequests.accounts[accountID]
	if a == nil {
		a = &bpsRequestAccount{active: make(map[string]int)}
		m.bpsRequests.accounts[accountID] = a
	}
	key := bpsRequestSessionKey{accountID: accountID, scope: scope}
	binding := m.bpsRequests.sessions[key]
	if binding != nil && !seen[binding.node] {
		if binding.active > 0 {
			return nil, bpsAcquisitionError(bpsSelectionError("session_draining", "bound proxy is unavailable while requests are active"), 0)
		}
		delete(m.bpsRequests.sessions, key)
		binding = nil
	}
	if binding != nil && binding.active == 0 && now.Sub(binding.lastUsed) >= bpsSessionIdleTTL {
		delete(m.bpsRequests.sessions, key)
		binding = nil
	}
	var best string
	if binding != nil {
		best = binding.node
	} else {
		best = leastBusyBPSRequestExit(nodes, a)
		a.last = best
		// Under exceptional cardinality, preserve existing bindings and still
		// serve new requests through the authorized per-request pool.
		if scope != "" && len(m.bpsRequests.sessions) < bpsRequestMaxSessions {
			if m.bpsRequests.sessions == nil {
				m.bpsRequests.sessions = make(map[bpsRequestSessionKey]*bpsRequestSession)
			}
			binding = &bpsRequestSession{node: best}
			m.bpsRequests.sessions[key] = binding
		}
	}
	if binding != nil {
		binding.active++
		binding.lastUsed = now
	}
	a.active[best]++
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.bpsMu.Lock()
			defer m.bpsMu.Unlock()
			if binding != nil {
				binding.active--
				binding.lastUsed = time.Now()
			}
			a.active[best]--
			if a.active[best] == 0 {
				delete(a.active, best)
			}
			// Keep the rotation cursor until the next background projection. A
			// newly enabled account may send before that projection includes it.
			// SetBPSStaticRequestGroups removes unconfigured idle accounts.
		})
	}
	return &BPSLease{ProxyID: selected[best].ID, ProxyName: selected[best].Name, ProxyURL: m.bpsStatic[best], manager: m, node: best, generation: m.bpsHealthAtLocked(best, now).generation, release: release}, nil
}

func leastBusyBPSRequestExit(nodes []string, account *bpsRequestAccount) string {
	sort.Strings(nodes)
	start := sort.SearchStrings(nodes, account.last)
	if start < len(nodes) && nodes[start] == account.last {
		start++
	}
	best := nodes[start%len(nodes)]
	for i := 1; i < len(nodes); i++ {
		node := nodes[(start+i)%len(nodes)]
		if account.active[node] < account.active[best] {
			best = node
		}
	}
	return best
}

// Warming selected subsets is independent of the old global warm target. A
// rotating cursor prevents large or unhealthy groups from starving other groups.
func (m *Manager) warmBPSRequestGroups(ctx context.Context) {
	m.bpsWarmMu.Lock()
	defer m.bpsWarmMu.Unlock()
	m.bpsMu.Lock()
	now := time.Now()
	nodes := make([]string, 0)
	seen := map[string]bool{}
	for _, ids := range m.bpsRequests.groups {
		for _, id := range ids {
			p, ok := m.bpsRequests.records[id]
			if !ok || (p.ExpiresAt != nil && !p.ExpiresAt.After(now)) {
				continue
			}
			node := bpsStaticNodeKey(p.URL)
			h := m.bpsHealthAtLocked(node, now)
			if seen[node] || m.bpsCoolingLocked(node, now) || now.Add(30*time.Second).Before(h.verifiedUntil) {
				continue
			}
			seen[node] = true
			nodes = append(nodes, node)
		}
	}
	sort.Strings(nodes)
	start := 0
	if len(nodes) > 0 {
		start = m.bpsRequests.warmCursor % len(nodes)
	}
	m.bpsMu.Unlock()
	warmCtx, cancel := context.WithTimeout(context.WithValue(ctx, bpsWarmContextKey{}, true), 8*time.Second)
	defer cancel()
	for i := 0; i < len(nodes) && warmCtx.Err() == nil; i += bpsProbeParallelism {
		var wg sync.WaitGroup
		for j := i; j < min(i+bpsProbeParallelism, len(nodes)); j++ {
			node := nodes[(start+j)%len(nodes)]
			m.bpsMu.Lock()
			proxy := m.bpsStatic[node]
			m.bpsRequests.warmCursor = start + j + 1
			m.bpsMu.Unlock()
			if proxy == "" {
				continue
			}
			wg.Add(1)
			go func() { defer wg.Done(); _ = m.checkBPSHealth(warmCtx, node, proxy, true) }()
		}
		wg.Wait()
	}
}

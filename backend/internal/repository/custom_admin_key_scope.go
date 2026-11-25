package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	dbaccountgroup "github.com/Wei-Shaw/sub2api/ent/accountgroup"
	dbpredicate "github.com/Wei-Shaw/sub2api/ent/predicate"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

func (r *accountRepository) ScopedAdminList(ctx context.Context, p pagination.PaginationParams, platform, kind, status, search string) ([]service.Account, int64, error) {
	principal := adminaccess.RestrictedFromContext(ctx)
	if principal == nil {
		return nil, 0, fmt.Errorf("缺少受限身份")
	}
	alternatives := scopedAdminAccountPredicates(principal.Policy, "accounts.read")
	if len(alternatives) == 0 {
		return []service.Account{}, 0, nil
	}
	q := r.accountListFilteredQuery(platform, kind, status, search, 0, "").Where(dbaccount.Or(alternatives...))
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return nil, 0, err
	}
	// SQL 只投影模型映射子字段，不加载 Token、API Key、接入地址或其他凭证。
	var rows []struct {
		ID                     int64
		Name                   string
		Notes                  *string
		Platform               string
		Type                   string
		Status                 string
		Schedulable            bool
		Concurrency            int
		Priority               int
		ProxyID                *int64          `json:"proxy_id"`
		ParentAccountID        *int64          `json:"parent_account_id"`
		LoadFactor             *int            `json:"load_factor"`
		ExpiresAt              *time.Time      `json:"expires_at"`
		AutoPauseOnExpired     bool            `json:"auto_pause_on_expired"`
		RateLimitedAt          *time.Time      `json:"rate_limited_at"`
		RateLimitResetAt       *time.Time      `json:"rate_limit_reset_at"`
		OverloadUntil          *time.Time      `json:"overload_until"`
		TempUnschedulableUntil *time.Time      `json:"temp_unschedulable_until"`
		ErrorMessage           *string         `json:"error_message"`
		OwnerID                json.RawMessage `json:"owner_id"`
		NetworkExtra           json.RawMessage `json:"network_extra"`
		ModelMapping           json.RawMessage `json:"model_mapping"`
	}
	err = q.Offset(p.Offset()).Limit(p.Limit()).Order(dbaccount.ByID()).
		Select(dbaccount.FieldID, dbaccount.FieldName, dbaccount.FieldNotes, dbaccount.FieldPlatform, dbaccount.FieldType, dbaccount.FieldStatus, dbaccount.FieldSchedulable, dbaccount.FieldConcurrency, dbaccount.FieldPriority,
			dbaccount.FieldProxyID, dbaccount.FieldParentAccountID, dbaccount.FieldLoadFactor, dbaccount.FieldExpiresAt, dbaccount.FieldAutoPauseOnExpired,
			dbaccount.FieldRateLimitedAt, dbaccount.FieldRateLimitResetAt, dbaccount.FieldOverloadUntil, dbaccount.FieldTempUnschedulableUntil, dbaccount.FieldErrorMessage).
		Aggregate(func(s *entsql.Selector) string {
			return s.C(dbaccount.FieldCredentials) + "->'model_mapping' AS model_mapping"
		}, func(s *entsql.Selector) string {
			return s.C(dbaccount.FieldExtra) + "->'credential_owner_account_id' AS owner_id"
		}, func(s *entsql.Selector) string {
			column := s.C(dbaccount.FieldExtra)
			return "jsonb_build_object('openai_oauth_responses_websockets_v2_mode'," + column + "->'openai_oauth_responses_websockets_v2_mode','openai_oauth_responses_websockets_v2_enabled'," + column + "->'openai_oauth_responses_websockets_v2_enabled') AS network_extra"
		}).Scan(ctx, &rows)
	if err != nil {
		return nil, 0, err
	}
	accounts := make([]service.Account, 0, len(rows))
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.ID)
	}
	groupsByAccount, groupIDsByAccount, err := r.scopedMonitorGroups(ctx, ids)
	if err != nil {
		return nil, 0, err
	}
	for _, row := range rows {
		var rawMapping map[string]any
		mapping := map[string]string{}
		if json.Unmarshal(row.ModelMapping, &rawMapping) == nil {
			for source, target := range rawMapping {
				if value, ok := target.(string); ok {
					mapping[source] = value
				}
			}
		}
		var credentials map[string]any
		if len(mapping) > 0 {
			credentials = map[string]any{"model_mapping": mapping}
		}
		extra := map[string]any{}
		_ = json.Unmarshal(row.NetworkExtra, &extra)
		var ownerID int64
		if json.Unmarshal(row.OwnerID, &ownerID) == nil && ownerID > 0 {
			extra[service.CredentialOwnerAccountIDKey] = ownerID
		}
		accounts = append(accounts, service.Account{ID: row.ID, Name: row.Name, Notes: row.Notes, Platform: row.Platform, Type: row.Type, Status: row.Status, Schedulable: row.Schedulable, Concurrency: row.Concurrency, Priority: row.Priority,
			GroupIDs: groupIDsByAccount[row.ID], Groups: groupsByAccount[row.ID], Credentials: credentials, Extra: extra,
			ProxyID: row.ProxyID, ParentAccountID: row.ParentAccountID, LoadFactor: row.LoadFactor, ExpiresAt: row.ExpiresAt, AutoPauseOnExpired: row.AutoPauseOnExpired,
			RateLimitedAt: row.RateLimitedAt, RateLimitResetAt: row.RateLimitResetAt, OverloadUntil: row.OverloadUntil, TempUnschedulableUntil: row.TempUnschedulableUntil,
			ErrorMessage: derefString(row.ErrorMessage)})
	}
	return accounts, int64(total), nil
}

func (r *accountRepository) scopedMonitorGroups(ctx context.Context, ids []int64) (map[int64][]*service.Group, map[int64][]int64, error) {
	groups := map[int64][]*service.Group{}
	groupIDs := map[int64][]int64{}
	if len(ids) == 0 {
		return groups, groupIDs, nil
	}
	rows, err := r.client.QueryContext(ctx, `SELECT ag.account_id,ag.group_id,CASE WHEN g.id IS NULL OR g.deleted_at IS NOT NULL THEN 'deleted' ELSE g.status END
	FROM account_groups ag LEFT JOIN groups g ON g.id=ag.group_id WHERE ag.account_id=ANY($1) ORDER BY ag.account_id,ag.group_id`, pq.Array(ids))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var accountID, groupID int64
		var status string
		if err = rows.Scan(&accountID, &groupID, &status); err != nil {
			return nil, nil, err
		}
		groupIDs[accountID] = append(groupIDs[accountID], groupID)
		groups[accountID] = append(groups[accountID], &service.Group{ID: groupID, Status: status})
	}
	return groups, groupIDs, rows.Err()
}

// ScopedAdminCommitted 提交后按实际操作删除或更新缓存，避免对已删除账户读取快照。
func (r *accountRepository) ScopedAdminCommitted(ctx context.Context, id int64, deleted bool) {
	if deleted {
		r.deleteSchedulerAccountSnapshot(ctx, id)
		return
	}
	r.syncSchedulerAccountSnapshot(ctx, id)
}

// scopedAdminBindGroups 只增删变化的关联，保留现有关联的优先级和创建时间。
// 调用者已在受限事务内验权并合并不可操作的分组，不得回退到全量重建。
func (r *accountRepository) scopedAdminBindGroups(ctx context.Context, accountID int64, groupIDs []int64) error {
	if dbent.TxFromContext(ctx) == nil {
		return fmt.Errorf("受限分组写入需要数据库事务")
	}
	groupIDs, err := lockOAuthDegradationGroupBindings(ctx, r.client, accountID, groupIDs)
	if err != nil {
		return err
	}
	existing, err := r.loadAccountGroupIDs(ctx, accountID)
	if err != nil {
		return err
	}
	added, removed := []int64{}, []int64{}
	for _, id := range groupIDs {
		if !slices.Contains(existing, id) && !slices.Contains(added, id) {
			added = append(added, id)
		}
	}
	for _, id := range existing {
		if !slices.Contains(groupIDs, id) {
			removed = append(removed, id)
		}
	}
	if len(added) == 0 && len(removed) == 0 {
		return nil
	}
	if err := lockLiveGroups(ctx, r.client, added); err != nil {
		return err
	}
	if len(removed) > 0 {
		if _, err := r.client.AccountGroup.Delete().Where(dbaccountgroup.AccountIDEQ(accountID), dbaccountgroup.GroupIDIn(removed...)).Exec(ctx); err != nil {
			return err
		}
	}
	for _, id := range added {
		if _, err := r.client.AccountGroup.Create().SetAccountID(accountID).SetGroupID(id).SetPriority(slices.Index(groupIDs, id) + 1).Save(ctx); err != nil {
			return err
		}
	}
	return enqueueSchedulerOutbox(ctx, r.sql, service.SchedulerOutboxEventAccountGroupsChanged, &accountID, nil, buildSchedulerGroupPayload(mergeGroupIDs(existing, groupIDs)))
}

// scopedAdminAccountPredicates 保证读取和导入候选查询使用相同的范围语义。
func scopedAdminAccountPredicates(policy adminaccess.Policy, permission string) []dbpredicate.Account {
	alternatives := []dbpredicate.Account{}
	for _, rule := range policy.RulesFor(permission) {
		if len(rule.AccountTypes) == 0 {
			continue
		}
		terms := []dbpredicate.Account{dbaccount.TypeIn(rule.AccountTypes...)}
		if len(rule.Platforms) > 0 {
			terms = append(terms, dbaccount.PlatformIn(rule.Platforms...))
		}
		if rule.AllGroups && !rule.IncludeUngrouped {
			terms = append(terms, dbaccount.HasAccountGroups())
		}
		if !rule.AllGroups {
			groups := []dbpredicate.Account{}
			if len(rule.GroupIDs) > 0 {
				groups = append(groups, dbaccount.HasAccountGroupsWith(dbaccountgroup.GroupIDIn(rule.GroupIDs...)))
			}
			if rule.IncludeUngrouped {
				groups = append(groups, dbaccount.Not(dbaccount.HasAccountGroups()))
			}
			if len(groups) == 0 {
				continue
			}
			terms = append(terms, dbaccount.Or(groups...))
		}
		alternatives = append(alternatives, dbaccount.And(terms...))
	}
	return alternatives
}

// ScopedAdminCodexCandidates 按导入身份逐级匹配，最多返回一个候选的身份字段。
// 不向应用加载其他账户的凭证；真正合并凭证仍在写事务锁内进行。
func (r *accountRepository) ScopedAdminCodexCandidates(ctx context.Context, identity service.ScopedCodexIdentity) ([]service.Account, error) {
	principal := adminaccess.RestrictedFromContext(ctx)
	if principal == nil {
		return nil, fmt.Errorf("缺少受限身份")
	}
	scope := scopedAdminAccountPredicates(principal.Policy, "accounts.credentials")
	if len(scope) == 0 {
		return []service.Account{}, nil
	}
	for _, match := range scopedCodexIdentityPredicates(identity) {
		var rows []struct {
			ID          int64
			Credentials json.RawMessage
		}
		err := r.accountListFilteredQuery(service.PlatformOpenAI, service.AccountTypeOAuth, "", "", 0, "").
			Where(dbaccount.Or(scope...), match).Order(dbaccount.ByID(entsql.OrderDesc())).Limit(1).
			Select(dbaccount.FieldID).Aggregate(func(s *entsql.Selector) string {
			c := s.C(dbaccount.FieldCredentials)
			return "jsonb_build_object('chatgpt_account_id', " + c + "->'chatgpt_account_id', 'chatgpt_user_id', " + c + "->'chatgpt_user_id', 'email', " + c + "->'email', 'access_token', " + c + "->'access_token') AS credentials"
		}).Scan(ctx, &rows)
		if err != nil {
			return nil, err
		}
		if len(rows) != 0 {
			var credentials map[string]any
			if err := json.Unmarshal(rows[0].Credentials, &credentials); err != nil {
				return nil, err
			}
			return []service.Account{{ID: rows[0].ID, Credentials: credentials}}, nil
		}
	}
	return []service.Account{}, nil
}

// 与导入器的 account_user / user / email / access / account 优先级保持一致。
// account 回退必须排除明确属于其他用户的账户；仅 access token 和 Agent Identity 各有独立匹配规则。
func scopedCodexIdentityPredicates(identity service.ScopedCodexIdentity) []dbpredicate.Account {
	accountID, userID := strings.TrimSpace(identity.AccountID), strings.TrimSpace(identity.UserID)
	email, token := strings.ToLower(strings.TrimSpace(identity.Email)), strings.TrimSpace(identity.AccessToken)
	// 与 Go strings.TrimSpace 的 Unicode 空白集合一致，兼容旧凭证中的首尾空白。
	const whitespace = "\t\n\v\f\r \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
	field := func(key string) string {
		// 字段名均为下方固定常量；非法 JSON 类型按导入器规则视为空值。
		raw := `"accounts"."credentials"->'` + key + `'`
		value := `"accounts"."credentials"->>'` + key + `'`
		// 旧 JSON 数值经 Go 解码为 float64：保留同样的精度，展开指数并去掉尾随小数零。
		number := "trim_scale(((" + value + ")::double precision)::text::numeric)::text"
		return "btrim(CASE jsonb_typeof(" + raw + ") WHEN 'string' THEN " + value + " WHEN 'number' THEN " + number + " ELSE '' END, '" + whitespace + "')"
	}
	a, u, e, access := field("chatgpt_account_id"), field("chatgpt_user_id"), field("email"), field("access_token")
	match := func(expr string, args ...any) dbpredicate.Account {
		parts := strings.Split(expr, "?")
		return func(s *entsql.Selector) {
			s.Where(entsql.P(func(b *entsql.Builder) {
				for i, part := range parts {
					b.WriteString(part)
					if i < len(args) {
						b.Arg(args[i])
					}
				}
			}))
		}
	}
	predicates := []dbpredicate.Account{}
	if !identity.AgentIdentity && !identity.AccessTokenOnly {
		switch {
		case accountID != "" && userID != "":
			predicates = append(predicates, match(a+" = ? AND "+u+" = ?", accountID, userID))
		case userID != "":
			predicates = append(predicates, match(a+" = '' AND "+u+" = ?", userID))
		case accountID == "" && email != "":
			predicates = append(predicates, match(a+" = '' AND "+u+" = '' AND lower("+e+") = ?", email))
		}
	}
	if !identity.AgentIdentity && token != "" {
		predicates = append(predicates, match(access+" = ?", token))
	}
	if accountID != "" && (identity.AgentIdentity || !identity.AccessTokenOnly) {
		if userID == "" {
			predicates = append(predicates, match(a+" = ?", accountID))
		} else {
			predicates = append(predicates, match(a+" = ? AND ("+u+" = '' OR "+u+" = ?)", accountID, userID))
		}
	}
	return predicates
}

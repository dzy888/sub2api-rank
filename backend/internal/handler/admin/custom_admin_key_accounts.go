package admin

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func scopedAdminAccountDTO(a *service.Account, p adminaccess.Policy) gin.H {
	resource := adminaccess.Account{ID: a.ID, Type: a.Type, Platform: a.Platform, GroupIDs: a.GroupIDs}
	groups := []int64{}
	for _, g := range a.GroupIDs {
		if p.AllowsGroup("accounts.read", resource, g) {
			groups = append(groups, g)
		}
	}
	result := gin.H{"id": a.ID, "name": a.Name, "notes": a.Notes, "platform": a.Platform, "type": a.Type, "status": a.Status, "schedulable": a.Schedulable, "concurrency": a.Concurrency, "priority": a.Priority, "group_ids": groups}
	result["load_factor"] = a.LoadFactor
	result["proxy_id"] = a.ProxyID
	result["configured_concurrency"] = a.Concurrency
	result["parent_account_id"] = a.ParentAccountID
	result["auto_pause_on_expired"] = a.AutoPauseOnExpired
	var expiresAt *int64
	if a.ExpiresAt != nil {
		expires := a.ExpiresAt.Unix()
		expiresAt = &expires
	}
	result["expires_at"] = expiresAt
	result["rate_limited_at"] = a.RateLimitedAt
	result["rate_limit_reset_at"] = a.RateLimitResetAt
	result["overload_until"] = a.OverloadUntil
	result["temp_unschedulable_until"] = a.TempUnschedulableUntil
	// Never echo raw provider errors or temporary-isolation messages.
	result["temp_unschedulable_reason"] = ""
	if a.TempUnschedulableUntil != nil && time.Now().Before(*a.TempUnschedulableUntil) {
		result["temp_unschedulable_reason"] = "temporary_unavailable"
	}
	result["auth_invalid_reason"] = service.AccountAuthInvalidReason(a)
	result["error_message"] = ""
	if service.AccountAuthInvalidReason(a) != "" {
		// Keep the existing push scanner compatible without exposing its raw body.
		result["error_message"] = "Unauthorized (401): authentication invalid"
	}
	ownerID := a.CredentialOwnerAccountID()
	result["credential_owner_account_id"] = ownerID
	result["source_account_id"] = ownerID
	if extra := service.ScopedAdminSafeExtra(a); len(extra) > 0 {
		result["extra"] = extra
	}
	groupStates := make([]gin.H, 0, len(groups))
	for _, group := range a.Groups {
		if group != nil && slices.Contains(groups, group.ID) {
			groupStates = append(groupStates, gin.H{"id": group.ID, "status": group.Status})
		}
	}
	result["groups"] = groupStates
	// 兼容原账户读取结构，仅返回模型映射，凭证及扩展字段继续脱敏。
	if p.AllowsAccount("accounts.read", resource) {
		mapping := map[string]string{}
		switch raw := a.Credentials["model_mapping"].(type) {
		case map[string]string:
			for source, target := range raw {
				mapping[source] = target
			}
		case map[string]any:
			for source, target := range raw {
				if value, ok := target.(string); ok {
					mapping[source] = value
				}
			}
		}
		result["credentials"] = gin.H{"model_mapping": mapping}
	}
	return result
}

// ScopedAdminKeyAccounts 在原处理器前完成独立授权和安全输出。
func (h *AccountHandler) ScopedAdminKeyAccounts(c *gin.Context) {
	principal := adminaccess.RestrictedFromContext(c.Request.Context())
	if principal == nil {
		return
	}
	path := c.FullPath()
	if !strings.HasPrefix(path, "/api/v1/admin/accounts") {
		return
	}
	c.Abort()
	base := "/api/v1/admin/accounts"
	// 静态默认映射不读取任何账户；权限独立于账户查看及凭证写入。
	if path == base+"/antigravity/default-model-mapping" && c.Request.Method == "GET" {
		if !adminaccess.RouteAllowed(principal.Policy, c.Request.Method, path) {
			response.Error(c, 403, "默认模型读取未授权")
			return
		}
		h.GetAntigravityDefaultModelMapping(c)
		return
	}
	if path == base+"/import/codex-session" && c.Request.Method == "POST" {
		h.scopedAdminImportCodexSession(c)
		return
	}
	if path == base+"/:id/proxy-replicas" && c.Request.Method == "POST" {
		h.CreateProxyReplicas(c)
		return
	}
	if path == base+"/:id/proxy-replicas/:operation_id" && c.Request.Method == "GET" {
		h.GetProxyReplicaOperation(c)
		return
	}
	if path == base+"/:id/proxy-replicas/relogin-recoveries/capabilities" && c.Request.Method == "GET" {
		h.ProxyRecoveryCapabilities(c)
		return
	}
	if path == base+"/:id/proxy-replicas/relogin-recoveries" && c.Request.Method == "POST" {
		h.PrepareProxyRecovery(c)
		return
	}
	if path == base+"/:id/proxy-replicas/relogin-recoveries/:operation_id" && c.Request.Method == "GET" {
		h.GetProxyRecovery(c)
		return
	}
	if path == base+"/:id/proxy-replicas/relogin-recoveries/:operation_id/resume" && c.Request.Method == "POST" {
		h.ResumeProxyRecovery(c)
		return
	}
	api, ok := h.adminService.(service.ScopedAdminAccounts)
	if !ok {
		response.Error(c, 503, "受限账户服务不可用")
		return
	}
	ctx := c.Request.Context()
	if path == base && c.Request.Method == "GET" {
		page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
		size, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
		if page < 1 {
			page = 1
		}
		if size < 1 || size > 1000 {
			response.BadRequest(c, "page_size 必须在 1 至 1000 之间")
			return
		}
		accounts, total, err := api.ScopedAdminList(ctx, pagination.PaginationParams{Page: page, PageSize: size}, c.Query("platform"), c.Query("type"), c.Query("status"), c.Query("search"))
		if err != nil {
			response.Error(c, 503, "账户查询失败")
			return
		}
		counts, err := h.scopedAccountConcurrency(ctx, accounts)
		if err != nil {
			response.Error(c, 503, "实时并发读取失败")
			return
		}
		items := make([]gin.H, 0, len(accounts))
		for i := range accounts {
			if principal.Policy.AllowsAccount("accounts.read", adminaccess.Account{ID: accounts[i].ID, GroupIDs: accounts[i].GroupIDs, Type: accounts[i].Type, Platform: accounts[i].Platform}) {
				item := scopedAdminAccountDTO(&accounts[i], principal.Policy)
				item["current_concurrency"] = counts[accounts[i].ID]
				items = append(items, item)
			}
		}
		c.Header("Cache-Control", "no-store")
		response.Success(c, gin.H{"items": items, "total": total, "page": page, "page_size": size, "snapshot_at": time.Now().UTC(), "snapshot_consistency": "live_page"})
		return
	}
	id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if (path == base+"/:id" || path == base+"/:id/models") && c.Request.Method == "GET" {
		a, err := h.adminService.GetAccount(ctx, id)
		if err != nil || a == nil || !principal.Policy.AllowsAccount("accounts.read", adminaccess.Account{ID: id, GroupIDs: accountGroupIDs(a), Type: accountType(a), Platform: accountPlatform(a)}) {
			response.Error(c, 404, "账户不存在")
			return
		}
		if path == base+"/:id/models" {
			h.respondAvailableModels(c, a)
			return
		}
		counts, err := h.scopedAccountConcurrency(ctx, []service.Account{*a})
		if err != nil {
			response.Error(c, 503, "实时并发读取失败")
			return
		}
		item := scopedAdminAccountDTO(a, principal.Policy)
		item["current_concurrency"] = counts[a.ID]
		c.Header("Cache-Control", "no-store")
		response.Success(c, item)
		return
	}
	if path == base+"/bulk-update" && c.Request.Method == "POST" {
		h.scopedAdminBulkUpdate(c, api)
		return
	}

	fields := map[string]json.RawMessage{}
	action := ""
	var run func(context.Context, *service.Account) (*service.Account, error)
	switch {
	case path == base && c.Request.Method == "POST":
		id = 0
		if c.ShouldBindJSON(&fields) != nil || fields == nil {
			response.Error(c, 400, "请求格式无效")
			return
		}
	case path == base+"/:id" && c.Request.Method == "PUT":
		if c.ShouldBindJSON(&fields) != nil || len(fields) == 0 {
			response.Error(c, 400, "请求格式无效")
			return
		}
	case path == base+"/:id" && c.Request.Method == "DELETE":
		action = "accounts.delete"
	case path == base+"/:id/schedulable" && c.Request.Method == "POST":
		if c.ShouldBindJSON(&fields) != nil || len(fields) != 1 || fields["schedulable"] == nil {
			response.Error(c, 400, "仅支持 schedulable 字段")
			return
		}
	case path == base+"/:id/clear-error" && c.Request.Method == "POST":
		action = "accounts.recover"
	case path == base+"/:id/refresh" && c.Request.Method == "POST":
		action = "accounts.refresh"
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		a, err := h.adminService.GetAccount(ctx, id)
		if err != nil || a == nil || !principal.Policy.AllowsAccount(action, adminaccess.Account{ID: id, GroupIDs: accountGroupIDs(a), Type: accountType(a), Platform: accountPlatform(a)}) {
			response.Error(c, 404, "账户不存在")
			return
		}

		run = func(txctx context.Context, a *service.Account) (*service.Account, error) {
			updated, _, err := h.refreshSingleAccount(txctx, a)
			if err != nil {
				code := infraerrors.Code(err)
				if code != 400 && code != 403 && code != 404 && code != 409 && code != 503 {
					return nil, infraerrors.New(502, "SCOPED_REFRESH_FAILED", "上游刷新失败")
				}
			}
			return updated, err
		}
	default:
		response.Error(c, 403, "此账户操作未开放给受限管理密钥")
		return
	}
	if id <= 0 && path != base {
		response.Error(c, 400, "账户 ID 无效")
		return
	}
	a, err := api.ScopedAdminWrite(ctx, id, action, fields, run)
	if err != nil {
		scopedAdminAccountError(c, err)
		return
	}
	if id > 0 && fields["credentials"] != nil {
		h.invalidateScopedAccountToken(ctx, a)
	}
	response.Success(c, gin.H{"id": a.ID, "status": a.Status})
}

func (h *AccountHandler) scopedAccountConcurrency(ctx context.Context, accounts []service.Account) (map[int64]int, error) {
	ids := make([]int64, 0, len(accounts))
	for _, account := range accounts {
		ids = append(ids, account.ID)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return h.concurrencyService.GetAccountConcurrencySnapshot(ctx, ids)
}
func accountGroupIDs(a *service.Account) []int64 {
	if a == nil {
		return nil
	}
	return a.GroupIDs
}
func accountType(a *service.Account) string {
	if a == nil {
		return ""
	}
	return a.Type
}
func accountPlatform(a *service.Account) string {
	if a == nil {
		return ""
	}
	return a.Platform
}

func scopedAdminAccountError(c *gin.Context, err error) {
	code, message := scopedAdminAccountErrorDetails(err)
	response.Error(c, code, message)
}

func scopedAdminAccountErrorDetails(err error) (int, string) {
	code := infraerrors.Code(err)
	message := "账户服务暂不可用"
	switch code {
	case 400:
		message = "请求参数无效"
	case 403:
		message = "操作未授权"
	case 404:
		message = "账户不存在"
	case 409:
		message = "账户正在处理，请稍后重试"
	case 502:
		message = "上游刷新失败"
	default:
		code = 503
	}
	return code, message
}

// scopedAdminBulkUpdate 沿用旧批量接口与结果结构，每个账户仍走锁内逐字段验权。
func (h *AccountHandler) scopedAdminBulkUpdate(c *gin.Context, api service.ScopedAdminAccounts) {
	var fields map[string]json.RawMessage
	if c.ShouldBindJSON(&fields) != nil || fields == nil {
		response.Error(c, 400, "请求格式无效")
		return
	}
	var ids []int64
	if json.Unmarshal(fields["account_ids"], &ids) != nil || len(ids) == 0 || len(ids) > 100 {
		response.Error(c, 400, "请明确指定 1 至 100 个账户 ID")
		return
	}
	delete(fields, "account_ids")
	// 不把全站筛选条件传给原批量服务，以免绕开账户范围。
	if _, exists := fields["filters"]; exists || len(fields) == 0 {
		response.Error(c, 400, "受限批量更新须明确指定账户及修改字段")
		return
	}
	uniqueIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			response.Error(c, 400, "账户 ID 无效")
			return
		}
		if !slices.Contains(uniqueIDs, id) {
			uniqueIDs = append(uniqueIDs, id)
		}
	}
	result := service.BulkUpdateAccountsResult{SuccessIDs: []int64{}, FailedIDs: []int64{}, Results: []service.BulkUpdateAccountResult{}}
	for _, id := range uniqueIDs {
		account, err := api.ScopedAdminWrite(c.Request.Context(), id, "", fields, nil)
		item := service.BulkUpdateAccountResult{AccountID: id, Success: err == nil}
		if err != nil {
			_, item.Error = scopedAdminAccountErrorDetails(err)
			result.Failed++
			result.FailedIDs = append(result.FailedIDs, id)
		} else {
			if fields["credentials"] != nil {
				h.invalidateScopedAccountToken(c.Request.Context(), account)
			}
			result.Success++
			result.SuccessIDs = append(result.SuccessIDs, id)
		}
		result.Results = append(result.Results, item)
	}
	response.Success(c, result)
}

// 凭证写入必须在数据库提交成功后清理令牌缓存，回滚不能发布旧状态。
func (h *AccountHandler) invalidateScopedAccountToken(ctx context.Context, account *service.Account) {
	if h.tokenCacheInvalidator == nil || account == nil {
		return
	}
	service.AfterAccountCommit(ctx, func(committed context.Context) {
		_ = h.tokenCacheInvalidator.InvalidateToken(committed, account)
	})
}

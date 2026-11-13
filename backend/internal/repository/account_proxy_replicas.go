package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	dbproxy "github.com/Wei-Shaw/sub2api/ent/proxy"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

var _ service.ProxyReplicaRepository = (*accountRepository)(nil)

func (r *accountRepository) AuthorizeProxyReplicaOwnerWrite(ctx context.Context, id int64, permissions []string) error {
	if scoped := r.accountTransactionScope(ctx); scoped != r {
		return scoped.AuthorizeProxyReplicaOwnerWrite(ctx, id, permissions)
	}
	if adminaccess.FromContext(ctx) == nil {
		return nil
	}
	if err := proxyRecoveryLock(ctx, r.client, `SELECT id FROM accounts WHERE id=$1 OR (extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=$2) ORDER BY id FOR UPDATE`, id, sourceIDString(id)); err != nil {
		return err
	}
	var resources []adminaccess.Account
	if err := proxyRecoveryJSON(ctx, r.client, `SELECT coalesce(jsonb_agg(jsonb_build_object('ID',id,'Platform',platform,'Type',type,
	'GroupIDs',coalesce((SELECT jsonb_agg(group_id) FROM account_groups WHERE account_id=accounts.id),'[]'))),'[]')
	FROM accounts WHERE id=$1 OR (extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=$2 AND deleted_at IS NULL)`, &resources, id, sourceIDString(id)); err != nil {
		return err
	}
	principal := adminaccess.FromContext(ctx)
	for _, a := range resources {
		for _, permission := range permissions {
			if !principal.Policy.AllowsAccountAndGroups(permission, a) {
				return infraerrors.Forbidden("SCOPED_ACCOUNT_FORBIDDEN", "关联代理副本或分组未授权")
			}
		}
	}
	return nil
}

func (r *accountRepository) authorizeStoredReplicaScope(ctx context.Context, permission string, sourceID int64, key string) error {
	if adminaccess.FromContext(ctx) == nil {
		return nil
	}
	if err := proxyRecoveryLock(ctx, r.client, `SELECT id FROM accounts WHERE id=$1 OR id IN
	(SELECT account_id FROM account_proxy_replica_items WHERE operation_id=$2) ORDER BY id FOR UPDATE`, sourceID, key); err != nil {
		return err
	}
	var resources []adminaccess.Account
	err := proxyRecoveryJSON(ctx, r.client, `SELECT coalesce(jsonb_agg(jsonb_build_object('ID',a.id,'Type',a.type,'Platform',a.platform,
	'GroupIDs',coalesce((SELECT jsonb_agg(group_id ORDER BY group_id) FROM account_groups WHERE account_id=a.id),'[]'))),'[]')
	FROM accounts a WHERE a.id=$1 OR a.id IN(SELECT account_id FROM account_proxy_replica_items WHERE operation_id=$2)`, &resources, sourceID, key)
	if err != nil {
		return err
	}
	if len(resources) == 0 {
		return recoveryHidden()
	}
	for _, a := range resources {
		if !recoveryAllowed(ctx, permission, a) {
			return recoveryHidden()
		}
	}
	return nil
}

func (r *accountRepository) IsProxyReplicaSourceID(ctx context.Context, id int64) (bool, error) {
	rows, err := r.client.QueryContext(ctx, `SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1 AND extra->>'proxy_replica_source'='true')`, id)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	var source bool
	if !rows.Next() {
		return false, fmt.Errorf("source fence returned no result")
	}
	if err := rows.Scan(&source); err != nil {
		return false, err
	}
	return source, rows.Err()
}

func (r *accountRepository) CreateProxyReplicas(ctx context.Context, sourceID int64, key string, in service.ProxyReplicasInput, hash string) (operation *service.ProxyReplicaOperation, failure error) {
	config, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	// Claim identity durably before work so failed attempts also reserve their
	// configuration. The following transaction serializes execution separately.
	_, err = r.client.ExecContext(ctx, `INSERT INTO account_proxy_replica_operations(operation_id,source_account_id,config_hash,config)
		VALUES($1,$2,$3,$4) ON CONFLICT(operation_id) DO NOTHING`, key, sourceID, hash, string(config))
	if err != nil {
		return nil, err
	}
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
		if failure != nil {
			message := "proxy replica transaction failed; retry this operation"
			if code := infraerrors.Code(failure); code >= 400 && code < 500 {
				message = infraerrors.Message(failure)
			}
			persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			_, _ = r.client.ExecContext(persistCtx, `UPDATE account_proxy_replica_operations SET status='failed',error=$4,updated_at=NOW() WHERE operation_id=$1 AND source_account_id=$2 AND config_hash=$3 AND NOT completed`, key, sourceID, hash, message)
		}
	}()
	client := tx.Client()
	rows, err := client.QueryContext(ctx, `SELECT source_account_id,config_hash,completed FROM account_proxy_replica_operations WHERE operation_id=$1 FOR UPDATE`, key)
	if err != nil {
		return nil, err
	}
	var storedID int64
	var storedHash string
	var completed bool
	if !rows.Next() {
		_ = rows.Close()
		return nil, fmt.Errorf("operation not found after insert")
	}
	err = rows.Scan(&storedID, &storedHash, &completed)
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	if storedID != sourceID || storedHash != hash {
		return nil, infraerrors.Conflict("PROXY_REPLICA_OPERATION_CONFLICT", "operation ID already has different configuration")
	}
	scoped := &accountRepository{client: client, sql: client}
	if err := scoped.authorizeStoredReplicaScope(ctx, "accounts.proxy_replicas.create", sourceID, key); err != nil {
		return nil, err
	}
	if completed {
		result, err := scoped.GetProxyReplicaOperation(ctx, sourceID, key)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return result, nil // Never re-enable an administrator-paused replica.
	}

	// Lock the source before its configuration is copied. Deletion, disable and
	// credential replacement cannot race the final verification/activation.
	sourceEntity, err := client.Account.Query().Where(dbaccount.IDEQ(sourceID)).ForUpdate().Only(ctx)
	if err != nil {
		return nil, translatePersistenceError(err, service.ErrAccountNotFound, nil)
	}
	source := accountEntityToService(sourceEntity)
	if !service.ProxyReplicaOwnerUsable(source, time.Now()) || source.IsShadow() || source.IsProxyReplica() || source.HasManagedOpenAIConnections() || source.IsSIWCEnabled() || source.IsOpenAIAgentIdentity() || source.IsOpenAIPersonalAccessToken() {
		return nil, infraerrors.BadRequest("INVALID_CREDENTIAL_OWNER", "source must be an available standalone OpenAI OAuth account")
	}
	if source.GetCredential("access_token") == "" || source.GetCredential("refresh_token") == "" {
		return nil, infraerrors.BadRequest("INVALID_CREDENTIAL_OWNER", "source OAuth credentials are incomplete")
	}
	hydrated, err := scoped.accountsToService(ctx, []*dbent.Account{sourceEntity})
	if err != nil {
		return nil, err
	}
	source = &hydrated[0]
	groups := append([]service.AccountGroup(nil), source.AccountGroups...)
	if principal := adminaccess.FromContext(ctx); principal != nil {
		resource := adminaccess.Account{ID: source.ID, GroupIDs: source.GroupIDs, Type: source.Type, Platform: source.Platform}
		if !slices.Contains(principal.Policy.GlobalPermissions, "proxies.read") ||
			!principal.Policy.AllowsAccountAndGroups("accounts.proxy_replicas.create", resource) {
			return nil, infraerrors.NotFound("PROXY_REPLICA_SOURCE_NOT_FOUND", "proxy replica source account not found")
		}
	}
	if err := lockLiveGroups(ctx, client, source.GroupIDs); err != nil {
		return nil, err
	}
	// Lock every requested proxy in deterministic order for the entire operation.
	proxies := make(map[int64]*service.Proxy, len(in.Proxies))
	for _, p := range in.Proxies {
		entity, e := client.Proxy.Query().Where(dbproxy.IDEQ(p.ProxyID)).ForShare().Only(ctx)
		if e == nil {
			proxies[p.ProxyID] = proxyEntityToService(entity)
		}
		if e != nil || proxies[p.ProxyID] == nil || !proxies[p.ProxyID].IsActive() || proxies[p.ProxyID].IsExpired(time.Now()) {
			result := &service.ProxyReplicaOperation{OperationID: key, SourceAccountID: sourceID, Status: "failed", Error: "proxy validation failed", Replicas: []service.ProxyReplicaResult{{ProxyID: p.ProxyID, Status: "failed", Error: "proxy missing, disabled or expired"}}}
			_, err = client.ExecContext(ctx, `UPDATE account_proxy_replica_operations SET status='failed',error=$2,updated_at=NOW() WHERE operation_id=$1`, key, fmt.Sprintf("proxy %d missing, disabled or expired", p.ProxyID))
			if err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return result, nil
		}
	}
	// Persist source ownership and its scheduling fence before creating children.
	if _, err = client.ExecContext(ctx, `SELECT set_config('sub2api.proxy_replica_operation',$1,true)`, key); err != nil {
		return nil, err
	}
	_, err = client.ExecContext(ctx, `UPDATE accounts SET schedulable=false,auto_pause_on_expired=true,extra=extra||'{"proxy_replica_source":true}'::jsonb,updated_at=NOW() WHERE id=$1`, sourceID)
	if err != nil {
		return nil, err
	}
	for _, p := range in.Proxies {
		rows, err := client.QueryContext(ctx, `SELECT account_id FROM account_proxy_replica_items WHERE operation_id=$1 AND proxy_id=$2`, key, p.ProxyID)
		if err != nil {
			return nil, err
		}
		exists := rows.Next()
		_ = rows.Close()
		if exists {
			continue
		}
		rows, err = client.QueryContext(ctx, `SELECT id FROM accounts WHERE deleted_at IS NULL AND extra ? 'credential_owner_account_id' AND extra->>'credential_owner_account_id'=$1 AND proxy_id=$2`, fmt.Sprint(sourceID), p.ProxyID)
		if err != nil {
			return nil, err
		}
		duplicate := rows.Next()
		_ = rows.Close()
		if duplicate {
			return nil, infraerrors.Conflict("PROXY_REPLICA_PROXY_ALREADY_BOUND", fmt.Sprintf("proxy %d already has a replica for this source", p.ProxyID))
		}
		credentials := map[string]any{}
		for _, k := range []string{"model_mapping", "compact_model_mapping", "model_mapping_mode", "plan_type", "chatgpt_account_id", "chatgpt_user_id", "organization_id", "email"} {
			if v, ok := source.Credentials[k]; ok {
				credentials[k] = v
			}
		}
		extra, err := service.ProxyReplicaExtra(source.Extra)
		if err != nil {
			return nil, err
		}
		extra[service.CredentialOwnerAccountIDKey] = sourceID
		extra[service.ProxyReplicaOperationKey] = key
		extra["openai_oauth_responses_websockets_v2_mode"] = in.WSMode
		proxyID := p.ProxyID
		account := &service.Account{
			Name:     fmt.Sprintf("%s [proxy %d]", truncateReplicaName(source.Name), p.ProxyID),
			Platform: source.Platform, Type: source.Type,
			Credentials: credentials, Extra: extra,
			ProxyID: &proxyID, Concurrency: p.Concurrency, Priority: source.Priority,
			Status: service.StatusActive, Schedulable: false, AutoPauseOnExpired: true,
			RateMultiplier: source.RateMultiplier, GroupRateMultiplier: source.GroupRateMultiplier,
		}
		if err := scoped.CreateWithAccountGroups(ctx, account, groups); err != nil {
			return nil, err
		}
		_, err = client.ExecContext(ctx, `INSERT INTO account_proxy_replica_items(operation_id,proxy_id,account_id) VALUES($1,$2,$3)`, key, p.ProxyID, account.ID)
		if err != nil {
			return nil, err
		}
	}
	// Verify the durable configuration before allowing any child to receive
	// traffic. Any error rolls back this transaction; committed earlier items
	// remain paused and are reused on retry.
	result, err := scoped.GetProxyReplicaOperation(ctx, sourceID, key)
	if err != nil {
		return nil, err
	}
	if len(result.Replicas) != len(in.Proxies) {
		return nil, fmt.Errorf("proxy replica count verification failed")
	}
	for i, replica := range result.Replicas {
		p := in.Proxies[i]
		if replica.ProxyID != p.ProxyID || replica.Concurrency != p.Concurrency || replica.WSMode != in.WSMode || replica.CredentialOwnerAccountID != sourceID || replica.Status == "deleted" {
			return nil, fmt.Errorf("proxy replica configuration verification failed for proxy %d", p.ProxyID)
		}
	}
	if !service.ProxyReplicaOwnerUsable(source, time.Now()) {
		return nil, infraerrors.Conflict("CREDENTIAL_OWNER_UNAVAILABLE", "source became unavailable before activation")
	}
	for _, proxy := range proxies {
		if proxy.IsExpired(time.Now()) {
			return nil, infraerrors.Conflict("PROXY_EXPIRED", "proxy expired before activation")
		}
	}
	status := "ready"
	if in.ActivateOnSuccess {
		status = "active"
		_, err = client.ExecContext(ctx, `UPDATE accounts SET schedulable=true,updated_at=NOW() WHERE id IN (SELECT account_id FROM account_proxy_replica_items WHERE operation_id=$1) AND deleted_at IS NULL`, key)
		if err != nil {
			return nil, err
		}
	}
	_, err = client.ExecContext(ctx, `UPDATE account_proxy_replica_operations SET status=$2,completed=true,error='',updated_at=NOW() WHERE operation_id=$1`, key, status)
	if err != nil {
		return nil, err
	}
	for _, replica := range result.Replicas {
		id := replica.AccountID
		if err := enqueueSchedulerOutbox(ctx, client, service.SchedulerOutboxEventAccountChanged, &id, nil, nil); err != nil {
			return nil, err
		}
	}
	if err := enqueueSchedulerOutbox(ctx, client, service.SchedulerOutboxEventAccountChanged, &sourceID, nil, nil); err != nil {
		return nil, err
	}
	result, err = scoped.GetProxyReplicaOperation(ctx, sourceID, key)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func truncateReplicaName(name string) string {
	runes := []rune(name)
	if len(runes) > 65 {
		runes = runes[:65]
	}
	return string(runes)
}

func (r *accountRepository) GetProxyReplicaOperation(ctx context.Context, sourceID int64, key string) (*service.ProxyReplicaOperation, error) {
	rows, err := r.client.QueryContext(ctx, `SELECT source_account_id,status,error FROM account_proxy_replica_operations WHERE operation_id=$1 AND source_account_id=$2`, key, sourceID)
	if err != nil {
		return nil, err
	}
	result := &service.ProxyReplicaOperation{OperationID: key, Replicas: []service.ProxyReplicaResult{}}
	if !rows.Next() {
		_ = rows.Close()
		return nil, infraerrors.NotFound("PROXY_REPLICA_OPERATION_NOT_FOUND", "proxy replica operation not found")
	}
	err = rows.Scan(&result.SourceAccountID, &result.Status, &result.Error)
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = r.client.QueryContext(ctx, `SELECT i.account_id,i.proxy_id,a.extra,a.concurrency,a.schedulable,a.status,a.deleted_at,
		COALESCE((SELECT jsonb_agg(ag.group_id ORDER BY ag.group_id) FROM account_groups ag WHERE ag.account_id=i.account_id),'[]'::jsonb)
		FROM account_proxy_replica_items i LEFT JOIN accounts a ON a.id=i.account_id WHERE i.operation_id=$1 ORDER BY i.proxy_id`, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var item service.ProxyReplicaResult
		var extraJSON, groupsJSON []byte
		var concurrency *int
		var schedulable *bool
		var status *string
		var deletedAt *time.Time
		if err := rows.Scan(&item.AccountID, &item.ProxyID, &extraJSON, &concurrency, &schedulable, &status, &deletedAt, &groupsJSON); err != nil {
			return nil, err
		}
		item.CredentialOwnerAccountID = sourceID
		item.Status = "deleted"
		if status != nil && deletedAt == nil {
			item.Status = *status
			if concurrency != nil {
				item.Concurrency = *concurrency
			}
			if schedulable != nil {
				item.Schedulable = *schedulable
			}
			var extra map[string]any
			if err := json.Unmarshal(extraJSON, &extra); err != nil {
				return nil, err
			}
			item.WSMode, _ = extra["openai_oauth_responses_websockets_v2_mode"].(string)
			if !item.Schedulable && item.Status == service.StatusActive {
				item.Status = "paused"
			}
		}
		if err := json.Unmarshal(groupsJSON, &item.GroupIDs); err != nil {
			return nil, err
		}
		result.Replicas = append(result.Replicas, item)
	}
	if result.Status == "active" {
		for _, item := range result.Replicas {
			if !item.Schedulable || item.Status != "active" {
				result.Status = "paused"
				break
			}
		}
	}
	return result, rows.Err()
}

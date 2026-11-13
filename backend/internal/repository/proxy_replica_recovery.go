package repository

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

func sourceIDString(id int64) string { return strconv.FormatInt(id, 10) }

type proxyRecoveryPause struct {
	Fault      string
	Generation string
	Revision   int64
	Owned      bool
}
type proxyRecoveryStored struct {
	Source   int64
	Hash     string
	Config   service.ProxyRecoveryInput
	Baseline string
	Pauses   map[int64]proxyRecoveryPause
	Result   *service.ProxyRecoveryOperation
	Expired  bool
}
type proxyRecoveryState struct {
	ID              int64
	Owner           string
	Operation       string
	ProxyID         int64
	Concurrency     int
	WSMode          string
	Groups          []int64
	Platform        string
	Type            string
	Status          string
	Deleted         bool
	Expired         bool
	Schedulable     bool
	Unavailable     bool
	ProxyAvailable  bool
	BindingValid    bool
	GroupsAvailable bool
	Revision        int64
	Fault           string
	Generation      string
	PauseRevision   int64
	Revoked         string
}

// JSON projections keep recovery snapshots token-free and bound the number of
// queries independently of the number of historical replica operations.
func proxyRecoveryJSON(ctx context.Context, c *dbent.Client, query string, out any, args ...any) error {
	rows, err := c.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return infraerrors.NotFound("PROXY_RECOVERY_NOT_FOUND", "recovery not found")
	}
	var b []byte
	if err = rows.Scan(&b); err != nil {
		return err
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
func proxyRecoveryLock(ctx context.Context, c *dbent.Client, query string, args ...any) error {
	rows, err := c.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return err
		}
	}
	return rows.Err()
}
func recoveryAllowed(ctx context.Context, permission string, resource adminaccess.Account) bool {
	p := adminaccess.FromContext(ctx)
	return p == nil || (slices.Contains(p.Policy.GlobalPermissions, "proxies.read") && p.Policy.AllowsAccountAndGroups(permission, resource))
}
func recoveryHidden() error {
	return infraerrors.NotFound("PROXY_RECOVERY_NOT_FOUND", "recovery not found")
}

func (r *accountRepository) ProxyRecoveryCredentials(ctx context.Context, id int64) (*service.ProxyRecoveryCredentials, error) {
	// One statement captures the exact tokens and generation together. A later
	// replacement cannot be marked verified by an older in-flight HTTP response.
	var v struct {
		Credentials   map[string]any
		Extra         map[string]any
		Generation    string
		Verified      bool
		ProxyID       *int64
		Status        string
		ExpiresAt     *time.Time
		TempUntil     *time.Time
		RateUntil     *time.Time
		OverloadUntil *time.Time
		Platform      string
		Type          string
	}
	err := proxyRecoveryJSON(ctx, r.client, `SELECT jsonb_build_object('Credentials',credentials,'Extra',extra,
 'Generation',proxy_credential_generation,'Verified',coalesce(proxy_verified_generation=proxy_credential_generation AND proxy_verified_until>NOW(),false),
 'Platform',platform,'Type',type,'ProxyID',proxy_id,'Status',status,'ExpiresAt',expires_at,'TempUntil',temp_unschedulable_until,'RateUntil',rate_limit_reset_at,'OverloadUntil',overload_until)
 FROM accounts WHERE id=$1 AND deleted_at IS NULL AND extra->>'proxy_replica_source'='true'`, &v, id)
	if err != nil {
		return nil, err
	}
	a := &service.Account{ID: id, Platform: v.Platform, Type: v.Type, Credentials: v.Credentials, Extra: v.Extra, ProxyID: v.ProxyID, Status: v.Status, ExpiresAt: v.ExpiresAt, TempUnschedulableUntil: v.TempUntil, RateLimitResetAt: v.RateUntil, OverloadUntil: v.OverloadUntil}
	if v.ProxyID != nil {
		p, err := r.client.Proxy.Get(ctx, *v.ProxyID)
		if err != nil {
			return nil, err
		}
		a.Proxy = proxyEntityToService(p)
	}
	return &service.ProxyRecoveryCredentials{Account: a, Generation: v.Generation, Verified: v.Verified}, nil
}
func (r *accountRepository) VerifyProxyRecoveryGeneration(ctx context.Context, id int64, generation string) error {
	_, err := r.client.ExecContext(ctx, `UPDATE accounts SET proxy_verified_generation=$2,proxy_verified_until=NOW()+INTERVAL '60 seconds'
 WHERE id=$1 AND proxy_credential_generation=$2 AND status='active' AND deleted_at IS NULL
 AND extra->>'proxy_replica_source'='true'`, id, generation)
	return err
}

func (r *accountRepository) ProxyReplicaRecovery(ctx context.Context, sourceID int64, key string, in *service.ProxyRecoveryInput, hash string, resume bool, permission string) (*service.ProxyRecoveryOperation, error) {
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	c := tx.Client()
	scoped := &accountRepository{client: c, sql: c}
	// Use a bounded lock wait. No outbound HTTP is performed in this transaction.
	if _, err = c.ExecContext(ctx, `SET LOCAL lock_timeout='5s'`); err != nil {
		return nil, err
	}
	if err = proxyRecoveryLock(ctx, c, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, sourceID); err != nil {
		return nil, err
	}
	source, err := scoped.GetByID(ctx, sourceID)
	if err != nil {
		if !errors.Is(err, service.ErrAccountNotFound) {
			return nil, err
		}
		// Soft-deleted sources remain auditable, but are never usable. The
		// ordinary repository intentionally hides them from live account reads.
		source = &service.Account{}
		if err = proxyRecoveryJSON(ctx, c, `SELECT jsonb_build_object('ID',id,'Platform',platform,'Type',type,'Status',status,
		'Extra',jsonb_build_object('proxy_replica_source',extra->'proxy_replica_source'),
		'GroupIDs',coalesce((SELECT jsonb_agg(group_id) FROM account_groups WHERE account_id=accounts.id),'[]'))
		FROM accounts WHERE id=$1 AND deleted_at IS NOT NULL`, source, sourceID); err != nil {
			return nil, err
		}
	}
	if !source.IsProxyReplicaSource() || !recoveryAllowed(ctx, permission, adminaccess.Account{ID: sourceID, GroupIDs: source.GroupIDs, Platform: source.Platform, Type: source.Type}) {
		return nil, recoveryHidden()
	}
	var sourceState struct {
		Generation  string
		Fault       string
		Verified    bool
		Deleted     bool
		Unavailable bool
	}
	if err = proxyRecoveryJSON(ctx, c, `SELECT jsonb_build_object('Generation',proxy_credential_generation,'Fault',proxy_auth_fault,
 'Deleted',deleted_at IS NOT NULL,'Unavailable',EXISTS(SELECT 1 FROM account_performance_monitors m WHERE m.account_id=accounts.id AND m.paused),
 'Verified',coalesce(proxy_verified_generation=proxy_credential_generation AND proxy_verified_until>NOW(),false)) FROM accounts WHERE id=$1`, &sourceState, sourceID); err != nil {
		return nil, err
	}
	if in != nil {
		config, _ := json.Marshal(in)
		_, err = c.ExecContext(ctx, `INSERT INTO account_proxy_recoveries(operation_id,source_account_id,config_hash,config,baseline_generation,pauses)
  VALUES($1,$2,$3,$4,$5,'{}') ON CONFLICT(operation_id) DO NOTHING`, key, sourceID, hash, string(config), sourceState.Generation)
		if err != nil {
			return nil, err
		}
	}
	if err = proxyRecoveryLock(ctx, c, `SELECT source_account_id FROM account_proxy_recoveries WHERE operation_id=$1 FOR UPDATE`, key); err != nil {
		return nil, err
	}
	var stored proxyRecoveryStored
	if err = proxyRecoveryJSON(ctx, c, `SELECT jsonb_build_object('Source',source_account_id,'Hash',config_hash,'Config',config,
 'Baseline',baseline_generation,'Pauses',pauses,'Result',result,'Expired',expires_at<=NOW()) FROM account_proxy_recoveries WHERE operation_id=$1`, &stored, key); err != nil {
		return nil, err
	}
	if stored.Source != sourceID || (in != nil && stored.Hash != hash) {
		return nil, infraerrors.Conflict("PROXY_RECOVERY_CONFLICT", "operation ID is bound to another source or configuration")
	}
	ids := make([]int64, 0, len(stored.Config.Replicas))
	proxies := []int64{}
	groups := append([]int64{}, source.GroupIDs...)
	for _, p := range stored.Config.Replicas {
		if !recoveryAllowed(ctx, permission, adminaccess.Account{ID: p.AccountID, GroupIDs: p.GroupIDs, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}) {
			return nil, recoveryHidden()
		}
		ids = append(ids, p.AccountID)
		proxies = append(proxies, p.ProxyID)
		groups = append(groups, p.GroupIDs...)
	}
	slices.Sort(ids)
	if err = proxyRecoveryLock(ctx, c, `SELECT id FROM accounts WHERE id=ANY($1) ORDER BY id FOR UPDATE`, pq.Array(ids)); err != nil {
		return nil, err
	}
	// NOWAIT prevents the proxy->account invalidation trigger and our
	// account->proxy validation from forming an unbounded deadlock.
	if source.ProxyID != nil {
		proxies = append(proxies, *source.ProxyID)
	}
	if err = proxyRecoveryLock(ctx, c, `SELECT id FROM proxies WHERE id=ANY($1) ORDER BY id FOR SHARE NOWAIT`, pq.Array(proxies)); err != nil {
		return nil, err
	}
	if err = proxyRecoveryLock(ctx, c, `SELECT id FROM groups WHERE id=ANY($1) ORDER BY id FOR SHARE NOWAIT`, pq.Array(groups)); err != nil {
		return nil, err
	}
	var sourceLive struct {
		GroupsAvailable bool
		ProxyAvailable  bool
	}
	if err = proxyRecoveryJSON(ctx, c, `SELECT jsonb_build_object('GroupsAvailable',NOT EXISTS(
	 SELECT 1 FROM account_groups ag LEFT JOIN groups g ON g.id=ag.group_id WHERE ag.account_id=a.id AND (g.id IS NULL OR g.deleted_at IS NOT NULL OR g.status<>'active')),
	 'ProxyAvailable',a.proxy_id IS NULL OR EXISTS(SELECT 1 FROM proxies p WHERE p.id=a.proxy_id AND p.deleted_at IS NULL AND p.status='active' AND (p.expires_at IS NULL OR p.expires_at>clock_timestamp())))
	 FROM accounts a WHERE a.id=$1`, &sourceLive, sourceID); err != nil {
		return nil, err
	}
	var states []proxyRecoveryState
	err = proxyRecoveryJSON(ctx, c, `SELECT coalesce(jsonb_agg(v),'[]') FROM (
 SELECT a.id AS "ID",a.extra->>'credential_owner_account_id' AS "Owner",a.extra->>'proxy_replica_operation_id' AS "Operation",
 a.proxy_id AS "ProxyID",a.concurrency AS "Concurrency",coalesce(a.extra->>'openai_oauth_responses_websockets_v2_mode','off') AS "WSMode",
 coalesce((SELECT jsonb_agg(group_id ORDER BY group_id) FROM account_groups WHERE account_id=a.id),'[]') AS "Groups",
 a.platform AS "Platform",a.type AS "Type",a.status AS "Status",a.deleted_at IS NOT NULL AS "Deleted",
 a.expires_at<=clock_timestamp() AS "Expired",a.schedulable AS "Schedulable",a.proxy_recovery_revision AS "Revision",
 EXISTS(SELECT 1 FROM account_proxy_replica_items i JOIN account_proxy_replica_operations o ON o.operation_id=i.operation_id
 WHERE i.account_id=a.id AND i.proxy_id=a.proxy_id AND i.operation_id=a.extra->>'proxy_replica_operation_id' AND o.source_account_id::text=a.extra->>'credential_owner_account_id') AS "BindingValid",
 (coalesce(a.rate_limit_reset_at>clock_timestamp(),false) OR coalesce(a.overload_until>clock_timestamp(),false) OR coalesce(a.temp_unschedulable_until>clock_timestamp(),false)
 OR EXISTS(SELECT 1 FROM account_performance_monitors m WHERE m.account_id=a.id AND m.paused)) AS "Unavailable",
 (p.id IS NOT NULL AND p.deleted_at IS NULL AND p.status='active' AND (p.expires_at IS NULL OR p.expires_at>clock_timestamp())) AS "ProxyAvailable",
 NOT EXISTS(SELECT 1 FROM account_groups ag LEFT JOIN groups g ON g.id=ag.group_id WHERE ag.account_id=a.id
  AND (g.id IS NULL OR g.deleted_at IS NOT NULL OR g.status<>'active')) AS "GroupsAvailable",
 proof.fault AS "Fault",proof.credential_generation AS "Generation",proof.revision AS "PauseRevision",proof.revoked_reason AS "Revoked"
 FROM accounts a LEFT JOIN proxies p ON p.id=a.proxy_id LEFT JOIN account_proxy_recovery_pauses proof ON proof.account_id=a.id
 WHERE a.id=ANY($1)) v`, &states, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	byID := map[int64]proxyRecoveryState{}
	for _, a := range states {
		if !recoveryAllowed(ctx, permission, adminaccess.Account{ID: a.ID, GroupIDs: a.Groups, Platform: a.Platform, Type: a.Type}) {
			return nil, recoveryHidden()
		}
		byID[a.ID] = a
	}
	if stored.Result != nil {
		if resume && stored.Expired {
			return nil, infraerrors.Conflict("PROXY_RECOVERY_EXPIRED", "recovery expired")
		}
		return stored.Result, nil
	}
	if resume && stored.Expired {
		return nil, infraerrors.Conflict("PROXY_RECOVERY_EXPIRED", "recovery expired")
	}
	first := len(stored.Pauses) == 0
	if first {
		stored.Pauses = map[int64]proxyRecoveryPause{}
		for _, p := range stored.Config.Replicas {
			a, ok := byID[p.AccountID]
			if !ok || !a.BindingValid || a.Owner != sourceIDString(sourceID) || a.Operation != p.ProxyOperationID || a.ProxyID != p.ProxyID || a.Concurrency != p.Concurrency || a.WSMode != p.WSMode || !slices.Equal(a.Groups, p.GroupIDs) {
				return nil, infraerrors.Conflict("PROXY_RECOVERY_BINDING", "replica does not match its source and original operation")
			}
			owned := a.Fault != "" && a.Fault == sourceState.Fault && a.Generation == stored.Baseline && a.Revoked == "" && a.Revision == a.PauseRevision && !a.Schedulable
			stored.Pauses[p.AccountID] = proxyRecoveryPause{Fault: a.Fault, Generation: a.Generation, Revision: a.Revision, Owned: owned}
		}
		b, _ := json.Marshal(stored.Pauses)
		if _, err = c.ExecContext(ctx, `UPDATE account_proxy_recoveries SET pauses=$2 WHERE operation_id=$1`, key, string(b)); err != nil {
			return nil, err
		}
	}
	sourceUsable := !sourceState.Deleted && !sourceState.Unavailable && sourceLive.GroupsAvailable && sourceLive.ProxyAvailable && service.ProxyReplicaOwnerUsable(source, time.Now())
	if source.ProxyID != nil && (source.Proxy == nil || !source.Proxy.IsActive() || source.Proxy.IsExpired(time.Now())) {
		sourceUsable = false
	}
	for _, g := range source.Groups {
		if g == nil || g.Status != service.StatusActive {
			sourceUsable = false
		}
	}
	recovered := sourceState.Generation != stored.Baseline && sourceState.Verified && sourceUsable
	result := &service.ProxyRecoveryOperation{BaselineGeneration: stored.Baseline, Version: 1, OperationID: key, SourceAccountID: sourceID, Status: "waiting", CredentialGeneration: sourceState.Generation, SourceAuthRecovered: recovered, Replicas: []service.ProxyRecoveryResult{}}
	if in != nil {
		result.Status = "prepared"
	}
	eligible := []int64{}
	for _, p := range stored.Config.Replicas {
		a, exists := byID[p.AccountID]
		proof := stored.Pauses[p.AccountID]
		status, reason := "skipped", "no_owned_pause"
		switch {
		case !exists || a.Deleted:
			reason = "deleted"
		case !a.BindingValid || a.Owner != sourceIDString(sourceID) || a.Operation != p.ProxyOperationID || a.ProxyID != p.ProxyID || a.Concurrency != p.Concurrency || a.WSMode != p.WSMode || !slices.Equal(a.Groups, p.GroupIDs):
			reason = "config_changed"
		case a.Status != service.StatusActive || a.Expired:
			reason = "disabled"
		case !a.ProxyAvailable:
			reason = "proxy_unavailable"
		case !a.GroupsAvailable || a.Unavailable:
			reason = "config_changed"
		case a.Revoked != "":
			reason = a.Revoked
		case a.Schedulable:
			status = "already_active"
			reason = "already_active"
		case !proof.Owned || proof.Fault != sourceState.Fault || a.Fault != proof.Fault || a.Generation != proof.Generation:
			reason = "no_owned_pause"
		case a.Revision != proof.Revision || a.PauseRevision != proof.Revision:
			reason = "config_changed"
		case !sourceUsable && service.AccountAuthInvalidReason(source) == "":
			reason = "source_unavailable"
		default:
			status = "eligible"
			reason = "auth_failure"
		}
		if resume && recovered && status == "eligible" {
			eligible = append(eligible, p.AccountID)
			status = "restored"
			reason = "restored"
			a.Schedulable = true
		}
		result.Replicas = append(result.Replicas, service.ProxyRecoveryResult{ProxyRecoveryReplica: p, Schedulable: a.Schedulable, RecoveryStatus: status, Reason: reason})
	}
	if resume && recovered {
		if len(eligible) > 0 {
			if _, err = c.ExecContext(ctx, `SELECT set_config('sub2api.proxy_recovery_internal',$1,true)`, "resume:"+sourceIDString(sourceID)); err != nil {
				return nil, err
			}
			rows, e := c.QueryContext(ctx, `UPDATE accounts a SET schedulable=true,updated_at=NOW() FROM proxies p,accounts owner
			 WHERE a.id=ANY($1) AND p.id=a.proxy_id AND owner.id=$2 AND owner.status='active' AND owner.deleted_at IS NULL
			 AND (owner.expires_at IS NULL OR owner.expires_at>clock_timestamp()) AND owner.proxy_verified_until>clock_timestamp()
			 AND a.status='active' AND a.deleted_at IS NULL AND (a.expires_at IS NULL OR a.expires_at>clock_timestamp())
			 AND p.status='active' AND p.deleted_at IS NULL AND (p.expires_at IS NULL OR p.expires_at>clock_timestamp()) RETURNING a.id`, pq.Array(eligible), sourceID)
			if e != nil {
				return nil, e
			}
			updated := 0
			for rows.Next() {
				var id int64
				if e = rows.Scan(&id); e != nil {
					break
				}
				updated++
			}
			rowErr := rows.Err()
			rows.Close()
			if e != nil {
				return nil, e
			}
			if rowErr != nil {
				return nil, rowErr
			}
			if updated != len(eligible) {
				return nil, infraerrors.Conflict("PROXY_RECOVERY_STATE_CHANGED", "recovery state changed before activation")
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO scheduler_outbox(event_type,account_id) SELECT 'account_changed',unnest($1::bigint[])`, pq.Array(eligible)); err != nil {
				return nil, err
			}
			if _, err = c.ExecContext(ctx, `DELETE FROM account_proxy_recovery_pauses WHERE account_id=ANY($1)`, pq.Array(eligible)); err != nil {
				return nil, err
			}
		}
		result.Status = "completed"
		b, _ := json.Marshal(result)
		if _, err = c.ExecContext(ctx, `UPDATE account_proxy_recoveries SET result=$2 WHERE operation_id=$1`, key, string(b)); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

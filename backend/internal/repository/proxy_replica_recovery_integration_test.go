//go:build integration

package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type recoveryFixture struct {
	repo          *accountRepository
	client        *dbent.Client
	source, group int64
	input         service.ProxyRecoveryInput
	key, hash     string
}

func newRecoveryFixture(t *testing.T, n int) *recoveryFixture {
	t.Helper()
	ctx := context.Background()
	c := testEntClient(t)
	r := newAccountRepositoryWithSQL(c, integrationDB, nil)
	suffix := time.Now().UnixNano()
	source, err := c.Account.Create().SetName(fmt.Sprintf("recovery-%d", suffix)).SetPlatform("openai").SetType("oauth").SetCredentials(map[string]any{"access_token": "old", "refresh_token": "old-refresh"}).Save(ctx)
	require.NoError(t, err)
	g, err := c.Group.Create().SetName(fmt.Sprintf("recovery-group-%d", suffix)).SetPlatform("openai").Save(ctx)
	require.NoError(t, err)
	_, err = c.AccountGroup.Create().SetAccountID(source.ID).SetGroupID(g.ID).Save(ctx)
	require.NoError(t, err)
	config := service.ProxyReplicasInput{WSMode: "ctx_pool", ActivateOnSuccess: true}
	for i := 0; i < n; i++ {
		p, err := c.Proxy.Create().SetName("recovery-proxy").SetProtocol("http").SetHost("127.0.0.1").SetPort(14000 + i).SetStatus("active").Save(ctx)
		require.NoError(t, err)
		config.Proxies = append(config.Proxies, service.ProxyReplicaInput{ProxyID: p.ID, Concurrency: i + 3})
	}
	config, hash, err := service.NormalizeProxyReplicasInput(config)
	require.NoError(t, err)
	originalKey := fmt.Sprintf("recovery-replicas-%d", suffix)
	op, err := r.CreateProxyReplicas(ctx, source.ID, originalKey, config, hash)
	require.NoError(t, err)
	in := service.ProxyRecoveryInput{Version: 1}
	for _, p := range op.Replicas {
		in.Replicas = append(in.Replicas, service.ProxyRecoveryReplica{AccountID: p.AccountID, ProxyOperationID: originalKey, ProxyID: p.ProxyID, Concurrency: p.Concurrency, WSMode: p.WSMode, GroupIDs: p.GroupIDs})
	}
	in, hash, err = service.NormalizeProxyRecoveryInput(in)
	require.NoError(t, err)
	return &recoveryFixture{repo: r, client: c, source: source.ID, group: g.ID, input: in, key: fmt.Sprintf("recovery-op-%d", suffix), hash: hash}
}
func (f *recoveryFixture) fail(t *testing.T) {
	t.Helper()
	require.NoError(t, f.repo.SetError(context.Background(), f.source, "Unauthorized (401): synthetic auth failure"))
}
func (f *recoveryFixture) prepare(t *testing.T) *service.ProxyRecoveryOperation {
	t.Helper()
	op, err := f.repo.ProxyReplicaRecovery(context.Background(), f.source, f.key, &f.input, f.hash, false, service.ProxyRecoveryWritePermission)
	require.NoError(t, err)
	return op
}
func (f *recoveryFixture) login(t *testing.T, verify bool) {
	t.Helper()
	ctx := context.Background()
	_, err := f.client.ExecContext(ctx, `UPDATE accounts SET credentials=credentials||'{"access_token":"fresh","refresh_token":"fresh-refresh"}',status='active',error_message='' WHERE id=$1`, f.source)
	require.NoError(t, err)
	if verify {
		v, err := f.repo.ProxyRecoveryCredentials(ctx, f.source)
		require.NoError(t, err)
		require.NoError(t, f.repo.VerifyProxyRecoveryGeneration(ctx, f.source, v.Generation))
	}
}
func (f *recoveryFixture) resume(t *testing.T) *service.ProxyRecoveryOperation {
	t.Helper()
	op, err := f.repo.ProxyReplicaRecovery(context.Background(), f.source, f.key, nil, "", true, service.ProxyRecoveryWritePermission)
	require.NoError(t, err)
	return op
}

func TestProxyRecoveryLifecycleAndDurableReplay(t *testing.T) {
	ctx := context.Background()
	f := newRecoveryFixture(t, 2)
	require.NoError(t, f.repo.SetSchedulable(ctx, f.input.Replicas[0].AccountID, false))
	f.fail(t)
	prepared := f.prepare(t)
	require.Equal(t, "prepared", prepared.Status)
	require.Equal(t, "skipped", prepared.Replicas[0].RecoveryStatus)
	require.Equal(t, "eligible", prepared.Replicas[1].RecoveryStatus)
	// Status-only recovery is not proof; neither are unverified fresh credentials.
	_, err := f.client.ExecContext(ctx, `UPDATE accounts SET status='active',error_message='' WHERE id=$1`, f.source)
	require.NoError(t, err)
	require.False(t, f.resume(t).SourceAuthRecovered)
	f.login(t, false)
	waiting := f.resume(t)
	require.Equal(t, "waiting", waiting.Status)
	require.False(t, waiting.SourceAuthRecovered)
	state, err := f.repo.ProxyRecoveryCredentials(ctx, f.source)
	require.NoError(t, err)
	require.NotEqual(t, prepared.CredentialGeneration, state.Generation)
	require.NoError(t, f.repo.VerifyProxyRecoveryGeneration(ctx, f.source, prepared.CredentialGeneration))
	state, err = f.repo.ProxyRecoveryCredentials(ctx, f.source)
	require.NoError(t, err)
	require.False(t, state.Verified, "stale verification must not bless replacement credentials")
	require.NoError(t, f.repo.VerifyProxyRecoveryGeneration(ctx, f.source, state.Generation))
	var wg sync.WaitGroup
	results := make(chan *service.ProxyRecoveryOperation, 6)
	errs := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, e := f.repo.ProxyReplicaRecovery(ctx, f.source, f.key, nil, "", true, service.ProxyRecoveryWritePermission)
			results <- op
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	for op := range results {
		require.Equal(t, "completed", op.Status)
		require.True(t, op.SourceAuthRecovered)
		require.Equal(t, "restored", op.Replicas[1].RecoveryStatus)
		require.True(t, op.Replicas[1].Schedulable)
		require.Equal(t, "skipped", op.Replicas[0].RecoveryStatus)
	}
	owner, err := f.repo.GetByID(ctx, f.source)
	require.NoError(t, err)
	require.False(t, owner.Schedulable)
	child, err := f.repo.GetByID(ctx, f.input.Replicas[1].AccountID)
	require.NoError(t, err)
	require.Empty(t, child.GetCredential("access_token"))
	require.True(t, child.Schedulable)
	require.NoError(t, f.repo.SetSchedulable(ctx, child.ID, false))
	require.Equal(t, "completed", f.resume(t).Status)
	child, err = f.repo.GetByID(ctx, child.ID)
	require.NoError(t, err)
	require.False(t, child.Schedulable)
	_, err = f.client.ExecContext(ctx, `UPDATE account_proxy_recoveries SET expires_at=NOW()-INTERVAL '1 second' WHERE operation_id=$1`, f.key)
	require.NoError(t, err)
	_, err = f.repo.ProxyReplicaRecovery(ctx, f.source, f.key, nil, "", true, service.ProxyRecoveryWritePermission)
	require.Equal(t, 409, infraerrors.Code(err))
	historical, err := f.repo.ProxyReplicaRecovery(ctx, f.source, f.key, nil, "", false, service.ProxyRecoveryReadPermission)
	require.NoError(t, err)
	require.Equal(t, "completed", historical.Status)
}

func TestProxyRecoveryManualAndConfigurationOverrides(t *testing.T) {
	for _, change := range []string{"same_false", "on_then_off", "timer", "disabled", "deleted", "proxy_disabled", "proxy_expired", "proxy_config", "concurrency", "ws", "groups", "group_status", "source_expired", "source_deleted", "no_proof", "performance"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			f := newRecoveryFixture(t, 1)
			f.fail(t)
			f.prepare(t)
			f.login(t, true)
			p := f.input.Replicas[0]
			var err error
			switch change {
			case "same_false":
				err = f.repo.SetSchedulable(ctx, p.AccountID, false)
			case "on_then_off":
				require.NoError(t, f.repo.SetSchedulable(ctx, p.AccountID, true))
				err = f.repo.SetSchedulable(ctx, p.AccountID, false)
			case "timer":
				_, err = f.client.ExecContext(ctx, `UPDATE accounts SET schedulable=false WHERE id=$1`, p.AccountID)
			case "disabled":
				_, err = f.client.ExecContext(ctx, `UPDATE accounts SET status='disabled' WHERE id=$1`, p.AccountID)
			case "deleted":
				err = f.repo.Delete(ctx, p.AccountID)
			case "proxy_disabled":
				_, err = f.client.ExecContext(ctx, `UPDATE proxies SET status='disabled' WHERE id=$1`, p.ProxyID)
			case "proxy_expired":
				_, err = f.client.ExecContext(ctx, `UPDATE proxies SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1`, p.ProxyID)
			case "proxy_config":
				_, err = f.client.ExecContext(ctx, `UPDATE proxies SET host='127.0.0.2' WHERE id=$1`, p.ProxyID)
			case "concurrency":
				_, err = f.client.ExecContext(ctx, `UPDATE accounts SET concurrency=concurrency+1 WHERE id=$1`, p.AccountID)
			case "ws":
				_, err = f.client.ExecContext(ctx, `UPDATE accounts SET extra=extra||'{"openai_oauth_responses_websockets_v2_mode":"off"}' WHERE id=$1`, p.AccountID)
			case "groups":
				_, err = f.client.ExecContext(ctx, `DELETE FROM account_groups WHERE account_id=$1`, p.AccountID)
			case "group_status":
				_, err = f.client.ExecContext(ctx, `UPDATE groups SET status='disabled' WHERE id=$1`, f.group)
			case "source_expired":
				_, err = f.client.ExecContext(ctx, `UPDATE accounts SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1`, f.source)
			case "source_deleted":
				err = f.repo.Delete(ctx, f.source)
			case "no_proof":
				_, err = f.client.ExecContext(ctx, `DELETE FROM account_proxy_recovery_pauses WHERE account_id=$1`, p.AccountID)
			case "performance":
				_, err = f.client.ExecContext(ctx, `INSERT INTO account_performance_monitors(account_id,paused) VALUES($1,true) ON CONFLICT(account_id) DO UPDATE SET paused=true`, p.AccountID)
			}
			require.NoError(t, err)
			op := f.resume(t)
			require.NotEqual(t, "restored", op.Replicas[0].RecoveryStatus)
			var scheduled bool
			require.NoError(t, integrationDB.QueryRow(`SELECT schedulable FROM accounts WHERE id=$1`, p.AccountID).Scan(&scheduled))
			require.False(t, scheduled)
		})
	}
}
func TestProxyRecoveryConcurrentManualPauseAndScope(t *testing.T) {
	ctx := context.Background()
	f := newRecoveryFixture(t, 1)
	f.fail(t)
	f.prepare(t)
	f.login(t, true)
	id := f.input.Replicas[0].AccountID
	manual, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer manual.Rollback()
	_, err = manual.ExecContext(ctx, `UPDATE accounts SET schedulable=false WHERE id=$1`, id)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, e := f.repo.ProxyReplicaRecovery(ctx, f.source, f.key, nil, "", true, service.ProxyRecoveryWritePermission)
		done <- e
	}()
	require.NoError(t, manual.Commit())
	require.NoError(t, <-done)
	a, err := f.repo.GetByID(ctx, id)
	require.NoError(t, err)
	require.False(t, a.Schedulable)
	principal := &adminaccess.Principal{Policy: adminaccess.Policy{GlobalPermissions: []string{"proxies.read"}, Rules: []adminaccess.Rule{{Permissions: []string{service.ProxyRecoveryReadPermission}, GroupIDs: []int64{f.group}, AccountTypes: []string{"oauth"}, Platforms: []string{"openai"}}}}}
	scoped := adminaccess.WithPrincipal(ctx, principal)
	_, err = f.repo.ProxyReplicaRecovery(scoped, f.source, f.key, nil, "", false, service.ProxyRecoveryReadPermission)
	require.NoError(t, err)
	_, err = f.repo.ProxyReplicaRecovery(scoped, f.source, f.key, nil, "", true, service.ProxyRecoveryWritePermission)
	require.Equal(t, 404, infraerrors.Code(err))
	hidden, err := f.client.Group.Create().SetName(fmt.Sprintf("hidden-recovery-%d", time.Now().UnixNano())).SetPlatform("openai").Save(ctx)
	require.NoError(t, err)
	_, err = f.client.AccountGroup.Create().SetAccountID(id).SetGroupID(hidden.ID).Save(ctx)
	require.NoError(t, err)
	_, err = f.repo.ProxyReplicaRecovery(scoped, f.source, f.key, nil, "", false, service.ProxyRecoveryReadPermission)
	require.Equal(t, 404, infraerrors.Code(err))
	other := newRecoveryFixture(t, 1)
	_, err = f.repo.ProxyReplicaRecovery(ctx, other.source, f.key, &other.input, other.hash, false, service.ProxyRecoveryWritePermission)
	require.Equal(t, 409, infraerrors.Code(err))
	_, err = f.repo.ProxyReplicaRecovery(ctx, f.source, f.key, &f.input, "changed", false, service.ProxyRecoveryWritePermission)
	require.Equal(t, 409, infraerrors.Code(err))
}

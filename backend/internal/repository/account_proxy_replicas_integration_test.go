//go:build integration

package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestProxyReplicaOperationIntegration(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := newAccountRepositoryWithSQL(client, integrationDB, nil)
	suffix := time.Now().UnixNano()
	source, err := client.Account.Create().SetName(fmt.Sprintf("replica-owner-%d", suffix)).SetPlatform("openai").SetType("oauth").SetStatus("active").SetSchedulable(true).SetCredentials(map[string]any{"access_token": "synthetic-access", "refresh_token": "synthetic-refresh", "model_mapping": map[string]any{"gpt-test": "gpt-test"}}).Save(ctx)
	require.NoError(t, err)
	group, err := client.Group.Create().SetName(fmt.Sprintf("replica-group-%d", suffix)).SetPlatform("openai").Save(ctx)
	require.NoError(t, err)
	_, err = client.AccountGroup.Create().SetAccountID(source.ID).SetGroupID(group.ID).SetPriority(13).SetAllowedModels([]string{"gpt-test"}).Save(ctx)
	require.NoError(t, err)
	p1, err := client.Proxy.Create().SetName("replica-proxy-1").SetProtocol("http").SetHost("127.0.0.1").SetPort(10101).SetStatus("active").Save(ctx)
	require.NoError(t, err)
	p2, err := client.Proxy.Create().SetName("replica-proxy-2").SetProtocol("http").SetHost("127.0.0.1").SetPort(10102).SetStatus("active").Save(ctx)
	require.NoError(t, err)
	key := fmt.Sprintf("harbor-%d", suffix)
	in, hash, err := service.NormalizeProxyReplicasInput(service.ProxyReplicasInput{Proxies: []service.ProxyReplicaInput{{ProxyID: p1.ID, Concurrency: 5}, {ProxyID: p2.ID, Concurrency: 8}}, WSMode: "ctx_pool", ActivateOnSuccess: true})
	require.NoError(t, err)
	var wg sync.WaitGroup
	results := make(chan *service.ProxyReplicaOperation, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := repo.CreateProxyReplicas(ctx, source.ID, key, in, hash)
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	var canonical *service.ProxyReplicaOperation
	for result := range results {
		require.Len(t, result.Replicas, 2)
		require.Equal(t, "active", result.Status)
		if canonical == nil {
			canonical = result
		} else {
			require.Equal(t, canonical.Replicas[0].AccountID, result.Replicas[0].AccountID)
			require.Equal(t, canonical.Replicas[1].AccountID, result.Replicas[1].AccountID)
		}
	}
	current, err := repo.GetByID(ctx, source.ID)
	require.NoError(t, err)
	require.False(t, current.Schedulable)
	require.True(t, current.IsProxyReplicaSource())
	for _, replica := range canonical.Replicas {
		a, err := repo.GetByID(ctx, replica.AccountID)
		require.NoError(t, err)
		require.True(t, a.Schedulable)
		require.Nil(t, a.ParentAccountID)
		require.Equal(t, source.ID, a.CredentialOwnerAccountID())
		require.Empty(t, a.GetCredential("access_token"))
		require.Empty(t, a.GetCredential("refresh_token"))
		require.Len(t, a.AccountGroups, 1)
		require.Equal(t, 13, a.AccountGroups[0].Priority)
		require.Equal(t, []string{"gpt-test"}, a.AccountGroups[0].AllowedModels)
	}
	_, err = repo.CreateProxyReplicas(ctx, source.ID, key, in, "different-hash")
	require.Equal(t, 409, infraerrors.Code(err))
	require.NoError(t, repo.SetSchedulable(ctx, canonical.Replicas[0].AccountID, false))
	replayed, err := repo.CreateProxyReplicas(ctx, source.ID, key, in, hash)
	require.NoError(t, err)
	require.False(t, replayed.Replicas[0].Schedulable)
	require.Equal(t, "paused", replayed.Status)
	// Even a bulk recovery cannot turn the source into a direct gateway entry.
	require.NoError(t, repo.SetSchedulable(ctx, source.ID, true))
	current, err = repo.GetByID(ctx, source.ID)
	require.NoError(t, err)
	require.False(t, current.Schedulable)
	// Proxy failure pauses only its replica and never rewrites its binding to
	// the direct/backup egress, even if the proxy permits fallback elsewhere.
	_, err = client.Proxy.UpdateOneID(p2.ID).SetFallbackMode("direct").SetStatus("disabled").Save(ctx)
	require.NoError(t, err)
	bound, err := repo.GetByID(ctx, canonical.Replicas[1].AccountID)
	require.NoError(t, err)
	require.False(t, bound.Schedulable)
	require.Equal(t, p2.ID, *bound.ProxyID)
	_, err = client.Proxy.UpdateOneID(p2.ID).SetStatus("active").Save(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.SetSchedulable(ctx, bound.ID, true))
	// Expiry must fence the children even if a raw maintenance writer disables
	// the legacy source auto-pause flag in the same update.
	_, err = client.ExecContext(ctx, `UPDATE accounts SET expires_at=NOW()-INTERVAL '1 second',auto_pause_on_expired=false WHERE id=$1`, source.ID)
	require.NoError(t, err)
	bound, err = repo.GetByID(ctx, bound.ID)
	require.NoError(t, err)
	require.False(t, bound.Schedulable)
	_, err = client.Account.UpdateOneID(source.ID).ClearExpiresAt().Save(ctx)
	require.NoError(t, err)
	require.NoError(t, repo.SetSchedulable(ctx, bound.ID, true))
	require.NoError(t, repo.Delete(ctx, source.ID))
	for _, item := range canonical.Replicas {
		a, err := repo.GetByID(ctx, item.AccountID)
		require.NoError(t, err)
		require.False(t, a.Schedulable)
		_, err = service.ResolveProxyReplicaOwner(ctx, repo, a)
		require.Error(t, err)
		_, _, err = repo.GetOpenAITurnAdmission(ctx, item.AccountID)
		require.Error(t, err)
	}
	// Audit records survive deletion and queries do not recreate missing rows.
	queried, err := repo.GetProxyReplicaOperation(ctx, source.ID, key)
	require.NoError(t, err)
	require.Len(t, queried.Replicas, 2)
	count, err := client.Account.Query().Where(dbaccount.IDIn(canonical.Replicas[0].AccountID, canonical.Replicas[1].AccountID)).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestProxyReplicaFailedProxyCanResumeIntegration(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	repo := newAccountRepositoryWithSQL(client, integrationDB, nil)
	source, err := client.Account.Create().SetName("replica-recovery-owner").SetPlatform("openai").SetType("oauth").SetStatus("active").SetCredentials(map[string]any{"access_token": "synthetic-access", "refresh_token": "synthetic-refresh"}).Save(ctx)
	require.NoError(t, err)
	proxy, err := client.Proxy.Create().SetName("replica-disabled").SetProtocol("http").SetHost("127.0.0.1").SetPort(10103).SetStatus("disabled").Save(ctx)
	require.NoError(t, err)
	in, hash, err := service.NormalizeProxyReplicasInput(service.ProxyReplicasInput{Proxies: []service.ProxyReplicaInput{{ProxyID: proxy.ID, Concurrency: 5}}, WSMode: "ctx_pool", ActivateOnSuccess: true})
	require.NoError(t, err)
	key := fmt.Sprintf("harbor-recovery-%d", source.ID)
	failed, err := repo.CreateProxyReplicas(ctx, source.ID, key, in, hash)
	require.NoError(t, err)
	require.Equal(t, "failed", failed.Status)
	query, err := repo.GetProxyReplicaOperation(ctx, source.ID, key)
	require.NoError(t, err)
	require.Empty(t, query.Replicas)
	require.NotEmpty(t, query.Error)
	_, err = client.Proxy.UpdateOneID(proxy.ID).SetStatus("active").Save(ctx)
	require.NoError(t, err)
	result, err := repo.CreateProxyReplicas(ctx, source.ID, key, in, hash)
	require.NoError(t, err)
	require.Len(t, result.Replicas, 1)
	require.True(t, result.Replicas[0].Schedulable)
}

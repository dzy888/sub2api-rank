package repository

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestProxyReplicaSchedulerProjectionPreservesOwner(t *testing.T) {
	replica := service.Account{ID: 456, Platform: "openai", Type: "oauth", Extra: map[string]any{service.CredentialOwnerAccountIDKey: int64(123), service.ProxyReplicaOperationKey: "harbor-test"}}
	projected := buildSchedulerMetadataAccount(replica)
	require.True(t, projected.IsProxyReplica())
	require.Equal(t, int64(123), projected.CredentialOwnerAccountID())
	require.Nil(t, projected.ParentAccountID)
	source := buildSchedulerMetadataAccount(service.Account{ID: 123, Status: "active", Schedulable: true, Extra: map[string]any{service.ProxyReplicaSourceKey: true}})
	require.True(t, source.IsProxyReplicaSource())
	require.False(t, source.IsSchedulable())
}

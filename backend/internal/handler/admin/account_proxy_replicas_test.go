package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type proxyReplicaAdminStub struct {
	service.AdminService
	calls int
	key   string
}

func (s *proxyReplicaAdminStub) CreateProxyReplicas(_ context.Context, id int64, key string, in service.ProxyReplicasInput) (*service.ProxyReplicaOperation, error) {
	s.calls++
	s.key = key
	return &service.ProxyReplicaOperation{OperationID: key, SourceAccountID: id, Status: "active", Replicas: []service.ProxyReplicaResult{{AccountID: 456, CredentialOwnerAccountID: id, ProxyID: in.Proxies[0].ProxyID, Schedulable: true}}}, nil
}
func (s *proxyReplicaAdminStub) GetProxyReplicaOperation(_ context.Context, id int64, key string) (*service.ProxyReplicaOperation, error) {
	s.calls++
	s.key = key
	return &service.ProxyReplicaOperation{OperationID: key, SourceAccountID: id, Status: "paused"}, nil
}

func TestProxyReplicaAdminAuthorityAndResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, role := range []string{"", service.RoleUser, service.RoleObserver, service.RoleAdmin} {
		t.Run(role, func(t *testing.T) {
			svc := &proxyReplicaAdminStub{}
			handler := &AccountHandler{adminService: svc}
			router := gin.New()
			router.Use(func(c *gin.Context) { c.Set(string(middleware.ContextKeyUserRole), role); c.Next() })
			router.POST("/accounts/:id/proxy-replicas", handler.CreateProxyReplicas)
			router.GET("/accounts/:id/proxy-replicas/:operation_id", handler.GetProxyReplicaOperation)
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				url := "/accounts/123/proxy-replicas"
				if method == http.MethodGet {
					url += "/harbor-test"
				}
				req := httptest.NewRequest(method, url, strings.NewReader(`{"proxies":[{"proxy_id":11,"concurrency":5}],"ws_mode":"ctx_pool","activate_on_success":true}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", "harbor-test")
				out := httptest.NewRecorder()
				router.ServeHTTP(out, req)
				if role == service.RoleAdmin {
					require.Equal(t, 200, out.Code)
					require.Equal(t, "harbor-test", svc.key)
					require.Contains(t, out.Body.String(), `"source_account_id":123`)
				} else {
					require.Equal(t, 403, out.Code)
					require.Zero(t, svc.calls)
				}
				require.NotContains(t, out.Body.String(), "access_token")
				require.NotContains(t, out.Body.String(), "refresh_token")
			}
		})
	}
}

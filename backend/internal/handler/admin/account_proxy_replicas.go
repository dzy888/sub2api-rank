package admin

import (
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/adminaccess"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *AccountHandler) proxyReplicaAuthority(c *gin.Context) (service.ProxyReplicaAdminService, int64, bool) {
	role, _ := middleware.GetUserRoleFromContext(c)
	_, observer := service.ObserverGroupIDs(c.Request.Context())
	if role != service.RoleAdmin || observer || adminaccess.FromContext(c.Request.Context()) != nil {
		response.Forbidden(c, "Full administrator authority required")
		return nil, 0, false
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "Invalid source account ID")
		return nil, 0, false
	}
	svc, ok := h.adminService.(service.ProxyReplicaAdminService)
	if !ok {
		response.Error(c, 503, "Proxy replicas unavailable")
		return nil, 0, false
	}
	return svc, id, true
}

func (h *AccountHandler) CreateProxyReplicas(c *gin.Context) {
	svc, id, ok := h.proxyReplicaAuthority(c)
	if !ok {
		return
	}
	var in service.ProxyReplicasInput
	if err := c.ShouldBindJSON(&in); err != nil {
		response.BadRequest(c, "Invalid proxy replica configuration")
		return
	}
	result, err := svc.CreateProxyReplicas(c.Request.Context(), id, c.GetHeader("Idempotency-Key"), in)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *AccountHandler) GetProxyReplicaOperation(c *gin.Context) {
	svc, id, ok := h.proxyReplicaAuthority(c)
	if !ok {
		return
	}
	result, err := svc.GetProxyReplicaOperation(c.Request.Context(), id, c.Param("operation_id"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

package admin

import (
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *AccountHandler) proxyReplicaAuthority(c *gin.Context) (service.ProxyReplicaAdminService, int64, bool) {
	role, _ := middleware.GetUserRoleFromContext(c)
	_, observer := service.ObserverGroupIDs(c.Request.Context())
	if observer || role != service.RoleAdmin {
		response.Forbidden(c, "Administrator authority required")
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

func (h *AccountHandler) proxyRecoveryAuthority(c *gin.Context) (service.ProxyRecoveryAdminService, int64, bool) {
	_, id, ok := h.proxyReplicaAuthority(c)
	if !ok {
		return nil, 0, false
	}
	svc, ok := h.adminService.(service.ProxyRecoveryAdminService)
	if !ok {
		response.Error(c, 503, "Proxy recovery unavailable")
		return nil, 0, false
	}
	c.Header("Cache-Control", "no-store")
	return svc, id, true
}
func (h *AccountHandler) ProxyRecoveryCapabilities(c *gin.Context) {
	svc, id, ok := h.proxyRecoveryAuthority(c)
	if !ok {
		return
	}
	if err := svc.ProxyRecoveryCapabilities(c.Request.Context(), id); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"version": 1, "pause_ownership": true, "atomic_manual_override": true, "credential_generation": true, "durable_idempotency": true})
}
func (h *AccountHandler) PrepareProxyRecovery(c *gin.Context) {
	svc, id, ok := h.proxyRecoveryAuthority(c)
	if !ok {
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<20)
	var in service.ProxyRecoveryInput
	if c.ShouldBindJSON(&in) != nil {
		response.BadRequest(c, "Invalid recovery configuration")
		return
	}
	result, err := svc.PrepareProxyRecovery(c.Request.Context(), id, c.GetHeader("Idempotency-Key"), in)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}
func (h *AccountHandler) GetProxyRecovery(c *gin.Context) {
	svc, id, ok := h.proxyRecoveryAuthority(c)
	if !ok {
		return
	}
	result, err := svc.GetProxyRecovery(c.Request.Context(), id, c.Param("operation_id"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}
func (h *AccountHandler) ResumeProxyRecovery(c *gin.Context) {
	svc, id, ok := h.proxyRecoveryAuthority(c)
	if !ok {
		return
	}
	if c.GetHeader("Idempotency-Key") != c.Param("operation_id") {
		response.BadRequest(c, "Idempotency-Key must match operation ID")
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1024)
	var in struct {
		Version int `json:"version"`
	}
	if c.ShouldBindJSON(&in) != nil || in.Version != 1 {
		response.BadRequest(c, "Recovery version 1 required")
		return
	}
	result, err := svc.ResumeProxyRecovery(c.Request.Context(), id, c.Param("operation_id"))
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
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

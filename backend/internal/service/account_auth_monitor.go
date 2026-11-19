package service

import (
	"encoding/json"
	"strings"
)

// AccountAuthInvalidReason exposes only a stable, credential-free reason.
// In particular, a provider message containing an auth keyword is insufficient
// evidence to trigger a paid or destructive re-login workflow.
func AccountAuthInvalidReason(account *Account) string {
	if accountHasRecordedAuthFailure(account) {
		return "requires_relogin"
	}
	return ""
}

// Keep the monitoring contract independent of the removed legacy token guard.
// Only persisted, explicit authentication evidence can trigger recovery.
func accountHasRecordedAuthFailure(account *Account) bool {
	if account == nil || account.Platform != PlatformOpenAI || !account.IsOAuth() || account.Status != StatusError {
		return false
	}
	if strings.HasPrefix(account.ErrorMessage, "Token revoked (401):") ||
		strings.HasPrefix(account.ErrorMessage, "Unauthorized (401):") {
		return true
	}
	raw, ok := strings.CutPrefix(account.ErrorMessage, "Authentication failed (401):")
	if !ok {
		return false
	}
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal([]byte(raw), &result) != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(result.Error.Code)) {
	case "auth_failed", "invalid_token", "token_invalid", "requires_relogin", "invalid_grant", "revoked",
		"token_invalidated", "token_revoked", "refresh_token_reused", "refresh_token_invalidated":
		return true
	default:
		return false
	}
}

package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountAuthInvalidReasonRequiresRecordedAuthEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		want          string
	}{
		{"revoked", "Token revoked (401): synthetic detail", "requires_relogin"},
		{"unauthorized", "Unauthorized (401): synthetic detail", "requires_relogin"},
		{"structured expired token", `Authentication failed (401): {"error":{"code":"invalid_token"}}`, "requires_relogin"},
		{"structured reused refresh", `Authentication failed (401): {"error":{"code":" REFRESH_TOKEN_REUSED "}}`, "requires_relogin"},
		{"transport failure", "proxy connection timeout", ""},
		{"unstructured auth keyword", "request failed: invalid_token", ""},
		{"auth keyword in provider message", `Authentication failed (401): {"error":{"code":"rate_limit_exceeded","message":"invalid_token"}}`, ""},
		{"message without code", `Authentication failed (401): {"error":{"message":"token_revoked"}}`, ""},
		{"malformed body", `Authentication failed (401): {"error":{"code":"invalid_token"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusError, ErrorMessage: tc.message}
			require.Equal(t, tc.want, AccountAuthInvalidReason(account))
		})
	}
}

func TestAccountAuthInvalidReasonRequiresOpenAIOAuthErrorState(t *testing.T) {
	for _, account := range []*Account{
		nil,
		{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, ErrorMessage: "Token revoked (401): synthetic"},
		{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusError, ErrorMessage: "Token revoked (401): synthetic"},
		{Platform: PlatformAnthropic, Type: AccountTypeOAuth, Status: StatusError, ErrorMessage: "Token revoked (401): synthetic"},
	} {
		require.Empty(t, AccountAuthInvalidReason(account))
	}
}

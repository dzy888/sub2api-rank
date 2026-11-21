//go:build unit

package service

import (
	"encoding/json"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestScopedRequestProxyExtraRequiresNetworkAndKeepsCreateContract(t *testing.T) {
	for _, body := range []string{
		`{"extra":{"openai_proxy_mode":"request","openai_proxy_ids":[11,22]}}`,
		`{"extra":{"openai_proxy_mode":"single"}}`,
		`{"extra":{"openai_proxy_ids":[22,11]}}`,
		`{"extra":{"openai_proxy_mode":"request","openai_proxy_ids":[11],"openai_oauth_responses_websockets_v2_mode":"ctx_pool"}}`,
	} {
		t.Run(body, func(t *testing.T) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(body), &fields))
			permissions, err := scopedAccountFieldPermissions(fields, false)
			require.NoError(t, err)
			require.Equal(t, []string{"accounts.network"}, permissions)
			_, err = scopedAccountFieldPermissions(fields, true)
			require.Equal(t, 400, infraerrors.Code(err), "new extra fields must not change creation authorization")
			var extra map[string]any
			require.NoError(t, json.Unmarshal(fields["extra"], &extra))
			_, err = scopedCodexPermissions(nil, extra, nil)
			require.Equal(t, 400, infraerrors.Code(err), "Codex import retains its existing extra contract")
		})
	}
}

func TestScopedRequestProxyExtraRejectsMalformedAndForeignFields(t *testing.T) {
	for _, extra := range []string{
		`{"openai_proxy_mode":"random"}`, `{"openai_proxy_mode":true}`, `{"openai_proxy_mode":null}`, `{"openai_proxy_mode":{}}`,
		`{"openai_proxy_ids":"11,22"}`, `{"openai_proxy_ids":[0]}`, `{"openai_proxy_ids":[-1]}`, `{"openai_proxy_ids":[11.5]}`,
		`{"openai_proxy_ids":[true]}`, `{"openai_proxy_ids":["11"]}`, `{"openai_proxy_ids":{}}`,
		`{"openai_proxy_mode":"request","openai_proxy_ids":[11],"credential_owner_account_id":123}`,
		`{"openai_proxy_mode":"request","openai_proxy_ids":[11],"openai_connections_managed":false}`,
		`{"openai_proxy_mode":"request","openai_proxy_ids":[11],"openai_excel_bps_proxy_ids":[22]}`,
	} {
		t.Run(extra, func(t *testing.T) {
			_, err := scopedAccountFieldPermissions(map[string]json.RawMessage{"extra": json.RawMessage(extra)}, false)
			require.Equal(t, 400, infraerrors.Code(err))
		})
	}
}

func TestScopedRequestProxyReadbackOnlyExposesSafeFields(t *testing.T) {
	a := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11, 22},
		"openai_oauth_responses_websockets_v2_mode": "ctx_pool", "private": "PRIVATE_VALUE", "password": "PRIVATE_PASSWORD",
	}}
	require.Equal(t, map[string]any{
		OpenAIProxyModeKey: OpenAIProxyModeRequest, OpenAIProxyIDsKey: []int64{11, 22},
		"openai_oauth_responses_websockets_v2_mode": "ctx_pool",
	}, ScopedAdminSafeExtra(a))
	a.Extra[OpenAIProxyIDsKey] = []any{"11"}
	require.NotContains(t, ScopedAdminSafeExtra(a), OpenAIProxyIDsKey)
	a.Type = AccountTypeAPIKey
	require.NotContains(t, ScopedAdminSafeExtra(a), OpenAIProxyModeKey)
	a.Type, a.Platform = AccountTypeOAuth, PlatformAnthropic
	require.NotContains(t, ScopedAdminSafeExtra(a), OpenAIProxyModeKey)
}

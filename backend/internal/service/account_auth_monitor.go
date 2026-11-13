package service

// AccountAuthInvalidReason exposes only a stable, credential-free reason.
// In particular, a provider message containing an auth keyword is insufficient
// evidence to trigger a paid or destructive re-login workflow.
func AccountAuthInvalidReason(account *Account) string {
	if account != nil && guardAccountHasAuthFailure(account) {
		return "requires_relogin"
	}
	return ""
}

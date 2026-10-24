package workflow

// maxTenant bounds a tenant ID so every store (a SQL column, a Redis key,
// an engine label) accepts it.
const maxTenant = 128

// ValidateTenant checks a Job.Tenant or RunQuery.Tenant: 1 to 128
// characters from letters, digits and ".", "_", ":", "-", "@". The
// characters are the queue-name set plus "@", so a tenant ID can be put
// into a key, a label or a log line without escaping. Errors wrap
// ErrInvalid.
func ValidateTenant(tenant string) error {
	if tenant == "" {
		return invalidf("tenant is empty")
	}
	if len(tenant) > maxTenant {
		return invalidf("tenant %q is longer than %d characters", tenant, maxTenant)
	}
	for _, r := range tenant {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == ':' || r == '-' || r == '@'
		if !ok {
			return invalidf("tenant %q has the character %q; use letters, digits and . _ : - @", tenant, r)
		}
	}
	return nil
}

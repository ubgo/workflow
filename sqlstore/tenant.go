package sqlstore

import (
	"context"
	"database/sql"
)

// scheduleTenantDefs is migration 009's table: Schedule.Tenant, one row
// per schedule that sets one. A side table rather than a column on
// schedules, because an ALTER TABLE ADD COLUMN cannot be written to run
// twice on MySQL (its DDL commits implicitly, so a failed migration is
// re-run), and a schedule without a row simply has no tenant.
func scheduleTenantDefs(t sqlTypes) []tableDef {
	return []tableDef{{stem: tblScheduleTenant, lines: []string{
		"schedule_id " + t.id + " NOT NULL PRIMARY KEY",
		// tenant is Schedule.Tenant, the Job.Tenant of every run the
		// schedule fires or is triggered to run; never empty here.
		"tenant " + t.name + " NOT NULL",
	}}}
}

// writeTenant stores tenant as schedule id's tenant in tx: no row for none.
// The delete comes first so the transaction opens with a write.
func (c *Client) writeTenant(ctx context.Context, tx *sql.Tx, id, tenant string) error {
	if _, err := c.st.exec(ctx, tx, "DELETE FROM {schedule_tenant} WHERE schedule_id = ?", id); err != nil {
		return err
	}
	if tenant == "" {
		return nil
	}
	_, err := c.st.exec(ctx, tx, "INSERT INTO {schedule_tenant} (schedule_id, tenant) VALUES (?, ?)", id, tenant)
	return err
}

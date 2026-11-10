// Package sqlrecorder is a recorder.Store on Postgres, MySQL or SQLite, so a
// run history the app owns lives in its own database, next to any driver
// (a Hatchet engine included), and survives the driver's own retention.
//
// It is built on database/sql alone and imports no database driver: the app
// opens the *sql.DB with the driver it already uses and names the engine:
//
//	db, _ := sql.Open("pgx", dsn)                  // the app's own driver
//	store, err := sqlrecorder.New(db, sqlrecorder.Postgres,
//		sqlrecorder.WithRetention(recorder.Retention{
//			RetentionRule: recorder.RetentionRule{Steps: 7 * 24 * time.Hour, Runs: 30 * 24 * time.Hour},
//		}))
//	err = store.Migrate(ctx)                        // idempotent, run on every start
//	rec, err := recorder.New(store)
//	c, err := workflow.Use(client, rec.Plugin())
//
// Tables (prefix "wf_rec_" by default, WithTablePrefix): runs (one row per
// run), steps (one row per step attempt), events (one row per step event),
// labels (one row per run label, for indexed label filters) and schema (the
// applied SchemaVersion). Steps are rows of their own rather than an array
// on the run row, so a busy run never rewrites one large row per event.
//
// Every write is one transaction that locks the run's row first and is
// retried on contention; reads answer workflow.RunQuerier and the
// recorder's richer Records and Run. recordertest.RunStore holds it to the
// Store contract on all three engines.
package sqlrecorder

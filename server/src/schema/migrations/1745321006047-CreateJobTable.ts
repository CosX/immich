import { Kysely, sql } from 'kysely';

export async function up(db: Kysely<any>): Promise<void> {
  // I wonder if we should have a column which has the time a job was started.
  // This would mean stale jobs can be cleaned up and retried.
  await sql`CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    queue VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    data JSON
  );`.execute(db);
  await sql`CREATE INDEX idx_jobs_pending ON jobs (queue);`.execute(db);

  await sql`CREATE OR REPLACE FUNCTION notify_job()
  RETURNS TRIGGER AS $$
  BEGIN
    PERFORM pg_notify('jobs', NEW.queue);
    RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;`.execute(db);

  await sql`CREATE TRIGGER notify_jobs
  AFTER INSERT ON jobs
  FOR EACH ROW
  EXECUTE FUNCTION notify_job();`.execute(db);
}

export async function down(db: Kysely<any>): Promise<void> {
  await sql`DROP TABLE IF EXISTS jobs;`.execute(db);
  await sql`DROP INDEX IF EXISTS idx_jobs_pending;`.execute(db);
  await sql`DROP FUNCTION IF EXISTS notify_job;`.execute(db);
  await sql`DROP TRIGGER IF EXISTS notify_jobs;`.execute(db);
}

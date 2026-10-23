CREATE TABLE IF NOT EXISTS jobs (
  id              uuid PRIMARY KEY,
  type            text        NOT NULL,
  payload         jsonb       NOT NULL DEFAULT '{}',
  priority        int         NOT NULL DEFAULT 5,   -- 0 (lowest) .. 9 (highest)
  status          text        NOT NULL,             -- scheduled|queued|running|retrying|completed|dead
  attempts        int         NOT NULL DEFAULT 0,
  max_attempts    int         NOT NULL DEFAULT 5,
  idempotency_key text        UNIQUE,
  run_at          timestamptz NOT NULL DEFAULT now(),
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  completed_at    timestamptz,
  last_error      text,
  result          jsonb
);
CREATE INDEX IF NOT EXISTS jobs_status_idx    ON jobs (status);
CREATE INDEX IF NOT EXISTS jobs_completed_idx ON jobs (completed_at) WHERE completed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_events (
  id     bigserial PRIMARY KEY,
  job_id uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event  text        NOT NULL,  -- started|completed|failed|dead|redelivered|duplicate_skipped|manual_retry|reconciled
  detail text,
  at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS job_events_job_idx ON job_events (job_id);
CREATE INDEX IF NOT EXISTS job_events_at_idx  ON job_events (at);

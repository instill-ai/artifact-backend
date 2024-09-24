BEGIN;

create type valid_run_source as enum ('RUN_SOURCE_CONSOLE', 'RUN_SOURCE_API');
create type valid_run_status as enum ('RUN_STATUS_COMPLETED', 'RUN_STATUS_FAILED', 'RUN_STATUS_PROCESSING', 'RUN_STATUS_QUEUED');
create type valid_catalog_action as enum ( 'RUN_ACTION_CREATE', 'RUN_ACTION_UPDATE', 'RUN_ACTION_DELETE', 'RUN_ACTION_CREATE_FILE', 'RUN_ACTION_PROCESS_FILE', 'RUN_ACTION_DELETE_FILE');

CREATE TABLE IF NOT EXISTS catalog_run
(
    uid            UUID PRIMARY KEY         DEFAULT gen_random_uuid(),
    catalog_uid    UUID                                               NOT NULL,
    status         valid_run_status                                   NOT NULL,
    source         valid_run_source                                   NOT NULL,
    action         valid_catalog_action                               NOT NULL,
    runner_uid     UUID                                               NOT NULL,
    requester_uid  UUID                                               NOT NULL,
    file_uids      VARCHAR(255)[],
    payload        JSONB,
    reference_uids VARCHAR(255)[],
    started_time   TIMESTAMP WITH TIME ZONE                           NOT NULL,
    completed_time TIMESTAMP WITH TIME ZONE,
    total_duration BIGINT,
    error          TEXT,
    create_time    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    update_time    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

COMMENT ON COLUMN catalog_run.total_duration IS 'in milliseconds';

CREATE INDEX IF NOT EXISTS idx_catalog_run_catalog_uid ON catalog_run (catalog_uid);
CREATE INDEX IF NOT EXISTS idx_catalog_run_status ON catalog_run (status);
CREATE INDEX IF NOT EXISTS idx_catalog_run_started_time ON catalog_run (started_time);
CREATE INDEX IF NOT EXISTS idx_catalog_run_completed_time ON catalog_run (completed_time);

COMMIT;

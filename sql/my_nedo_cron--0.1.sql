CREATE SCHEMA nedo_cron;
GRANT USAGE ON SCHEMA nedo_cron TO public;

CREATE SEQUENCE nedo_cron.jobid_seq;
GRANT USAGE ON SEQUENCE nedo_cron.jobid_seq TO public;

CREATE TABLE nedo_cron.jobs (
    jobid bigint PRIMARY KEY DEFAULT nextval('nedo_cron.jobid_seq'),
    schedule text not null,
    command text not null,
    nodename text not null default 'localhost',
    nodeport int not null default coalesce(inet_server_port(), current_setting('port')::int),
    database text not null default current_database(),
    username text not null default current_user
);

ALTER SEQUENCE nedo_cron.jobid_seq OWNED BY nedo_cron.jobs.jobid;

CREATE SEQUENCE nedo_cron.run_id_seq;
GRANT USAGE ON SEQUENCE nedo_cron.run_id_seq TO public;

CREATE TABLE nedo_cron.job_run_details (
    run_id bigint primary key DEFAULT nextval('nedo_cron.run_id_seq'),
    jobid bigint not null REFERENCES nedo_cron.jobs (jobid) on DELETE CASCADE,
    job_pid int,
    database text,
    username text,
    status text not null check ( status in ('starting', 'running', 'succeeded', 'failed', 'timeout', 'cancelled') ),
    return_message text,
    start_time timestamptz not null,
    end_time timestamptz
);

ALTER SEQUENCE nedo_cron.run_id_seq OWNED BY nedo_cron.job_run_details.run_id;
CREATE INDEX job_run_details_start_time_index on nedo_cron.job_run_details(jobid, start_time desc );

CREATE FUNCTION  nedo_cron.schedule(schedule text, command text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$schedule_job$$;
    COMMENT ON FUNCTION nedo_cron.schedule(text,text)
        IS 'schedule a job';


CREATE FUNCTION  nedo_cron.unschedule(bigint)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$unschedule_job$$;
    COMMENT ON FUNCTION nedo_cron.unschedule(bigint)
        IS 'unschedule a job';

CREATE FUNCTION nedo_cron.invalidate_job_cache()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$invalidate_job_cache$$;
    COMMENT ON FUNCTION nedo_cron.invalidate_job_cache()
        IS 'invalidate job cache';

CREATE TRIGGER cron_job_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON nedo_cron.jobs
    FOR STATEMENT EXECUTE PROCEDURE nedo_cron.invalidate_job_cache();

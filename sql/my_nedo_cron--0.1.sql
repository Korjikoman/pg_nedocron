CREATE SCHEMA nedo_cron;
GRANT USAGE ON SCHEMA nedo_cron TO public;

CREATE SEQUENCE nedo_cron.jobid_seq;
GRANT USAGE ON SEQUENCE nedo_cron.jobid_seq TO public;

CREATE TABLE nedo_cron.jobs (
    jobid bigint PRIMARY KEY ,
    schedule text not null,
    query text not null,
    host text not null default 'localhost',
    port int not null default inet_server_port(),
    database text not null default current_database(),
    username text not null default current_user
);

CREATE TABLE nedo_cron.results (
    resultid bigint primary key,
    jobid text not null ,
    start_time timestamptz,
    end_time timestamptz,
    status int not null,
    output text
);


CREATE FUNCTION  nedo_cron.schedule_job(schedule text, command text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$schedule_job$$;
    COMMENT ON FUNCTION nedo_cron.schedule_job(text,text)
        IS 'schedule a job';


CREATE FUNCTION  nedo_cron.unschedule_job(bigint)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$unschedule_job$$;
    COMMENT ON FUNCTION nedo_cron.unschedule_job(bigint)
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

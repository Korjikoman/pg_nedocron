CREATE SCHEMA my_cron
    CREATE TABLE jobs (
        job_name text not null,
        cron_string not null ,
        query text not null,
        connection_string text not null ,
        PRIMARY KEY (job_name)
    )

    CREATE TABLE results (
        id bigint primary key default nextval('task_id_sequence'),
        job_name text not null ,
        start_time timestamptz,
        end_time timestamptz,
        output text
    )

    CREATE SEQUENCE task_id_sequence NO CYCLE ;

-- CREATE FUNCTION  my_cron.shedule(text, text, text, text)
--     RETURNS text
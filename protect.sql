\set ON_ERROR_STOP on
\timing on

\echo ''
\echo 'my_nedo_cron: демонстрационный тест'
\echo ''

CREATE EXTENSION IF NOT EXISTS my_nedo_cron;

\echo ''
\echo 'Расширение загружено, worker запущен, база настроена'

SELECT current_database() AS current_database,
       current_setting('nedo_cron.database_name', true) AS nedo_cron_database_name,
       current_setting('nedo_cron.statement_timeout_ms', true) AS statement_timeout_ms;

SELECT backend_type, state
FROM pg_stat_activity
WHERE backend_type = 'my_nedo_cron';

\echo ''
\echo 'Основные объекты расширения'

SELECT to_regnamespace('nedo_cron') AS schema,
       to_regclass('nedo_cron.jobs') AS jobs_table,
       to_regclass('nedo_cron.job_run_details') AS run_details_table,
       to_regprocedure('nedo_cron.schedule(text,text)') AS schedule_function,
       to_regprocedure('nedo_cron.unschedule(bigint)') AS unschedule_function,
       to_regprocedure('nedo_cron.check_schedule(text)') AS check_schedule_function;

\echo ''
\echo 'Очищаем старые демонстрационные данные'

DO $$
DECLARE
    id bigint;
BEGIN
    FOR id IN SELECT jobid FROM nedo_cron.jobs LOOP
        BEGIN
            PERFORM nedo_cron.unschedule(id);
        EXCEPTION WHEN others THEN
            RAISE NOTICE 'ignored cleanup error for job %: %', id, SQLERRM;
        END;
    END LOOP;
END;
$$;

SELECT pg_sleep(1);

TRUNCATE TABLE nedo_cron.job_run_details, nedo_cron.jobs
RESTART IDENTITY CASCADE;

SELECT pg_sleep(1);

SELECT count(*) AS jobs_after_cleanup FROM nedo_cron.jobs;
SELECT count(*) AS run_details_after_cleanup FROM nedo_cron.job_run_details;

\echo ''
\echo 'Parser: валидные и невалидные расписания'

SELECT schedule,
       nedo_cron.check_schedule(schedule) AS is_valid
FROM (
    VALUES
        ('* * * * *'),
        ('*/5 * * * *'),
        ('0 12 $ * *'),
        ('5 seconds'),
        ('59 seconds'),
        ('0 seconds'),
        ('*//2 * * * *'),
        ('* * * * 8')
) AS schedules(schedule);

\echo ''
\echo 'Понятная ошибка parser-а'

DO $$
BEGIN
    PERFORM nedo_cron.schedule('* * * * 8', 'SELECT 1');
EXCEPTION WHEN others THEN
    RAISE NOTICE 'expected parser error: %', SQLERRM;
END;
$$;

\echo ''
\echo 'Успешная seconds job'

CREATE TEMP TABLE protect_jobs (
    label text PRIMARY KEY,
    jobid bigint NOT NULL
) ON COMMIT PRESERVE ROWS;

INSERT INTO protect_jobs(label, jobid)
SELECT 'success_seconds', nedo_cron.schedule('2 seconds', 'SELECT 1');

SELECT 'created job' AS action, *
FROM protect_jobs
WHERE label = 'success_seconds';

SELECT pg_sleep(6);

SELECT d.run_id,
       d.jobid,
       d.job_pid,
       d.status,
       d.return_message,
       d.start_time,
       d.end_time
FROM nedo_cron.job_run_details d
JOIN protect_jobs j ON j.jobid = d.jobid
WHERE j.label = 'success_seconds'
ORDER BY d.run_id;

\echo ''
\echo 'Direct INSERT тоже подхватывается worker-ом'

WITH inserted AS (
    INSERT INTO nedo_cron.jobs(schedule, command)
    VALUES ('2 seconds', 'SELECT 42')
    RETURNING jobid
)
INSERT INTO protect_jobs(label, jobid)
SELECT 'direct_insert', jobid
FROM inserted;

SELECT 'directly inserted job' AS action, *
FROM protect_jobs
WHERE label = 'direct_insert';

SELECT pg_sleep(4);

SELECT d.run_id,
       d.jobid,
       d.status,
       d.return_message,
       d.start_time,
       d.end_time
FROM nedo_cron.job_run_details d
JOIN protect_jobs j ON j.jobid = d.jobid
WHERE j.label = 'direct_insert'
ORDER BY d.run_id;

\echo ''
\echo 'Ошибка SQL-команды сохраняется как failed'

INSERT INTO protect_jobs(label, jobid)
SELECT 'sql_error', nedo_cron.schedule('2 seconds', 'SELECT 1/0');

SELECT pg_sleep(4);

SELECT d.run_id,
       d.jobid,
       d.status,
       d.return_message,
       d.start_time,
       d.end_time
FROM nedo_cron.job_run_details d
JOIN protect_jobs j ON j.jobid = d.jobid
WHERE j.label = 'sql_error'
ORDER BY d.run_id;

\echo ''
\echo 'Долгий запрос отменяется по timeout'

INSERT INTO protect_jobs(label, jobid)
SELECT 'timeout', nedo_cron.schedule('5 seconds', 'SELECT pg_sleep(30)');

SELECT pg_sleep(greatest(12, ceil(current_setting('nedo_cron.statement_timeout_ms')::numeric / 1000)::integer + 3));

SELECT d.run_id,
       d.jobid,
       d.status,
       d.return_message,
       d.start_time,
       d.end_time
FROM nedo_cron.job_run_details d
JOIN protect_jobs j ON j.jobid = d.jobid
WHERE j.label = 'timeout'
ORDER BY d.run_id;

\echo ''
\echo 'Worker видит UPDATE в jobs'

INSERT INTO protect_jobs(label, jobid)
SELECT 'update_invalidation', nedo_cron.schedule('2 seconds', 'SELECT 1');

SELECT pg_sleep(3);

UPDATE nedo_cron.jobs
SET command = 'SELECT 1/0'
WHERE jobid = (SELECT jobid FROM protect_jobs WHERE label = 'update_invalidation');

SELECT pg_sleep(4);

SELECT d.run_id,
       d.jobid,
       d.status,
       d.return_message,
       d.start_time,
       d.end_time
FROM nedo_cron.job_run_details d
JOIN protect_jobs j ON j.jobid = d.jobid
WHERE j.label = 'update_invalidation'
ORDER BY d.run_id;

\echo ''
\echo 'unschedule удаляет job и связанные run details'

SELECT 'before unschedule' AS phase,
       j.label,
       j.jobid,
       count(d.run_id) AS run_details_count
FROM protect_jobs j
LEFT JOIN nedo_cron.job_run_details d ON d.jobid = j.jobid
GROUP BY j.label, j.jobid
ORDER BY j.label;

SELECT nedo_cron.unschedule(jobid) AS unscheduled, label, jobid
FROM protect_jobs
WHERE label IN ('success_seconds',
                'direct_insert',
                'sql_error',
                'timeout',
                'update_invalidation')
ORDER BY label;

SELECT pg_sleep(2);

SELECT 'after unschedule' AS phase,
       count(*) AS remaining_jobs
FROM nedo_cron.jobs;

SELECT 'after unschedule cascade' AS phase,
       count(*) AS remaining_run_details_for_demo_jobs
FROM nedo_cron.job_run_details
WHERE jobid IN (SELECT jobid FROM protect_jobs);

\echo ''
\echo 'Финальная проверка'

SELECT count(*) AS final_jobs_count FROM nedo_cron.jobs;
SELECT count(*) AS final_run_details_for_demo_jobs
FROM nedo_cron.job_run_details
WHERE jobid IN (SELECT jobid FROM protect_jobs);

SELECT 'my_nedo_cron protect demo passed' AS result;

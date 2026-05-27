
#include <stdio.h>
#include <stdbool.h>
#include <time.h>

#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/inval.h"
#include "fmgr.h"
#include <libpq-fe.h>
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "signal.h"
#include <poll.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <utils/snapmgr.h>
#include <utils/wait_event.h>

#include "utils/backend_status.h"
#include "catalog/indexing.h"
#include "cron_task.h"
#include "cron_parser.h"
#include "cron_time.h"
#include "utils/palloc.h"
#include "utils/timestamp.h"
#include "../include/cron_sql.h"
#include "utils/hsearch.h"
#include "mb/pg_wchar.h"
#include "utils/guc.h"
#include "storage/latch.h"

static const int MaxSleep = 1000; // ms

volatile sig_atomic_t sighupFlag = false;
volatile sig_atomic_t sigtermFlag = false;

HTAB *CronJobHashTable = NULL;
HTAB *CronTaskHashTable = NULL;
MemoryContext CronJobContext = NULL;
MemoryContext CronTaskContext = NULL;
bool CronJobCacheValid = false;
const char *tabname = "nedo_cron jobs";
char *NedoCronDatabaseName = "postgres";
static char *extension_name = "nedo_cron";
int TaskTimeout = 10000; // 10sec
int maxPendingTasks = 20;
int StatementTimeoutMs = 10000;
static bool CronJobRelcacheCallbackRegistered = false;
static Oid CronJobRelcacheOid = InvalidOid;



PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(schedule_job);
PG_FUNCTION_INFO_V1(unschedule_job);
PG_FUNCTION_INFO_V1(invalidate_job_cache);
PG_FUNCTION_INFO_V1(check_schedule);


PGDLLEXPORT void PgOctopusWorkerMain(Datum arg);
static void sigtermHandler(SIGNAL_ARGS);
static void sighupHandler(SIGNAL_ARGS);
List * LoadTasksList();
static bool ShouldRunTask(CronSchedule *schedule, TimestampTz currentTime, bool floatingHourOrMinute,bool fixedHourOrMinute) ;
void StartPendingTasks(List * taskList, TimestampTz currentTime);
void ManageCronTasks(List * tasks, TimestampTz currentTime);

static bool TaskNeedsSocketWait(CronTask * task);
static uint32 TaskWaitEvents(CronTask * task);
static int CalculateWaitTimeout(List * taskList, TimestampTz currentTime);

static void CronJobRelcacheCallback(Datum arg, Oid relid);
static void RegisterCronJobRelcacheCallbackIfNeeded(void);



static void CronJobRelcacheCallback(Datum arg, Oid relid) {
    Oid jobsRelid = DatumGetObjectId(arg);
    if (relid == InvalidOid || relid == jobsRelid) {
        elog(LOG, "my_nedo_cron: jobs relcache invalidation received");
        CronJobCacheValid = false;
    }
}

static void RegisterCronJobRelcacheCallbackIfNeeded(void) {
    Oid schemaId = InvalidOid;
    Oid jobsRelid = InvalidOid;

    if (CronJobRelcacheCallbackRegistered) {
        return;
    }

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    if (CronLoaded()) {
        schemaId = get_namespace_oid(CRON_SCHEMA_NAME, true);
        if (OidIsValid(schemaId)) {
            jobsRelid = get_relname_relid(JOBS_TABLE_NAME, schemaId);
        }
    }
    PopActiveSnapshot();
    CommitTransactionCommand();

    if (!OidIsValid(jobsRelid)) {
        return;
    }

    CronJobRelcacheOid = jobsRelid;

    CacheRegisterRelcacheCallback(CronJobRelcacheCallback, ObjectIdGetDatum(CronJobRelcacheOid));
    elog(LOG, "my_nedo_cron: registered jobs relcache callback, relid=%u",
       CronJobRelcacheOid);
    CronJobRelcacheCallbackRegistered = true;
}

void _PG_init() {
    BackgroundWorker worker;
    char * func_name = "PgOctopusWorkerMain";

    memset(&worker, 0, sizeof(BackgroundWorker));

    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    snprintf(worker.bgw_function_name, BGW_MAXLEN , "%s", func_name);
    worker.bgw_main_arg = Int32GetDatum(0);
    worker.bgw_notify_pid = 0;
    sprintf(worker.bgw_library_name, "my_nedo_cron");
    snprintf(worker.bgw_name, sizeof(worker.bgw_name), "my_nedo_cron_sheduler");
    snprintf(worker.bgw_type, sizeof(worker.bgw_type), "my_nedo_cron");

    DefineCustomStringVariable("nedo_cron.database_name",
        "Database containing nedo_cron metadata tables",
        NULL,
        &NedoCronDatabaseName,
        "postgres",
        PGC_POSTMASTER,
        0,
        NULL,
        NULL,
        NULL
    );

    DefineCustomIntVariable("nedo_cron.statement_timeout_ms",
        "Maximum SQL execution time for nedo_cron jobs",
        NULL,
        &StatementTimeoutMs,
        10000,
        0,
        INT_MAX,
        PGC_SIGHUP,
        0,
        NULL,
        NULL,
        NULL
    );

    RegisterBackgroundWorker(&worker);
}

static void SetTaskError(CronTask * task, const char * fmt, ...) {
    MemoryContext old_context;
    va_list args;
    char message[1024];

    if (fmt == NULL || fmt[0] == '\0') {
        fmt = "unknown error, message text = '\\0'";
    }
    va_start(args, fmt);
    vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);


    old_context = MemoryContextSwitchTo(CronTaskContext);
    task->errorMessage = pstrdup(message);
    MemoryContextSwitchTo(old_context);
}
static void sigtermHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    sigtermFlag = true;
    SetLatch(MyLatch); // wake up
    errno = save_errno;
}


static void sighupHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    sighupFlag = true;
    SetLatch(MyLatch); // wake up
    errno = save_errno;
}


void PgOctopusWorkerMain(Datum arg) {
    MemoryContext loop_memory_context = NULL;
    TimestampTz lastJobReloadTime = 0;

    pqsignal(SIGHUP, sighupHandler);
    pqsignal(SIGTERM, sigtermHandler);
    pqsignal(SIGINT, SIG_IGN); // ignored

    BackgroundWorkerUnblockSignals(); // разблокируем сигналы для bgw

    // connecting to db
    BackgroundWorkerInitializeConnection(NedoCronDatabaseName, NULL, BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ALLOWCONN);

    CronJobContext = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronJob context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);

    CronTaskContext = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronState context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);


    loop_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronLoop context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);

    CronJobHashTable = CreateCronJobHashTable();
    CronTaskHashTable = CreateCronTaskHashTable();

    MemoryContextSwitchTo(loop_memory_context);

    elog(LOG, "my_nedo_cron started ...");
    while (!sigtermFlag) {
        List* cronTasks = NIL;
        TimestampTz currentTime = GetCurrentTimestamp();

        AcceptInvalidationMessages();
        RegisterCronJobRelcacheCallbackIfNeeded();

        if (sighupFlag) {
            sighupFlag = false;
            ProcessConfigFile(PGC_SIGHUP);
            CronJobCacheValid = false;
        }

        if (!CronJobCacheValid || lastJobReloadTime == 0 ||
            TimestampDifferenceExceeds(lastJobReloadTime, currentTime, 60000)) {
            ReloadCronJobs();
            lastJobReloadTime = currentTime;
        }

        cronTasks = LoadTasksList();

        StartPendingTasks(cronTasks, currentTime);


        WaitForEvent(cronTasks);

        currentTime = GetCurrentTimestamp();
        ManageCronTasks(cronTasks, currentTime);

        MemoryContextReset(loop_memory_context);


    }
    elog(LOG, "my_nedo_cron exiting ...");

    proc_exit(0);
}


List * LoadTasksList() {
    List * tasks = NIL;
    HASH_SEQ_STATUS status;
    CronTask * task = NULL;
    hash_seq_init(&status, CronTaskHashTable);
    while ((task = hash_seq_search(&status)) != NULL) {
        tasks = lappend(tasks, task);
    }

    return tasks;
}

static bool TaskNeedsSocketWait(CronTask * task) {
    return task->state == CRON_TASK_CONNECTING ||
           task->state == CRON_TASK_SENDING ||
           task->state == CRON_TASK_RUNNING;
}

static uint32 TaskWaitEvents(CronTask * task) {
    if (task->polling_status == PGRES_POLLING_READING) {
        return WL_SOCKET_READABLE;
    }
    if (task->polling_status == PGRES_POLLING_WRITING) {
        return WL_SOCKET_WRITEABLE;
    }
    return 0;
}

static int CalculateWaitTimeout(List * taskList, TimestampTz currentTime) {
    TimestampTz nextEventTime;
    ListCell * taskCell = NULL;
    long waitSecs = 0;
    int waitMs = 0;
    int timeoutMs = 0;

    nextEventTime = EndOfMinute(currentTime);
    foreach(taskCell, taskList) {
        CronTask * task = (CronTask *) lfirst(taskCell);
        CronJob * job = getCronJob(task->jobId);

        if (task->state == CRON_TASK_WAITING && task->pendingRunCount > 0) {
            return 0;
        }

        if (job != NULL && job->schedule.SECONDS && task->nextRunTime != 0) {
            if (TimestampDifferenceExceeds(task->nextRunTime, nextEventTime, 0)) {
                nextEventTime = task->nextRunTime;
            }
        }

        if ( (task->state == CRON_TASK_CONNECTING || task->state == CRON_TASK_SENDING)
            && task->startDeadline != 0) {
            if (TimestampDifferenceExceeds(task->startDeadline, nextEventTime, 0)) {
                nextEventTime = task->startDeadline;
            }
        }

        if (task->state == CRON_TASK_RUNNING && task->runDeadline != 0 && !task->cancelRequested) {
            if (TimestampDifferenceExceeds(task->runDeadline, nextEventTime, 0)) {
                nextEventTime = task->runDeadline;
            }
        }

    }

    TimestampDifference(currentTime, nextEventTime, &waitSecs, &waitMs);

    timeoutMs = (int) waitSecs * 1000 + waitMs / 1000;
    if (timeoutMs < 0) {
        timeoutMs = 0;
    }
    if (timeoutMs > MaxSleep) {
        timeoutMs = MaxSleep;
    }
    return timeoutMs;
}

void WaitForEvent(List *taskList) {
    WaitEventSet * set = NULL;
    WaitEvent *events = NULL;
    TimestampTz currentTime;
    ListCell * taskCell = NULL;
    int timeoutMs = 0;
    int maxEvents = 0;
    int eventCount;
    currentTime = GetCurrentTimestamp();

    foreach(taskCell, taskList) {
        CronTask * task = (CronTask *) lfirst(taskCell);
        task->isSocketReady = false;
    }

    timeoutMs = CalculateWaitTimeout(taskList, currentTime);
    if (timeoutMs ==0) return;

    maxEvents = list_length(taskList) + 2;
    ResourceOwner oldResourceOwner = CurrentResourceOwner;

    set = CreateWaitEventSet(oldResourceOwner, maxEvents);
    events = palloc0(sizeof(WaitEvent) * maxEvents);

    AddWaitEventToSet(set,
        WL_LATCH_SET,
        PGINVALID_SOCKET,
        MyLatch,
        NULL);

    AddWaitEventToSet(set,
        WL_POSTMASTER_DEATH,
        PGINVALID_SOCKET,
        NULL,
        NULL);

    foreach(taskCell, taskList) {
        CronTask * task = (CronTask*) lfirst(taskCell);
        uint32 waitEvents;
        int socket;

        if (!TaskNeedsSocketWait(task)) continue;
        waitEvents = TaskWaitEvents(task);

        if (waitEvents == 0) continue;

        if (task->conn == NULL) {
            task->state = CRON_TASK_ERROR;
            task->polling_status = 0;
            SetTaskError(task,
                "[my_nedo_cron] connection is NULL, job_id: %ld", task->jobId );
            continue;
        }

        socket = PQsocket(task->conn);
        if (socket < 0) {
            task->state = CRON_TASK_ERROR;
            task->polling_status = PGRES_POLLING_READING;
            SetTaskError(task,
                "[my_nedo_cron] connection socket < 0, job_id: %ld", task->jobId );
            continue;
        }

        AddWaitEventToSet(set,waitEvents, socket, NULL, task);
    }

    eventCount = WaitEventSetWait(set, timeoutMs, events, maxEvents, PG_WAIT_EXTENSION);
    for (int i = 0; i < eventCount; i++) {
        if (events[i].events & WL_LATCH_SET) {
            ResetLatch(MyLatch);
            continue;
        }

        if (events[i].events & WL_POSTMASTER_DEATH) {
            proc_exit(1);
        }
        if (events[i].user_data != NULL) {
            CronTask * task = (CronTask*)events[i].user_data;
            task->isSocketReady=true;
        }
    }
    pfree(events);
    FreeWaitEventSet(set);
    CHECK_FOR_INTERRUPTS();
}


void ManageCronTasks(List * tasks, TimestampTz currentTime) {
    ListCell * taskCell = NULL;
    foreach(taskCell, tasks) {
        CronTask * task = (CronTask* )lfirst(taskCell);
        ManageCronTask(task, currentTime);
    }
}

void ManageCronTask(CronTask *task, TimestampTz currentTime) {
    CronJob *job = getCronJob(task->jobId);
    CronTaskState taskState = task->state;
    int64 key = task->jobId;
    int connectionBusy = 0;
    PGconn *connection = task->conn;
    ConnStatusType conn_status = CONNECTION_BAD;

    PGresult * result = NULL;
    bool stopReadingResults = false;

    if (job == NULL && taskState != CRON_TASK_DONE) {
        task->state = CRON_TASK_ERROR;
        SetTaskError(task, "job metadata not found");
        taskState = task->state;
    }

    switch (taskState) {
        case CRON_TASK_WAITING: {
            if (task->isActive == false) {
                bool found = false;
                hash_search(CronTaskHashTable, &key, HASH_REMOVE, &found);
                break;
            }

            if (task->pendingRunCount == 0) break;

            task->pendingRunCount --;
            task->state = CRON_TASK_START;
            break;
        }
        case CRON_TASK_START: {
            const char* dbEncoding = GetDatabaseEncodingName();
            TimestampTz deadline = 0;
            char optionsString[64];
            char nodePortString[12];
            snprintf(nodePortString, sizeof(nodePortString), "%d", job->nodePort); // для values
            snprintf(optionsString, sizeof(optionsString), "-c statement_timeout=%d", StatementTimeoutMs); // для values

            const char * keywords[] = {
                "host",
                "port",
                "fallback_application_name",
                "client_encoding",
                "dbname",
                "user",
                "options",

                NULL
            };

            const char * values[] = {
                job->nodeName,
                nodePortString,
                extension_name,
                dbEncoding,
                job->database,
                job->userName,
                optionsString,
                NULL
            };


            elog(LOG, "my_nedo_cron: starting job %ld: %s", key, job->command);
            task->runId = InsertRunDetailsStart(task,job,GetCurrentTimestamp());
            connection = PQconnectStartParams(keywords, values, false);

            if (connection == NULL) {
                SetTaskError(task, "PQconnectStartParams returned NULL");
                task->state = CRON_TASK_ERROR;
                task->polling_status = 0;
                break;
            }

            task->conn = connection;

            if (PQsetnonblocking(connection, 1) != 0 ) {
                SetTaskError(task,PQerrorMessage(connection));
                task->state = CRON_TASK_ERROR;
                task->polling_status = 0;
                break;
            }

            if (PQstatus(connection) == CONNECTION_BAD) {
                SetTaskError(task,PQerrorMessage(connection));
                task->state = CRON_TASK_ERROR;
                task->polling_status = 0;
                break;
            }
            deadline = TimestampTzPlusMilliseconds(currentTime,TaskTimeout);
            task->startDeadline = deadline;
            task->polling_status = PGRES_POLLING_WRITING;
            task->state = CRON_TASK_CONNECTING;

            break;
        }



        case CRON_TASK_CONNECTING: {

            PostgresPollingStatusType pollingStatus = 0;

            if (!task->isActive) {
                task->state= CRON_TASK_ERROR;
                task->polling_status = 0;
                SetTaskError(task, "job has been removed") ;
                break;
            }

            if (TimestampDifferenceExceeds(task->startDeadline, currentTime, 0)) {
                task->state = CRON_TASK_ERROR;
                task->polling_status = 0;
                SetTaskError(task, "timeout reached, job_id=%ld", task->jobId);
                break;
            }

            conn_status  = PQstatus(connection);
            if (conn_status == CONNECTION_BAD) {
                SetTaskError(task, PQerrorMessage(connection));
                task->polling_status = 0;
                task->state = CRON_TASK_ERROR;
                break;
            }

            if (!task->isSocketReady) {
                break;
            }

            pollingStatus = PQconnectPoll(connection);
            if (pollingStatus == PGRES_POLLING_OK) {
                int pid = PQbackendPID(connection);
                UpdatePID(task->runId, pid);
                task->polling_status = PGRES_POLLING_WRITING;
                task->state = CRON_TASK_SENDING;
            }
            else if (pollingStatus == PGRES_POLLING_FAILED) {
                task->polling_status = 0;
                task->state = CRON_TASK_ERROR;
                SetTaskError(task, PQerrorMessage(connection));

            }
            else {
                task->polling_status = pollingStatus;
            }
            break;



        }
        case CRON_TASK_SENDING: {
            /* этап отправки данных в базу */

            char * command = job->command;
            int send_status = 0;
            if (!task->isActive) {
                task->state= CRON_TASK_ERROR;
                task->polling_status = 0;
                SetTaskError(task,"job has been removed");
                break;
            }

            if (TimestampDifferenceExceeds(task->startDeadline, currentTime, 0)) {
                task->state = CRON_TASK_ERROR;
                task->polling_status = 0;
                SetTaskError(task, "timeout reached, job_id=%ld", task->jobId);
                break;
            }

            conn_status  = PQstatus(connection);
            if (conn_status == CONNECTION_BAD) {
                SetTaskError(task,PQerrorMessage(connection));
                task->polling_status = 0;
                task->state = CRON_TASK_ERROR;
                break;
            }

            if (!task->isSocketReady) {
                break;
            }

            send_status = PQsendQuery(connection, command);
            if (send_status == 1) {
                elog(LOG, "my_nedo_cron: query sent for job %ld", key);
                task->polling_status = PGRES_POLLING_READING;
                task->state = CRON_TASK_RUNNING;
                task->startDeadline = 0;
                task->runDeadline = TimestampTzPlusMilliseconds(currentTime, StatementTimeoutMs);
                elog(LOG,
                   "my_nedo_cron: job " INT64_FORMAT " runDeadline set, statement_timeout_ms=%d",
                   task->jobId,
                   StatementTimeoutMs);
                task->cancelRequested = false;
                task->timedOut = false;
            }
            else {
                SetTaskError(task,PQerrorMessage(connection));
                task->polling_status = 0;
                task->state = CRON_TASK_ERROR;
            }

            break;

        }
        case CRON_TASK_RUNNING: {
            /* задание выполняется */
            if (!task->isActive) {
                task->state= CRON_TASK_ERROR;
                task->polling_status = 0;
                SetTaskError(task,"job has been removed");
                break;
            }
            conn_status  = PQstatus(connection);
            if (conn_status == CONNECTION_BAD) {
                SetTaskError(task,PQerrorMessage(connection));
                task->polling_status = 0;
                task->state = CRON_TASK_ERROR;
                break;
            }

            if (TimestampDifferenceExceeds(task->runDeadline, currentTime, 0)
                && task->runDeadline !=0
                && !task->cancelRequested) {
                PGcancel *cancel;
                char errbuf[256];

                cancel = PQgetCancel(connection);
                elog(LOG,
       "my_nedo_cron: runtime timeout reached for job " INT64_FORMAT,
       task->jobId);
                if (cancel == NULL) {
                    SetTaskError(task,
      "could not cancel timed out job, job_id=" INT64_FORMAT ": PQgetCancel returned NULL",
      task->jobId);
                    task->timedOut = true;
                    task->state = CRON_TASK_ERROR;
                    task->polling_status = 0;
                    break;
                }
                if ( PQcancel(cancel,errbuf,sizeof(errbuf)) == 0) {
                    SetTaskError(task,
                        "could not cancel timed out job, job_id=" INT64_FORMAT " : %s",
                        task->jobId,
                        errbuf);
                    task->cancelRequested = true;
                    task->timedOut = true;
                    task->state = CRON_TASK_ERROR;
                    task->runDeadline = 0;
                    task->polling_status = 0;

                    PQfreeCancel(cancel);

                    break;
                }
                PQfreeCancel(cancel);
                task->cancelRequested = true;
                task->timedOut = true;
                task->runDeadline = 0;
                task->polling_status = PGRES_POLLING_READING;

            }

            if (!task->isSocketReady) {
                break;
            }



            if (PQconsumeInput(connection) != 1) {
                SetTaskError(task,PQerrorMessage(connection));
                task->polling_status = 0;
                task->state = CRON_TASK_ERROR;
                break;
            }

            connectionBusy = PQisBusy(connection);
            if (connectionBusy == 1) {
                break;
            }

            while ((result = PQgetResult(connection)) != NULL) {
                ExecStatusType exec_status = PQresultStatus(result);
                switch (exec_status) {
                    case PGRES_TUPLES_OK: {
                        break;
                    }
                    case PGRES_COMMAND_OK : {
                        break;
                    }
                    case PGRES_BAD_RESPONSE:
                    case PGRES_FATAL_ERROR:
                    case PGRES_NONFATAL_ERROR: {
                        const char * sqlState = PQresultErrorField(result, PG_DIAG_SQLSTATE);
                        // 57014 - timeout
                        if (sqlState != NULL && strcmp(sqlState, "57014") == 0) {
                            SetTaskError(task,PQresultErrorMessage(result));
                            task->timedOut = true;
                            task->polling_status = 0;
                            task->state = CRON_TASK_ERROR;
                        }else {
                            SetTaskError(task,PQresultErrorMessage(result));
                            task->polling_status = 0;
                            task->state = CRON_TASK_ERROR;
                        }

                        stopReadingResults = true;
                        break;
                    }
                    case PGRES_COPY_BOTH:
                    case PGRES_COPY_IN:
                    case PGRES_COPY_OUT: {
                        SetTaskError(task,"COPY not supported");
                        task->polling_status = 0;
                        task->state = CRON_TASK_ERROR;
                        stopReadingResults = true;
                        break;
                    }

                    default: {
                        break;
                    }

                }
                PQclear(result);
                if (stopReadingResults) {
                    break;
                }

            }
            PQfinish(connection);


            task->conn = NULL;
            task->polling_status = 0;
            task->isSocketReady = false;
            if (task->state != CRON_TASK_ERROR) {
                elog(LOG, "my_nedo_cron: job %ld completed", key);

                task->state = CRON_TASK_DONE;
                UpdateRunDetailsFinish(task->runId, "succeeded", "OK", GetCurrentTimestamp());

            }
            break;

        }
        case CRON_TASK_ERROR: {

            if (task->runId !=0 ) {
                UpdateRunDetailsFinish(task->runId,
                    task->timedOut ? "timeout" : "failed",
                    task->errorMessage,
                    GetCurrentTimestamp());
                task->runId = 0;
            }

            if (task->errorMessage != NULL) {
                elog(LOG, "error running job %ld: %s", key, task->errorMessage);
            }

            if (connection != NULL) {
                PQfinish(connection);
                task->conn = NULL;
            }

            if (!task->isActive) {
                bool found = false;
                hash_search(CronTaskHashTable, &key, HASH_REMOVE, &found);
                break;
            }
            task->state = CRON_TASK_DONE;
            task->polling_status = 0;
            task->startDeadline = 0;
            task->runDeadline = 0;
            task->cancelRequested = false;
            task->isSocketReady = false;

            break;
        }

        case CRON_TASK_DONE:
        default: {
            if (task->pendingRunCount > maxPendingTasks) {
                task->pendingRunCount = maxPendingTasks;
            }
            ResetCronTaskAfterRun(task);


            break;
        }


    }
}


void StartPendingTask(CronTask* task, ClockStatus clock_status, TimestampTz lastMinute, TimestampTz currentTime) {

    TimestampTz currentMinute = StartOfMinute(currentTime);
    TimestampTz virtTime = lastMinute;

    CronJob * job = getCronJob(task->jobId);

    CronSchedule *schedule = &job->schedule;

    switch (clock_status) {
        case CLOCK_PROGRESSED: {
            while (virtTime < currentMinute) {
                virtTime = TimestampTzPlusMilliseconds(virtTime, 60*1000);

                if (ShouldRunTask(schedule, virtTime, true, true)) {
                    task->pendingRunCount ++;
                    elog(LOG, "my_nedo_cron: job %ld is due", task->jobId);
                }
            }
            break;
        }
        case CLOCK_JUMP_FORWARD: {
            while (virtTime < currentMinute) {
                virtTime = TimestampTzPlusMilliseconds(virtTime, 60*1000);

                if (ShouldRunTask(schedule, virtTime, false, true)) {
                    task->pendingRunCount ++;
                    elog(LOG, "my_nedo_cron: job %ld is due", task->jobId);
                }
            }

            if (ShouldRunTask(schedule, currentMinute, true, false )) {
                task->pendingRunCount++;
                elog(LOG, "my_nedo_cron: job %ld is due", task->jobId);
            }

            break;
        }


        case CLOCK_JUMP_BACKWARD: {
            if (ShouldRunTask(schedule, currentMinute, true, false )) {
                task->pendingRunCount++;
                elog(LOG, "my_nedo_cron: job %ld is due", task->jobId);
            }
            break;
        }

        default: {
            if (ShouldRunTask(schedule, currentMinute, true, true )) {
                task->pendingRunCount++;
                elog(LOG, "my_nedo_cron: job %ld is due", task->jobId);
            }
            break;
        }


    }
}

void StartPendingTasks(List * taskList, TimestampTz currentTime) {
    static TimestampTz lastMinute = 0;
    int minutesPassed = 0;
    ListCell * taskCell = NULL;
    ClockStatus clock_status;

    if (lastMinute == 0) {
        lastMinute = StartOfMinute(currentTime);
    }

    minutesPassed = MinutesPassed(lastMinute, currentTime);

    //if (minutesPassed == 0) return ;

    if (minutesPassed > 3 * 60) {
        clock_status = CLOCK_CHANGE;
    }
    else if (minutesPassed > 5) {
        clock_status = CLOCK_JUMP_FORWARD;
    }
    else if (minutesPassed > 0) {
        clock_status = CLOCK_PROGRESSED;
    }
    else if (minutesPassed > -3 * 60) {
        clock_status = CLOCK_JUMP_BACKWARD;
    }
    else {
        clock_status = CLOCK_CHANGE;
    }

    foreach(taskCell, taskList) {
        CronTask * task = lfirst(taskCell);
        CronJob* job = getCronJob(task->jobId);

        if (job == NULL) {
            continue;
        }


        if (job->schedule.SECONDS) {
            StartSecondIntervalTask(task, currentTime);
        }
        else if (minutesPassed != 0) {
            StartPendingTask(task, clock_status, lastMinute, currentTime);
        }
    }

    if (clock_status != CLOCK_JUMP_BACKWARD) {
        lastMinute = StartOfMinute(currentTime);
    }

}


static bool ShouldRunTask(CronSchedule *schedule, TimestampTz currentTime, bool floatingHourOrMinute,
              bool fixedHourOrMinute) {
    time_t currentTime_t = timestamptz_to_time_t(currentTime);
    struct tm *tm = gmtime(&currentTime_t);

    if (schedule->SECONDS) return false;

    int minute = tm->tm_min;
    int hour = tm->tm_hour;
    int dayOfMonth = tm->tm_mday;
    int month = tm->tm_mon + 1;
    int dayOfWeek = tm->tm_wday;
    int year = tm->tm_year + 1900; // tm_year в struct tm хранит не полный год, а
                                    // количество лет с 1900 года

    bool dayOfMonthAndDayOfWeek = false;
    bool isLastDOM = isLastDayOfMonth(dayOfMonth, month, year);
    bool dayOfMonthMatches = schedule->dayOfMonth[dayOfMonth] || (schedule->LAST_DOM && isLastDOM);
    bool dayOfWeekMatches = schedule->dayOfWeek[dayOfWeek];

    if (schedule->DOM_STAR || schedule->DOW_STAR) {
        dayOfMonthAndDayOfWeek = dayOfMonthMatches && dayOfWeekMatches;
    }
    else {
        dayOfMonthAndDayOfWeek = dayOfMonthMatches || dayOfWeekMatches;
    }


    if (schedule->minute[minute] && schedule->hour[hour] && schedule->month[month]
    && dayOfMonthAndDayOfWeek) {
        bool isFloating = schedule->MIN_STAR || schedule->HOUR_STAR;
        if ((fixedHourOrMinute && !isFloating) ||
            (floatingHourOrMinute && isFloating)) {
            return true;
        }
    }

    return false;
}


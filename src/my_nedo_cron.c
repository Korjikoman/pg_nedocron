// жалкая копия pg_cron от недоразработчика
// апрель, 2026
// прав нет

#include <stdio.h>
#include <stdbool.h>
#include <time.h>

#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/inval.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include <libpq-fe.h>
#include "libpq/pqsignal.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "tcop/utility.h"
#include "signal.h"
#include <poll.h>
#include <utils/rel.h>

#include "cron_job.h"
#include "catalog/indexing.h"
#include "cron_task.h"
#include "cron_parser.h"
#include  "commands/sequence.h"
#include "postmaster/postmaster.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "server/access/table.h"
#include "access/heapam.h"
#include "cron_parser.h"
#include "utils/syscache.h"
#include "access/genam.h"
#include "utils/fmgroids.h"










#define CRON_SCHEMA_NAME "nedo_cron"
#define JOBS_TABLE_NAME "jobs"
#define CRON_TASK_TIMEOUT_MS 3000
#define JOB_ID_SEQUENCE_NAME "nedo_cron.jobid_seq"
#define JOB_ID_INDEX_NAME "job_pkey"
#define HOW_MUCH_ATTRS_IN_CRON_JOB 7

#define Natts_cron_job 7
#define Att_cron_job_jobid 1
#define Att_cron_job_schedule 2
#define Att_cron_job_command 3
#define Att_cron_job_nodename 4
#define Att_cron_job_nodeport 5
#define Att_cron_job_database 6
#define Att_cron_job_username 7

static char * nodename = "localhost";

bool CronJobCacheValid = false;
static Oid CachedCronJobRealtionId = InvalidOid;

static char *CronTableDatabaseName = "postgres";
HTAB * CronJobHashTable = NULL;
HTAB * CronTaskHashTable = NULL;

static MemoryContext CronJobContext = NULL;
static MemoryContext CronTaskContext = NULL;


PG_MODULE_MAGIC;

void _PG_init(void);
static void PgOctopusWorkerMain(Datum arg);
static void sigtermHandler(SIGNAL_ARGS);
static void sighupHandler(SIGNAL_ARGS);





static void LogTaskResult(char * jobName, int code);
static void InvalidateJobCache(void);



// делаем quick-setup'чик, регаем background worker
void _PG_init() {
    BackgroundWorker worker;
    char * func_name = "PgOctopusWorkerMain";
    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION; // к чему воркер хочет получить доступ
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished; // стартуем, когда система полностью загрузилась, чтобы мы могли всё делать с базой
    worker.bgw_restart_time = BGW_NEVER_RESTART; // не рестартим в случае сбоя
    snprintf(worker.bgw_function_name, BGW_MAXLEN , "%s", func_name); // имя main функции
    worker.bgw_main_arg = Int32GetDatum(0); // тип аргумента в pgOctopusMain
    worker.bgw_notify_pid = 0;
    sprintf(worker.bgw_library_name, "my_nedo_cron");  // где функции искать
    snprintf(worker.bgw_name, sizeof(worker.bgw_name), "my_nedo_cron_sheduler"); // в логах это имя и additional information о воркере будут появляться

    RegisterBackgroundWorker(&worker);
}

// сигнальчик для sigterm
static void sigtermHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    sigtermFlag = true;
    SetLatch(MyLatch); // wake up
    errno = save_errno;
}

// сигнальчик для sighup
static void sighupHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    sighupFlag = true;
    SetLatch(MyLatch); // wake up
    errno = save_errno;
}


static void PgOctopusWorkerMain(Datum arg) {
    MemoryContext cron_state_memory_context = NULL;
    MemoryContext loop_memory_context = NULL;
    MemoryContext cron_job_memory_context = NULL;

    pqsignal(SIGHUP, sighupHandler);
    pqsignal(SIGTERM, sigtermHandler);
    pqsignal(SIGINT, SIG_IGN); // ignored

    BackgroundWorkerUnblockSignals(); // разблокируем сигналы для bgw

    // connecting to db
    BackgroundWorkerInitializeConnection("postgres", NULL, BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ALLOWCONN);

    cron_job_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronJob context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);

    cron_state_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronState context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);


    loop_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronLoop context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);



    MemoryContextSwitchTo(loop_memory_context);

    elog(LOG, "my_nedo_cron started ...");
    while (!sigtermFlag) {
        List* cronStates = NIL;

        AcceptInvalidationMessages();

        if (!CronJobCacheValid) {
            ReloadCronJobs();
            elog(LOG, "jobs reloaded");
        }

        // cronStates = LoadCronStates();
        // TimestampTz currentTime = loadCurrentTime();
        //
        // StartPendingJobs(cronStates, currentTime);
        //
        //
        // WaitForEvent(cronStates);
        // ManageCronStates(cronStates, currentTime);
        //
        // MemoryContextReset(loop_memory_context);


    }
    elog(LOG, "my_nedo_cron exiting ...");

    proc_exit(0);
}

/*
 *    Эта функция только вставляет в postgres таблицу (не в структуру, именно в таблицу)
 *    ОДНУ строку job'а, потом отправляет сообщение воркеру, что нужно обновить
 *    хэш таблицы (refresh_jobs)
 */
Datum schedule_job(PG_FUNCTION_ARGS) {
    text *scheduleText = PG_GETARG_TEXT_P(0);
    text *queryText = PG_GETARG_TEXT_P(0);

    char* schedule = text_to_cstring(scheduleText);
    char* query = text_to_cstring(queryText);
    CronSchedule *parsedSchedule = NULL;
    Oid userId = GetUserId();
    char* username = GetUserNameFromId(userId, false);
    parsedSchedule = parse(schedule);

    Oid cronSchemaId = InvalidOid;
    Oid cronJobsRelationId = InvalidOid;

    Datum jobIdSeqName = 0;
    Datum jobIdDatum = 0;
    int64 jobId = 0;
    if (parsedSchedule == NULL) {
        free(parsedSchedule);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE)), errmsg("invalid schedule: %s", schedule));
    }

    free(parsedSchedule);

    jobIdSeqName = CStringGetDatum(JOB_ID_SEQUENCE_NAME);
    jobIdDatum = DirectFunctionCall1(nextval, jobIdSeqName);
    jobId = DatumGetInt64(jobIdDatum);

    // создаем и заполняем массив values, который потом превратим в строку таблицы и insert'нем её
    Datum values[Natts_cron_job];
    bool isNulls[Natts_cron_job];

    memset(values, 0, sizeof(values));
    memset(isNulls, false, sizeof(isNulls));

    values[Att_cron_job_jobid - 1] = jobIdDatum;
    values[Att_cron_job_schedule - 1] = CStringGetTextDatum(schedule);
    values[Att_cron_job_command - 1] = CStringGetTextDatum(query);
    values[Att_cron_job_nodename - 1] = CStringGetTextDatum(nodename);
    values[Att_cron_job_nodeport - 1] = Int32GetDatum(PostPortNumber);
    values[Att_cron_job_database - 1] = CStringGetTextDatum(CronTableDatabaseName);
    values[Att_cron_job_username - 1] = CStringGetTextDatum(username);

    cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
    cronJobsRelationId = get_relname_relid(JOBS_TABLE_NAME, cronSchemaId);

    Relation cronJobsTable = table_open(cronJobsRelationId, RowExclusiveLock);

    TupleDesc tupleDescriptor = RelationGetDescr(cronJobsTable);
    HeapTuple heap_tuple = heap_form_tuple(tupleDescriptor, values, isNulls); // строка таблицы

    CatalogTupleInsert(cronJobsTable, heap_tuple);
    CommandCounterIncrement();

    table_close(cronJobsTable, RowExclusiveLock);

    InvalidateJobCache();

    PG_RETURN_INT64(jobId);
}

Datum unschedule_job(PG_FUNCTION_ARGS) {

    int64 jobId = PG_GETARG_INT64(0);

    Oid cronSchemaId = InvalidOid;
    Oid cronJobRelationId = InvalidOid;
    Oid cronJobIndexId = InvalidOid;

    Relation cronJobsTable = NULL;
    SysScanDesc scan_desc = NULL;
    ScanKeyData scankey[1];
    int scanKeyCounter = 1;
    bool indexOK = true;
    HeapTuple heap_tuple = NULL;


    cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
    cronJobRelationId = get_relname_relid(JOBS_TABLE_NAME, cronSchemaId);
    cronJobIndexId  = get_relname_relid(JOB_ID_INDEX_NAME, cronSchemaId);

    cronJobsTable = table_open(cronJobRelationId, RowExclusiveLock);

    ScanKeyInit(&scankey[0], Att_cron_job_jobid, BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

    scan_desc = systable_beginscan(cronJobsTable, cronJobIndexId, indexOK, NULL, scanKeyCounter, scankey);

    heap_tuple = systable_getnext(scan_desc);
    if (!HeapTupleIsValid(heap_tuple)) {
        ereport(ERROR, (errmsg("Cannot find current job " UINT64_FORMAT, jobId)));
    }

    simple_heap_delete(cronJobsTable, &heap_tuple->t_self);
    CommandCounterIncrement();

    systable_endscan(scan_desc);
    table_close(cronJobsTable, RowExclusiveLock);

    InvalidateJobCache();

    PG_RETURN_BOOL(true);
}

Oid CronJobRelationId(void) {
    if (CachedCronJobRealtionId == InvalidOid) {
        Oid schema_id = get_namespace_oid(CRON_SCHEMA_NAME, false);
        CachedCronJobRealtionId = get_relname_relid(JOBS_TABLE_NAME, schema_id);
    }

    return CachedCronJobRealtionId;
}

static void InvalidateJobCache(void) {
    HeapTuple tuple = NULL;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(CronJobRelationId()));
    if (HeapTupleIsValid(tuple)) {
        CacheInvalidateRelcacheByTuple(tuple);
        ReleaseSysCache(tuple);
    }
}

List * LoadJobsList() {
    List * jobs = NULL;

}

HTAB * CreateCronJobHashTable() {

}

static void ReloadCronJobs(void) {
    List * jobsList = NULL;
    ListCell * jobCell = NULL;
    CronTask * task = NULL;
    HASH_SEQ_STATUS status;


    MemoryContextResetChildren(CronJobContext);
    CronJobHashTable = CreateCronJobHashTable(); // -------

    hash_seq_init(&status, CronTaskHashTable);

    while ((task = (CronTask *)hash_seq_search(&status)) != NULL) {
        task->isActive = false;
    }

    jobsList = LoadJobsList(); // -----------

    foreach(jobCell, jobsList) {
        CronJob * job = (CronJob *) lfirst(jobCell);

        CronTask *task = getCronTask(job->jobId); // -------
        task->isActive = true;
    }

    CronJobCacheValid = true;


}


// пока тут ничего нет, но скоро она начнет писать логи
static void LogTaskResult(char * jobName, int code) {

}
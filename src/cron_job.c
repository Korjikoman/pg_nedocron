#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "../include/cron_job.h"

#include <access/genam.h>
#include <access/heapam.h>
#include <access/skey.h>
#include <access/table.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <commands/extension.h>
#include <commands/sequence.h>
#include <postmaster/postmaster.h>
#include <utils/fmgroids.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "cron_task.h"

#define NUM_JOB_NAME 1
#define NUM_CRON_STRING 2
#define NUM_NUM_QUERY 3
#define NUM_CONNECTION_STRING 4

static Oid CachedCronJobRealtionId = InvalidOid;
static char *nodename = "localhost";


static void invalidate_job_cache_internal(void);

/* Преобразует строку nedo_cron.jobs в CronJob и кладет ее в job hash table. */
CronJob * TupleToJob(TupleDesc desc, HeapTuple heap_tuple) {
    bool isNull = false;
    bool found = false;
    int64 jobKey = 0;
    CronSchedule * schedule = NULL;
    char * scheduleText = NULL;
    CronJob * job = NULL;
    CronParseError parse_error;

    Datum jobid = heap_getattr(heap_tuple, Att_cron_job_jobid, desc, &isNull);
    Datum schedule_text = heap_getattr(heap_tuple, Att_cron_job_schedule, desc, &isNull);
    Datum command = heap_getattr(heap_tuple, Att_cron_job_command, desc, &isNull);
    Datum nodename = heap_getattr(heap_tuple, Att_cron_job_nodename, desc, &isNull);
    Datum nodeport = heap_getattr(heap_tuple, Att_cron_job_nodeport, desc, &isNull);
    Datum db = heap_getattr(heap_tuple, Att_cron_job_database, desc, &isNull);
    Datum username = heap_getattr(heap_tuple, Att_cron_job_username, desc, &isNull);

    Assert(!HeapTupleHasNulls(heap_tuple));

    jobKey = DatumGetInt64(jobid);

    scheduleText = TextDatumGetCString(schedule_text);


    /* Расписание парсится при загрузке, чтобы worker не держал битые jobs в памяти. */
    schedule = parse_with_error(scheduleText, &parse_error);

    if (schedule == NULL)
    {
        elog(WARNING,
             "my_nedo_cron: invalid schedule for job %ld at field %d near \"%s\": %s",
             jobKey,
             parse_error.fieldIndex,
             parse_error.token,
             parse_error.message);

        return NULL;
    }

    job  = hash_search(CronJobHashTable, &jobKey, HASH_ENTER, &found);

    job->jobId  = DatumGetInt64(jobid);
    job->command = TextDatumGetCString(command);
    job->scheduleText = TextDatumGetCString(schedule_text);
    job->schedule = *schedule;
    job->nodeName = TextDatumGetCString(nodename);
    job->nodePort = DatumGetInt32(nodeport);
    job->database = TextDatumGetCString(db);
    job->userName = TextDatumGetCString(username);

    free_cron_schedule(schedule);

    return job;

}

/* Ищет загруженную job в локальном hash table worker-а. */
CronJob * getCronJob(int64 jobId) {
    CronJob * job = NULL;
    int64 hashKey = jobId;
    bool found = false;

    job = hash_search(CronJobHashTable, &hashKey, HASH_FIND, &found);

    return job;
}


/* Полностью перечитывает nedo_cron.jobs и синхронизирует active-флаги tasks. */
void ReloadCronJobs(void) {
    List * jobsList = NULL;
    ListCell * jobCell = NULL;
    CronTask * task = NULL;
    HASH_SEQ_STATUS status;
    int jobCount = 0;


    /* Все строки jobs живут в отдельном context, поэтому reload можно сделать reset-ом. */
    MemoryContextReset(CronJobContext);
    CronJobHashTable = CreateCronJobHashTable();

    hash_seq_init(&status, CronTaskHashTable);

    /* После reload старые tasks сначала считаются удаленными, потом активные включаются назад. */
    while ((task = (CronTask *)hash_seq_search(&status)) != NULL) {
        task->isActive = false;
    }

    jobsList = LoadJobsList();

    foreach(jobCell, jobsList) {
        CronJob * job = (CronJob *) lfirst(jobCell);

        CronTask *task = getCronTask(job->jobId);
        task->isActive = true;
        jobCount++;
    }

    CronJobCacheValid = true;
    elog(LOG, "my_nedo_cron: loaded %d jobs", jobCount);


}

/* Создает hash table для загруженных CronJob. */
HTAB * CreateCronJobHashTable() {
    HTAB *jobHashTable = NULL;
    HASHCTL hashctl;
    int hashFlags = 0;

    memset(&hashctl, 0, sizeof(hashctl));
    hashctl.keysize = sizeof(int64);
    hashctl.entrysize = sizeof(CronJob);
    hashctl.hash = tag_hash;
    hashctl.hcxt = CronJobContext;
    hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    jobHashTable = hash_create(tabname, 32, &hashctl, hashFlags);
    return jobHashTable;
}


/* Открывает короткую transaction + SPI-сессию для SQL из background worker-а. */
void StartTransaction(void) {
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

}

/* Закрывает SPI-сессию и коммитит короткую transaction worker-а. */
void EndTransaction(void) {
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}


/* Читает все jobs из metadata-таблицы внутри server-side transaction. */
List * LoadJobsList() {
    List * jobs = NULL;
    Relation jobs_table = NULL;
    ScanKeyData key[1];
    TupleDesc tuple_desc = NULL;
    HeapTuple heap_tuple = NULL;
    const int keyCount = 0;

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    /* На recovery или до CREATE EXTENSION metadata-таблицу читать нельзя. */
    if (RecoveryInProgress() || !CronLoaded()) {
        PopActiveSnapshot();
        CommitTransactionCommand();
        pgstat_report_activity(STATE_IDLE, NULL);

        return NIL;
    }

    jobs_table = table_open(CronJobRelationId(), AccessShareLock);
    SysScanDesc scan_desc = systable_beginscan(jobs_table, InvalidOid, false, NULL, keyCount, key);
    tuple_desc = RelationGetDescr(jobs_table);

    heap_tuple  = systable_getnext(scan_desc);
    while (HeapTupleIsValid(heap_tuple)){
        MemoryContext oldContext = NULL;
        CronJob * job = NULL;

        /* Job-структуры должны пережить loop context до следующего reload. */
        oldContext = MemoryContextSwitchTo(CronJobContext);
        job = TupleToJob(tuple_desc, heap_tuple);

        if (job != NULL) {
            jobs = lappend(jobs, job);
        }


        MemoryContextSwitchTo(oldContext);
        heap_tuple  = systable_getnext(scan_desc);

    }

    systable_endscan(scan_desc);
    table_close(jobs_table, AccessShareLock);
    PopActiveSnapshot();
    CommitTransactionCommand();
    pgstat_report_activity(STATE_IDLE, NULL);
    return jobs;

}

/* Проверяет, что extension уже создан и сейчас не идет CREATE EXTENSION. */
bool CronLoaded() {
    Oid extensionOid = get_extension_oid(EXTENSION_NAME, true);
    if (extensionOid != InvalidOid) {
        if (creating_extension && CurrentExtensionObject == extensionOid) {
            return false;
        }
        return true;
    }
    return false;
}

/* Возвращает OID nedo_cron.jobs и кэширует его в процессе worker-а. */
Oid CronJobRelationId(void) {
    if (CachedCronJobRealtionId == InvalidOid) {
        Oid schema_id = get_namespace_oid(CRON_SCHEMA_NAME, false);
        CachedCronJobRealtionId = get_relname_relid(JOBS_TABLE_NAME, schema_id);
    }

    return CachedCronJobRealtionId;
}

/* SQL-функция nedo_cron.check_schedule(text). */
Datum check_schedule(PG_FUNCTION_ARGS) {
    CronSchedule * schedule = NULL;
    text * scheduleText = PG_GETARG_TEXT_P(0);
    char * scheduleChar = text_to_cstring(scheduleText);
    schedule = parse(scheduleChar);
    if (schedule == NULL) {
        PG_RETURN_BOOL(false);
    }
    free_cron_schedule(schedule);
    PG_RETURN_BOOL(true);
}

/* SQL-функция nedo_cron.schedule(text,text): валидирует schedule и вставляет job. */
Datum schedule_job(PG_FUNCTION_ARGS) {
    text *scheduleText = PG_GETARG_TEXT_P(0);
    text *queryText = PG_GETARG_TEXT_P(1);
    CronParseError parseError;
    char* scheduleChar = text_to_cstring(scheduleText);
    char* query = text_to_cstring(queryText);
    CronSchedule *schedule = NULL;
    Oid userId = GetUserId();
    char* username = GetUserNameFromId(userId, false);
    /* Парсим до INSERT, чтобы вернуть пользователю точную ошибку поля/token-а. */
    schedule = parse_with_error(scheduleChar, &parseError);
    char * databaseName = get_database_name(MyDatabaseId);
    int ret;
    Oid argTypes[6] = {TEXTOID, TEXTOID, TEXTOID, INT4OID, TEXTOID, TEXTOID};
    Datum argValues[6];
    char nulls[6] = {' ', ' ', ' ', ' ', ' ', ' '};
    bool isNull = false;
    int64 jobId = 0;

    if (schedule == NULL)
    {
        if (parseError.fieldIndex > 0 && parseError.token[0] != '\0')
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("invalid schedule at field %d near \"%s\": %s",
                            parseError.fieldIndex,
                            parseError.token,
                            parseError.message)));
        }

        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid schedule: %s", parseError.message)));
    }

    free_cron_schedule(schedule);

    argValues[0] = CStringGetTextDatum(scheduleChar);
    argValues[1] = CStringGetTextDatum(query);
    argValues[2] = CStringGetTextDatum(nodename);
    argValues[3] = Int32GetDatum(PostPortNumber);
    argValues[4] = CStringGetTextDatum(databaseName);
    argValues[5] = CStringGetTextDatum(username);

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("could not connect to SPI")));
    }

    /*
     * INSERT делаем через SPI, а не через heap API, чтобы сработали CHECK,
     * defaults, triggers и relcache invalidation.
     */
    ret = SPI_execute_with_args(
        "INSERT INTO nedo_cron.jobs(schedule, command, nodename, nodeport, database, username) "
        "VALUES ($1, $2, $3, $4, $5, $6) "
        "RETURNING jobid",
        6,
        argTypes,
        argValues,
        nulls,
        false,
        0);

    if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1) {
        SPI_finish();
        ereport(ERROR, (errmsg("could not insert row into nedo_cron.jobs")));
    }

    jobId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                        SPI_tuptable->tupdesc,
                                        1,
                                        &isNull));
    if (isNull) {
        SPI_finish();
        ereport(ERROR, (errmsg("inserted nedo_cron.jobs row returned null jobid")));
    }

    SPI_finish();

    PG_RETURN_INT64(jobId);
}

/* SQL-функция nedo_cron.unschedule(bigint): удаляет job через SQL DELETE. */
Datum unschedule_job(PG_FUNCTION_ARGS) {

    int64 jobId = PG_GETARG_INT64(0);
    int ret;
    Oid argTypes[1] = {INT8OID};
    Datum argValues[1] = {Int64GetDatum(jobId)};
    char nulls[1] = {' '};

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("could not connect to SPI")));
    }

    /* DELETE через SPI сохраняет ON DELETE CASCADE для job_run_details. */
    ret = SPI_execute_with_args(
        "DELETE FROM nedo_cron.jobs WHERE jobid = $1 RETURNING jobid",
        1,
        argTypes,
        argValues,
        nulls,
        false,
        0);

    if (ret != SPI_OK_DELETE_RETURNING) {
        SPI_finish();
        ereport(ERROR, (errmsg("could not delete row from nedo_cron.jobs")));
    }

    if (SPI_processed != 1) {
        SPI_finish();
        ereport(ERROR, (errmsg("Cannot find current job " UINT64_FORMAT, jobId)));
    }

    SPI_finish();

    PG_RETURN_BOOL(true);
}

/* Trigger function: просит PostgreSQL разослать relcache invalidation по jobs. */
Datum invalidate_job_cache(PG_FUNCTION_ARGS) {
    if (!CALLED_AS_TRIGGER(fcinfo)) {
        elog(ERROR, "invalidate_job_cache must be called as a trigger");
    }


    invalidate_job_cache_internal();
    PG_RETURN_POINTER(NULL);
}

/* Инвалидирует relcache tuple nedo_cron.jobs для других процессов, включая worker. */
static void invalidate_job_cache_internal(void) {
    HeapTuple tuple = NULL;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(CronJobRelationId()));
    if (HeapTupleIsValid(tuple)) {
        CacheInvalidateRelcacheByTuple(tuple);
        ReleaseSysCache(tuple);
    }
}

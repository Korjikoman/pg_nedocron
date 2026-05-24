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

CronJob * getCronJob(int64 jobId) {
    CronJob * job = NULL;
    int64 hashKey = jobId;
    bool found = false;

    job = hash_search(CronJobHashTable, &hashKey, HASH_FIND, &found);

    return job;
}



void ReloadCronJobs(void) {
    List * jobsList = NULL;
    ListCell * jobCell = NULL;
    CronTask * task = NULL;
    HASH_SEQ_STATUS status;
    int jobCount = 0;


    MemoryContextReset(CronJobContext);
    CronJobHashTable = CreateCronJobHashTable();

    hash_seq_init(&status, CronTaskHashTable);

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


void StartTransaction(void) {
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

}

void EndTransaction(void) {
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}



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

Oid CronJobRelationId(void) {
    if (CachedCronJobRealtionId == InvalidOid) {
        Oid schema_id = get_namespace_oid(CRON_SCHEMA_NAME, false);
        CachedCronJobRealtionId = get_relname_relid(JOBS_TABLE_NAME, schema_id);
    }

    return CachedCronJobRealtionId;
}

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

Datum schedule_job(PG_FUNCTION_ARGS) {
    text *scheduleText = PG_GETARG_TEXT_P(0);
    text *queryText = PG_GETARG_TEXT_P(1);
    CronParseError parseError;
    char* scheduleChar = text_to_cstring(scheduleText);
    char* query = text_to_cstring(queryText);
    CronSchedule *schedule = NULL;
    Oid userId = GetUserId();
    char* username = GetUserNameFromId(userId, false);
    schedule = parse_with_error(scheduleChar, &parseError);
    char * databaseName = get_database_name(MyDatabaseId);

    Oid cronSchemaId = InvalidOid;
    Oid cronJobsRelationId = InvalidOid;

    Datum jobIdSeqName = 0;
    Datum jobIdDatum = 0;
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

    free(schedule);

    jobIdSeqName = CStringGetTextDatum(JOB_ID_SEQUENCE_NAME);
    jobIdDatum = DirectFunctionCall1(nextval, jobIdSeqName);
    jobId = DatumGetInt64(jobIdDatum);

    // создаем и заполняем массив values, который потом превратим в строку таблицы и insert'нем её
    Datum values[Natts_cron_job];
    bool isNulls[Natts_cron_job];

    memset(values, 0, sizeof(values));
    memset(isNulls, false, sizeof(isNulls));

    values[Att_cron_job_jobid - 1] = jobIdDatum;
    values[Att_cron_job_schedule - 1] = CStringGetTextDatum(scheduleChar);
    values[Att_cron_job_command - 1] = CStringGetTextDatum(query);
    values[Att_cron_job_nodename - 1] = CStringGetTextDatum(nodename);
    values[Att_cron_job_nodeport - 1] = Int32GetDatum(PostPortNumber);
    values[Att_cron_job_database - 1] = CStringGetTextDatum(databaseName);
    values[Att_cron_job_username - 1] = CStringGetTextDatum(username);

    cronSchemaId = get_namespace_oid(CRON_SCHEMA_NAME, false);
    cronJobsRelationId = get_relname_relid(JOBS_TABLE_NAME, cronSchemaId);

    Relation cronJobsTable = table_open(cronJobsRelationId, RowExclusiveLock);

    TupleDesc tupleDescriptor = RelationGetDescr(cronJobsTable);
    HeapTuple heap_tuple = heap_form_tuple(tupleDescriptor, values, isNulls); // строка таблицы

    CatalogTupleInsert(cronJobsTable, heap_tuple);
    CommandCounterIncrement();

    table_close(cronJobsTable, RowExclusiveLock);

    invalidate_job_cache_internal();

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

    invalidate_job_cache_internal();

    PG_RETURN_BOOL(true);
}

Datum invalidate_job_cache(PG_FUNCTION_ARGS) {
    if (!CALLED_AS_TRIGGER(fcinfo)) {
        elog(ERROR, "invalidate_job_cache must be called as a trigger");
    }


    invalidate_job_cache_internal();
    PG_RETURN_POINTER(NULL);
}

static void invalidate_job_cache_internal(void) {
    HeapTuple tuple = NULL;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(CronJobRelationId()));
    if (HeapTupleIsValid(tuple)) {
        CacheInvalidateRelcacheByTuple(tuple);
        ReleaseSysCache(tuple);
    }
}

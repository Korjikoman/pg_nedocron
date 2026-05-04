//
// Created by stasyan on 4/12/26.
//

#include "../include/job_metadata.h"


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

#define NUM_JOB_NAME 1
#define NUM_CRON_STRING 2
#define NUM_NUM_QUERY 3
#define NUM_CONNECTION_STRING 4

static void StartTransaction();
static void EndTransaction();
CronJob * TupleToCronJob(HeapTuple tuple, TupleDesc tupleDescriptor);

List* LoadCronJobs(void) {
    List *cronJobList = NULL;
    int spiStatus;
    StringInfoData query;

    MemoryContext memContext = CurrentMemoryContext;
    MemoryContext oldContext = NULL;

    StartTransaction();

    initStringInfo(&query);
    appendStringInfo(&query, "SELECT job_name, cron_string, query, connection_string FROM cron.jobs");
    pgstat_report_activity(STATE_RUNNING, query.data);

    spiStatus = SPI_execute(query.data, false, 0);
    Assert(spiStatus == SPI_OK_SELECT);

    oldContext = MemoryContextSwitchTo(memContext);

    for (uint32 row=0; row < SPI_processed; ++row) {
        HeapTuple tuple = SPI_tuptable->vals[row];
        CronJob *cronJob = TupleToCronJob(tuple, SPI_tuptable->tupdesc);
        cronJobList = lappend(cronJobList, cronJob);
    }

    MemoryContextSwitchTo(oldContext);


    pgstat_report_activity(STATE_IDLE, NULL);

    EndTransaction();
    return cronJobList;
}

// ?
static void StartTransaction(void) {
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

}

static void EndTransaction(void) {
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

CronJob * TupleToCronJob(HeapTuple tuple, TupleDesc tupleDescriptor) {
    CronJob *cronJob = NULL;

    Datum jobNameDatum = SPI_getbinval(tuple, tupleDescriptor, NUM_JOB_NAME, false);
    Datum cronStringDatum = SPI_getbinval(tuple, tupleDescriptor, NUM_CRON_STRING, false);
    Datum queryDatum = SPI_getbinval(tuple, tupleDescriptor, NUM_NUM_QUERY, false);
    Datum connectionDatum = SPI_getbinval(tuple, tupleDescriptor, NUM_CONNECTION_STRING, false);

    cronJob = palloc0(sizeof(CronJob));
    cronJob->jobName = TextDatumGetCString(jobNameDatum);
    cronJob->cronString = TextDatumGetCString(cronStringDatum);
    cronJob->query = TextDatumGetCString(queryDatum);
    cronJob->connectionString = TextDatumGetCString(connectionDatum);
}
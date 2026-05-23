#ifndef MY_EXTENSION_NEDO_CRON_CRON_JOB_H
#define MY_EXTENSION_NEDO_CRON_CRON_JOB_H

#include "postgres.h"
#include "fmgr.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"
#include "cron_parser.h"
#include "datatype/timestamp.h"
#include "common/hashfn.h"
#include "utils/hsearch.h"
#include "miscadmin.h"
#include "utils/lsyscache.h"
#include "commands/dbcommands.h"

#define Natts_cron_job 7
#define Att_cron_job_jobid 1
#define Att_cron_job_schedule 2
#define Att_cron_job_command 3
#define Att_cron_job_nodename 4
#define Att_cron_job_nodeport 5
#define Att_cron_job_database 6
#define Att_cron_job_username 7
#define EXTENSION_NAME "my_nedo_cron"
#define CRON_SCHEMA_NAME "nedo_cron"
#define JOBS_TABLE_NAME "jobs"
#define JOB_ID_SEQUENCE_NAME "nedo_cron.jobid_seq"
#define JOB_ID_INDEX_NAME "jobs_pkey"



extern HTAB *CronJobHashTable;
extern MemoryContext CronJobContext;
extern bool CronJobCacheValid;
extern const char *tabname;

typedef struct CronJob {
    int64 jobId;
    char* scheduleText;
    CronSchedule schedule;
    char * command;
    char * nodeName;
    int nodePort;
    char * database;
    char * userName;

} CronJob;

List *LoadJobsList(void);
HTAB *CreateCronJobHashTable(void);
extern void StartTransaction(void);
extern void EndTransaction(void) ;
void ReloadCronJobs(void);
CronJob * TupleToJob(TupleDesc desc, HeapTuple heap_tuple);
bool CronLoaded(void);
Oid CronJobRelationId(void);
extern PGDLLEXPORT Datum schedule_job(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum unschedule_job(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum invalidate_job_cache(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum check_schedule(PG_FUNCTION_ARGS);
#endif

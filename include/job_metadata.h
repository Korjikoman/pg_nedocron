//
// Created by stasyan on 4/12/26.
//

#ifndef MY_EXTENTSION_JOB_METADATA_H
#define MY_EXTENTSION_JOB_METADATA_H


#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"


typedef struct CronJob {
    char * jobName;
    char *cronString;
    char *query;
    char *connectionString;
} CronJob;

List* LoadCronJobs(void);
static void StartTransaction(void);
static void EndTransaction(void) ;
CronJob * TupleToCronJob(HeapTuple tuple, TupleDesc tupleDescriptor);
#endif
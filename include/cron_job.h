#include "postgres.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"
#include "cron_parser.h"
#include "datatype/timestamp.h"

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

List* LoadCronJobs(void);
extern void StartTransaction(void);
extern void EndTransaction(void) ;
CronJob * TupleToCronJob(HeapTuple tuple, TupleDesc tupleDescriptor);
extern  List* CreateCronTasks(List* cronJobs);

extern void DoCronTasks(List* tasks);

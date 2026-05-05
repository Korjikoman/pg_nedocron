#include "postgres.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/pg_list.h"

#include "datatype/timestamp.h"

typedef struct CronJob {
    char * jobName;
    char *cronString;
    char *query;
    char *connectionString;
} CronJob;

List* LoadCronJobs(void);
extern void StartTransaction(void);
extern void EndTransaction(void) ;
CronJob * TupleToCronJob(HeapTuple tuple, TupleDesc tupleDescriptor);
extern  List* CreateCronTasks(List* cronJobs);

extern void DoCronTasks(List* tasks);

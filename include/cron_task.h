#ifndef CRON_TASK_H
#define CRON_TASK_H

#include "postgres.h"
#include "libpq-fe.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include <signal.h>
#include <utils/hsearch.h>



// флаги для обработчиков
extern volatile sig_atomic_t sighupFlag;
extern volatile sig_atomic_t sigtermFlag;
extern MemoryContext CronTaskContext;

extern HTAB *CronTaskHashTable;

typedef enum CronTaskState
{
    CRON_TASK_WAITING = 0,
    CRON_TASK_START = 1,
    CRON_TASK_CONNECTING = 2,
    CRON_TASK_SENDING = 3,
    CRON_TASK_RUNNING = 4,
    CRON_TASK_RECEIVING = 5,
    CRON_TASK_DONE = 6,
    CRON_TASK_ERROR = 7
} CronTaskState;

typedef struct CronTask
{
    int64 jobId;
    int64 runId;
    CronTaskState state;
    PostgresPollingStatusType polling_status;
    int pendingRunCount;
    TimestampTz startDeadline;
    TimestampTz nextRunTime;
    PGconn * conn;
    char * errorMessage;
    bool isActive;
    bool isSocketReady;
} CronTask;


HTAB *CreateCronTaskHashTable(void);
CronTask * getCronTask(int64 jobId);
extern void ManageCronTask(CronTask *cronState, TimestampTz currentTime);
void WaitForEvent(List *taskList);
void InitCronTask(CronTask *task, int64 jobId);
void ResetCronTaskAfterRun(CronTask *task) ;
void StartSecondIntervalTask(CronTask* task, TimestampTz currentTime);
#endif

#ifndef CRON_TASK_H
#define CRON_TASK_H

#include "postgres.h"

#include "libpq-fe.h"
#include <sys/time.h>
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include <signal.h>
#include <poll.h>
#include "cron_time.h"

typedef struct CronJob CronJob;

// флаги для обработчиков
extern volatile sig_atomic_t sighupFlag;
extern volatile sig_atomic_t sigtermFlag;


typedef enum CronState
{
    CRON_TASK_INITIAL = 0,
    CRON_TASK_CONNECTING = 1,
    CRON_TASK_SENDING = 2,
    CRON_TASK_RUNNING = 3,
    CRON_TASK_RECEIVING = 4,
    CRON_TASK_OK = 5,
    CRON_TASK_ERROR = 6,
    CRON_TASK_WAITING = 7,
    CRON_TASK_DEAD = 8
} CronState;

typedef struct CronTask
{
    CronJob *job;
    CronState state;
    bool readyToPollFlag;
    PGconn *conn;
    PostgresPollingStatusType pollingStatus;
    TimestampTz nextEventTime;
} CronTask;
extern CronTask* CreateCronTask(CronJob *cronJob);
extern List* CreateCronTasks(List* cronJobs);

extern void ManageCronTask(CronTask *cronState, TimestampTz currentTime);
extern int WaitForEvent(List *taskList);
#endif
// жалкая копия pg_cron от недоразработчика
// апрель, 2026
// прав нет

#include <stdio.h>
#include <stdbool.h>
#include <time.h>

#include "postgres.h"
#include "../include/job_metadata.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include <libpq-fe.h>
#include "libpq/pqsignal.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "tcop/utility.h"
#include "signal.h"

PG_MODULE_MAGIC;

typedef enum {
    CRON_TASK_INITIAL = 0,
    CRON_TASK_CONNECTING = 1,
    CRON_TASK_SENDING = 2,
    CRON_TASK_RUNNING = 3,
    CRON_TASK_RECEIVING = 4,
    CRON_TASK_OK = 5,
    CRON_TASK_ERROR = 6,
    CRON_TASK_WAITING = 7
} CronTaskState;

typedef struct CronTask {
    CronJob *job;
    CronTaskState state;
    bool readyToPollFlag;
    PGconn *conn;
    PostgresPollingStatusType pollingStatus;
    struct timeval  nextEventTime;

} CronTask;

void _PG_init(void);
static void PgOctopusWorkerMain(Datum arg);
static List* CreateCronTasks(List* cronJobs);
static CronTask* CreateCronTask(CronJob *cronJob);
static void DoCronTasks(List* tasks);
static void ManageCronTask(CronTask *cronTask, struct timeval currentTime);
static int WaitForEvent(List *taskList);
static int CompareTimes(struct timeval *leftTime, struct timeval *rightTime);
static int SubtractTimes(struct timeval base, struct timeval subtract);
static struct timeval AddTimeMillis(struct timeval base, uint32 additionalMs);

// флаги для обработчиков
static volatile sig_atomic_t sighupFlag = false;
static volatile sig_atomic_t sigtermFlag = false;

// делаем quick-setup'чик, регаем background worker
void _PG_init() {
    BackgroundWorker worker;

}

#include "../include/cron_task.h"

#include <utils/timestamp.h>

#include "cron_job.h"

/* Инициализирует runtime-состояние job в task hash table. */
void InitCronTask(CronTask *task, int64 jobId) {
    task->runId = 0;
    task->jobId = jobId;
    task->state = CRON_TASK_WAITING;
    task->pendingRunCount = 0;
    task->conn = NULL;
    task->isActive = true;
    task->errorMessage = NULL;
    task->isSocketReady = false;
    task->nextRunTime = 0;
    task->runDeadline = 0;
    task->cancelRequested = false;
    task->timedOut = false;
}

/* Сбрасывает состояние task после завершения одного запуска. */
void ResetCronTaskAfterRun(CronTask *task) {
    task->runId = 0;
    task->state = CRON_TASK_WAITING;
    task->polling_status = 0;
    task->startDeadline = 0;
    task->conn = NULL;
    task->errorMessage = NULL;
    task->isSocketReady = false;
    task->runDeadline = 0;
    task->cancelRequested = false;
    task->timedOut = false;
}

/* Планирует следующий запуск для расписания вида "N seconds". */
void StartSecondIntervalTask(CronTask* task, TimestampTz currentTime) {
    CronJob * job = getCronJob(task->jobId);
    CronSchedule* schedule = &job->schedule;
    int intervalMs = 1000 * schedule->secondsInterval;

    if (task->nextRunTime == 0) {
        task->nextRunTime = TimestampTzPlusMilliseconds(currentTime, intervalMs);
    }

    /* pendingRunCount ограничивает запуск одной job одним активным экземпляром. */
    if (TimestampDifferenceExceeds(task->nextRunTime, currentTime, 0)) {
        if (task->pendingRunCount == 0 ) {
            task->pendingRunCount ++;
            elog(LOG, "my_nedo_cron: added pending job=%ld for second interval", task->jobId);
        }

        task->nextRunTime = TimestampTzPlusMilliseconds(currentTime, intervalMs);
    }
}

/* Возвращает существующий task или создает новый для jobId. */
CronTask * getCronTask(int64 jobId) {
    CronTask * task;
    int64 jobKey = 0;
    bool found = false;

    jobKey = DatumGetInt64(jobId);
    task  = hash_search(CronTaskHashTable, &jobKey, HASH_ENTER, &found);

    if (!found) {
        InitCronTask(task, jobId);
    }
    return task;
}

/* Создает hash table runtime-состояний jobs. */
HTAB * CreateCronTaskHashTable() {
    HTAB *taskHashTable = NULL;
    HASHCTL hashctl;
    int hashFlags = 0;

    memset(&hashctl, 0, sizeof(hashctl));
    hashctl.keysize = sizeof(int64);
    hashctl.entrysize = sizeof(CronTask);
    hashctl.hash = tag_hash;
    hashctl.hcxt = CronTaskContext;
    hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    taskHashTable = hash_create(tabname, 32, &hashctl, hashFlags);
    return taskHashTable;

}

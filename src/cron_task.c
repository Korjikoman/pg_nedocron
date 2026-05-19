#include "../include/cron_task.h"
#include "cron_job.h"

void InitCronTask(CronTask *task, int64 jobId) {
    task->runId = 0;
    task->jobId = jobId;
    task->state = CRON_TASK_WAITING;
    task->pendingRunCount = 0;
    task->conn = NULL;
    task->isActive = true;
    task->errorMessage = NULL;
    task->isSocketReady = false;
}


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

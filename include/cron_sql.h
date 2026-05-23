#ifndef MY_EXTENSION_NEDO_CRON_CRON_SQL_H
#define MY_EXTENSION_NEDO_CRON_CRON_SQL_H

#include "cron_task.h"
#include "cron_job.h"
#include "utils/relcache.h"
#include "string.h"
#include "commands/sequence.h"
#include "utils/builtins.h"







#define JOB_RESULT_ID_SEQUENCE_NAME "nedo_cron.run_id_seq"
#define JOB_RUN_DETAILS_TABLE_NAME "job_run_details"

#define JOB_RUN_DETAILS_COLS 9
#define JOB_RUN_DETAILS_RUN_ID 0
#define JOB_RUN_DETAILS_JOB_ID 1
#define JOB_RUN_DETAILS_JOB_PID 2
#define JOB_RUN_DETAILS_DATABASE 3
#define JOB_RUN_DETAILS_USERNAME 4
#define JOB_RUN_DETAILS_STATUS 5
#define JOB_RUN_DETAILS_RETURN_MESSAGE 6
#define JOB_RUN_DETAILS_START_TIME 7
#define JOB_RUN_DETAILS_END_TIME 8


int64 InsertRunDetailsStart(CronTask *task, CronJob *job, TimestampTz startTime);
void UpdateRunDetailsFinish(int64 runId, const char *status, const char *message, TimestampTz endTime);
void UpdatePID(int64 runId, int pid);

#endif //MY_EXTENSION_NEDO_CRON_CRON_SQL_H

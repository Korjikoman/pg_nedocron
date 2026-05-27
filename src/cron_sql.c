//
// Created by stasyan on 5/4/26.
//

#include "../include/cron_sql.h"

#include <access/htup_details.h>
#include <access/table.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <executor/spi.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/timestamp.h>

/*
Как чинить C-код:

4. В `CRON_TASK_RUNNING`, когда все `PQgetResult()` успешно обработаны,
   обновить строку в `results`: `status = 'succeeded'`, `end_time = now`,
   `return_message = 'OK'` или command tag.
5. В `CRON_TASK_ERROR` обновить строку: `status = 'failed'`,
   `return_message = task->errorMessage`.
6. При timeout писать `status = 'timeout'`.
7. Текущий `RunCounter` как источник `runId` убрать или оставить только для
   временной отладки. Run id должен приходить из таблицы.
*/



int64 InsertRunDetailsStart(CronTask *task, CronJob *job, TimestampTz startTime) {
   int ret;
   Oid argTypes[4] = {INT8OID, TEXTOID, TEXTOID, TIMESTAMPTZOID};
   Datum argValues[4];
   char nulls[4] = {' ', ' ', ' ', ' '};
   bool isNull = false;
   int64 runId = 0;

   StartTransaction();

   argValues[0] = Int64GetDatum(job->jobId);
   argValues[1] = CStringGetTextDatum(job->database);
   argValues[2] = CStringGetTextDatum(job->userName);
   argValues[3] = TimestampTzGetDatum(startTime);

   ret = SPI_execute_with_args(
      "INSERT INTO nedo_cron.job_run_details(jobid, database, username, status, start_time) "
      "VALUES ($1, $2, $3, 'starting', $4) "
      "RETURNING run_id",
      4,
      argTypes,
      argValues,
      nulls,
      false,
      0);

   if (ret != SPI_OK_INSERT_RETURNING || SPI_processed != 1) {
      EndTransaction();
      ereport(ERROR, (errmsg("could not insert row into nedo_cron.job_run_details")));
   }

   runId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                       SPI_tuptable->tupdesc,
                                       1,
                                       &isNull));
   if (isNull) {
      EndTransaction();
      ereport(ERROR, (errmsg("inserted nedo_cron.job_run_details row returned null run_id")));
   }

   EndTransaction();

   task->runId = runId;
   return runId;
}

void UpdatePID(int64 runId, int pid) {
   int ret;

   Oid argTypes[2] = {INT4OID, INT8OID};
   Datum argValues[2] = {Int32GetDatum(pid), Int64GetDatum(runId)};
   char nulls[2] = { ' ', ' '};
   if (pid == 0) {
      return;
   }


   StartTransaction();

   ret = SPI_execute_with_args("update nedo_cron.job_run_details  "
                               "set status = 'running', job_pid = $1  "
                               "where run_id = $2;",
      2,
      argTypes,
      argValues,
      nulls,
      false,
      0
      );
   if (ret != SPI_OK_UPDATE) {
      ereport(ERROR, (errmsg("could not update pid in job_run_details row with runId=%ld", runId)));
   }
   if (SPI_processed == 0) {
      elog(WARNING, "job_run_details row with runId=%ld disappeared before pid update", runId);
      EndTransaction();
      return;
   }
   if (SPI_processed != 1) { // в spi_processed лежит кол-во обработанных строк
      ereport(ERROR,
                  (errmsg("updated %lu job_run_details rows for runId=%ld",
                          SPI_processed, runId)));
   }
   EndTransaction();
}

void UpdateRunDetailsFinish(int64 runId, const char *status, const char *message, TimestampTz endTime) {
   int ret;
   Oid argTypes[4] = {TEXTOID, TEXTOID, TIMESTAMPTZOID, INT8OID};
   Datum argValues[4] = {CStringGetTextDatum(status), CStringGetTextDatum(message),
      TimestampTzGetDatum(endTime), Int64GetDatum(runId)
      };
   char nulls[4] = { ' ', ' ', ' ', ' ' };
   if (message == NULL) {
      argValues[1] = (Datum) 0;
      nulls[1] = 'n';
   }


   StartTransaction();

   ret = SPI_execute_with_args("UPDATE nedo_cron.job_run_details "
                               "SET status=$1, return_message=$2, end_time=$3 "
                               "WHERE run_id=$4;",
      4,
      argTypes,
      argValues,
      nulls,
      false,
      0
      );
   if (ret != SPI_OK_UPDATE) {
      ereport(ERROR, (errmsg("could not update job_run_details row with runId=%ld", runId)));
   }
   if (SPI_processed == 0) {
      elog(WARNING, "job_run_details row with runId=%ld disappeared before finish update", runId);
      EndTransaction();
      return;
   }
   if (SPI_processed != 1) { // в spi_processed лежит кол-во обработанных строк
      ereport(ERROR,
                  (errmsg("updated %lu job_run_details rows for runId=%ld",
                          SPI_processed, runId)));
   }
   EndTransaction();
}

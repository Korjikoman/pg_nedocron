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
   Datum resultIdSequenceName = 0;
   Relation job_run_details_table = NULL;
   Datum values[JOB_RUN_DETAILS_COLS];
   bool isNulls[JOB_RUN_DETAILS_COLS];
   Datum runIdDatum = 0;
   int64 runId = 0;
   Oid schemaOid = InvalidOid;
   Oid relationOid = InvalidOid;
   TupleDesc tuple_desc = NULL;
   HeapTuple heap_tuple = NULL;

   StartTransaction();


   resultIdSequenceName = CStringGetTextDatum(JOB_RESULT_ID_SEQUENCE_NAME);
   runIdDatum = DirectFunctionCall1(nextval, resultIdSequenceName);
   runId = DatumGetInt64(runIdDatum);
   memset(values, 0 , sizeof(values));
   memset(isNulls, false, sizeof(isNulls));

   values[JOB_RUN_DETAILS_RUN_ID] = runIdDatum;
   values[JOB_RUN_DETAILS_JOB_ID] = Int64GetDatum(job->jobId);

   isNulls[JOB_RUN_DETAILS_JOB_PID] = true;

   values[JOB_RUN_DETAILS_DATABASE] = CStringGetTextDatum(job->database);
   values[JOB_RUN_DETAILS_USERNAME] = CStringGetTextDatum(job->userName);
   values[JOB_RUN_DETAILS_STATUS] = CStringGetTextDatum("starting");

   isNulls[JOB_RUN_DETAILS_RETURN_MESSAGE] = true;
   values[JOB_RUN_DETAILS_START_TIME] = TimestampTzGetDatum(startTime);

   isNulls[JOB_RUN_DETAILS_END_TIME]=true;

   schemaOid = get_namespace_oid(CRON_SCHEMA_NAME, false);
   relationOid = get_relname_relid(JOB_RUN_DETAILS_TABLE_NAME, schemaOid);

   if (!OidIsValid(relationOid)) {
      ereport(ERROR, (errmsg("could not find nedo_cron.job_run_details")));
   }

   job_run_details_table = table_open(relationOid, RowExclusiveLock);
   tuple_desc = RelationGetDescr(job_run_details_table);

   heap_tuple = heap_form_tuple(tuple_desc, values, isNulls);

   CatalogTupleInsert(job_run_details_table, heap_tuple);
   CommandCounterIncrement();

   heap_freetuple(heap_tuple);
   table_close(job_run_details_table, RowExclusiveLock);

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
   if (SPI_processed != 1) { // в spi_processed лежит кол-во обработанных строк
      ereport(ERROR,
                  (errmsg("updated %lu job_run_details rows for runId=%ld",
                          SPI_processed, runId)));
   }
   EndTransaction();
}
// жалкая копия pg_cron от недоразработчика
// апрель, 2026
// прав нет

#include <stdio.h>
#include <stdbool.h>
#include <time.h>

#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/inval.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include <libpq-fe.h>
#include "libpq/pqsignal.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "tcop/utility.h"
#include "signal.h"
#include <poll.h>
#include "cron_task.h"


#define CRON_TASK_TIMEOUT_MS 3000


bool CronJobCacheValid = false;

HTAB * CronJobHashTable = NULL;
HTAB * CronStateHashTable = NULL;


PG_MODULE_MAGIC;

void _PG_init(void);
static void PgOctopusWorkerMain(Datum arg);
static void sigtermHandler(SIGNAL_ARGS);
static void sighupHandler(SIGNAL_ARGS);





static void LogTaskResult(char * jobName, int code);




// делаем quick-setup'чик, регаем background worker
void _PG_init() {
    BackgroundWorker worker;
    char * func_name = "PgOctopusWorkerMain";
    if (!process_shared_preload_libraries_in_progress) {
        return;
    }

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION; // к чему воркер хочет получить доступ
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished; // стартуем, когда система полностью загрузилась, чтобы мы могли всё делать с базой
    worker.bgw_restart_time = BGW_NEVER_RESTART; // не рестартим в случае сбоя
    snprintf(worker.bgw_function_name, BGW_MAXLEN , "%s", func_name); // имя main функции
    worker.bgw_main_arg = Int32GetDatum(0); // тип аргумента в pgOctopusMain
    worker.bgw_notify_pid = 0;
    sprintf(worker.bgw_library_name, "my_nedo_cron");  // где функции искать
    snprintf(worker.bgw_name, sizeof(worker.bgw_name), "my_nedo_cron_sheduler"); // в логах это имя и additional information о воркере будут появляться

    RegisterBackgroundWorker(&worker);
}

// сигнальчик для sigterm
static void sigtermHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    sigtermFlag = true;
    SetLatch(MyLatch); // wake up
    errno = save_errno;
}

// сигнальчик для sighup
static void sighupHandler(SIGNAL_ARGS) {
    int save_errno = errno;
    sighupFlag = true;
    SetLatch(MyLatch); // wake up
    errno = save_errno;
}


static void PgOctopusWorkerMain(Datum arg) {
    MemoryContext cron_state_memory_context = NULL;
    MemoryContext loop_memory_context = NULL;
    MemoryContext cron_job_memory_context = NULL;

    pqsignal(SIGHUP, sighupHandler);
    pqsignal(SIGTERM, sigtermHandler);
    pqsignal(SIGINT, SIG_IGN); // ignored

    BackgroundWorkerUnblockSignals(); // разблокируем сигналы для bgw

    // connecting to db
    BackgroundWorkerInitializeConnection("postgres", NULL, BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ALLOWCONN);

    cron_job_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronJob context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);

    cron_state_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronState context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);


    loop_memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronLoop context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);



    MemoryContextSwitchTo(loop_memory_context);

    elog(LOG, "my_nedo_cron started ...");
    // while (!sigtermFlag) {
    //     List* cronStates = NIL;
    //
    //     AcceptInvalidationMessages();
    //
    //     if (!CronJobCacheValid) {
    //         ReloadCronJobs();
    //     }
    //
    //     cronStates = LoadCronStates();
    //     TimestampTz currentTime = loadCurrentTime();
    //
    //     StartPendingJobs(cronStates, currentTime);
    //
    //
    //     WaitForEvent(cronStates);
    //     ManageCronStates(cronStates, currentTime);
    //
    //     MemoryContextReset(loop_memory_context);
    //
    //
    // }
    // elog(LOG, "my_nedo_cron exiting ...");
    //
    // proc_exit(0);
}





// пока тут ничего нет, но скоро она начнет писать логи
static void LogTaskResult(char * jobName, int code) {

}
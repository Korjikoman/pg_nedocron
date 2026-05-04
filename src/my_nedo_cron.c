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
#include <poll.h>

#define CRON_TASK_TIMEOUT_MS 3000

PG_MODULE_MAGIC;

typedef enum {
    CRON_TASK_INITIAL = 0,
    CRON_TASK_CONNECTING = 1,
    CRON_TASK_SENDING = 2,
    CRON_TASK_RUNNING = 3,
    CRON_TASK_RECEIVING = 4,
    CRON_TASK_OK = 5,
    CRON_TASK_ERROR = 6,
    CRON_TASK_WAITING = 7,
    CRON_TASK_DEAD = 8
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
static void LogTaskResult(char * jobName, int code);


// флаги для обработчиков
static volatile sig_atomic_t sighupFlag = false;
static volatile sig_atomic_t sigtermFlag = false;

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
    MemoryContext memory_context = NULL;
    pqsignal(SIGHUP, sighupHandler);
    pqsignal(SIGTERM, sigtermHandler);
    pqsignal(SIGINT, SIG_IGN); // ignored

    BackgroundWorkerUnblockSignals(); // разблокируем сигналы для bgw

    // connecting to db
    BackgroundWorkerInitializeConnection("postgres", NULL, BGWORKER_BYPASS_ALLOWCONN | BGWORKER_BYPASS_ALLOWCONN);

    memory_context = AllocSetContextCreate(CurrentMemoryContext,
                                               "CronTask context",
                                               ALLOCSET_DEFAULT_MINSIZE,
                                               ALLOCSET_DEFAULT_INITSIZE,
                                               ALLOCSET_DEFAULT_MAXSIZE);

    MemoryContextSwitchTo(memory_context);

    elog(LOG, "my_nedo_cron started ...");
    while (!sigtermFlag) {
        List* cronJobs = NIL;
        List* cronTasks = NIL;

        cronJobs = LoadCronJobs();
        cronTasks = CreateCronTasks(cronJobs);

        DoCronTasks(cronTasks);

        MemoryContextReset(memory_context);

        if (sighupFlag) {
            sighupFlag = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
    }
    elog(LOG, "my_nedo_cron exiting ...");

    proc_exit(0);
}


static List* CreateCronTasks(List* cronJobs) {
    List* cronTasks = NULL;
    ListCell *cronJobCell = NULL;
    foreach(cronJobCell, cronJobs) {
        CronJob* cronJob = lfirst(cronJobCell);
        CronTask *cronTask = CreateCronTask(cronJob);
        cronTasks = lappend(cronTasks, cronTask);

    }

    return cronTasks;
}

static CronTask* CreateCronTask(CronJob *cronJob) {
    CronTask * task = NULL;

    struct timeval fakeTime = {0,0};

    task = palloc0(sizeof(CronTask));
    task->conn = NULL;
    task->job = cronJob;
    task->state = CRON_TASK_INITIAL;
    task->nextEventTime  = fakeTime;

    return task;
}

static void DoCronTasks(List* tasks) {
    int pendingTasksCounter;
    while (!sigtermFlag) {
        pendingTasksCounter = 0;
        ListCell * taskCell = NULL;
        struct timeval currentTime = {0, 0};
        foreach (taskCell, tasks) {
            CronTask *cronTask = (CronTask *) lfirst(taskCell);

            ManageCronTask(cronTask, currentTime);

            if (cronTask->state != CRON_TASK_OK && cronTask->state != CRON_TASK_DEAD) {
                pendingTasksCounter ++;
            }
        }

        if (pendingTasksCounter == 0) {
            break;
        }

        WaitForEvent(tasks);
    }
}


static void ManageCronTask(CronTask *cronTask, struct timeval currentTime) {
    CronJob *cronJob = cronTask->job;
    CronTaskState taskState = cronTask->state;

    switch (taskState) {
        case CRON_TASK_INITIAL: {
            /* девственная задача ещё не начала свое грязное выполнение */
            PGconn *connection = NULL;
            ConnStatusType connStatus = CONNECTION_BAD;

            connection = PQconnectStart(cronJob->connectionString);
            connStatus = PQstatus(connection);

            if (connStatus == CONNECTION_BAD) {
                PQfinish(connection);
                LogTaskResult(cronJob->jobName, 1);
                cronTask->conn = NULL;
                cronTask->pollingStatus = PGRES_POLLING_FAILED;

            }
            else {
                cronTask->conn = connection;
                cronTask->pollingStatus=PGRES_POLLING_WRITING;
                cronTask->state = CRON_TASK_CONNECTING;
                struct timeval timeout = {0,0};
                timeout = AddTimeMillis(currentTime, CRON_TASK_TIMEOUT_MS);

            }
            break;
        }



        case CRON_TASK_CONNECTING: {
            /* этап установления соединения */
            PGconn *connection = cronTask->conn;
            PostgresPollingStatusType pollingStatus = PGRES_POLLING_FAILED;

            if (CompareTimes( &cronTask->nextEventTime, &currentTime) < 0 ) {
                PQfinish(connection);
                LogTaskResult(cronJob->jobName, 1);
                cronTask->conn = NULL;
                cronTask->pollingStatus = pollingStatus;
                cronTask->state = CRON_TASK_DEAD;
                break;
            }
            if (!cronTask->readyToPollFlag) { break; }

            pollingStatus = PQconnectPoll(connection);
            if (pollingStatus == PGRES_POLLING_FAILED) {
                PQfinish(connection);
                LogTaskResult(cronJob->jobName, 1);
                cronTask->conn = NULL;
                cronTask->pollingStatus = pollingStatus;
                cronTask->state = CRON_TASK_DEAD;
            }
            else if (pollingStatus == PGRES_POLLING_OK) {
                PQfinish(connection);
                LogTaskResult(cronJob->jobName, 0);
                cronTask->conn = NULL;
                cronTask->pollingStatus = pollingStatus;
                cronTask->state = CRON_TASK_OK;
            }
            else {
                /* всё ещё подключен */

                cronTask->state = CRON_TASK_SENDING;
                cronTask->conn = connection;
                cronTask->pollingStatus=PGRES_POLLING_WRITING;

            }
            cronTask->pollingStatus=pollingStatus;
            break;

        }
        case CRON_TASK_SENDING: {
            /* этап отправки данных в базу */

        }
        case CRON_TASK_RUNNING: {
            /* задание выполняется */
        }
        case CRON_TASK_RECEIVING: {
            /* получаем результаты */
        }

        case CRON_TASK_DEAD: {
            /* похуй проебали */
        }

        case CRON_TASK_OK: {
            /* всё чики пуки */
        }

        default: {
            /* тут таска завершилась, надо обработать завершение */
        }


    }
}

static int WaitForEvent(List *taskList) {
    ListCell * taskCell = NULL;
    int taskCount = list_length(taskList);
    int taskIndex = 0;
    struct pollfd *pollfdList = NULL;
    struct timeval currentTime = {0, 0};
    struct timeval nextEventTime = {0, 0};
    pollfdList = (struct  pollfd *) palloc0(taskCount * sizeof(struct pollfd));
    
    foreach (taskCell, taskList) {
        CronTask * cronTask = (CronTask *) lfirst(taskCell);
        struct pollfd *pollfd = NULL; // тут хранятся дескрипторы

        if (cronTask->state == CRON_TASK_WAITING) {
            bool hasNextTime = cronTask->nextEventTime.tv_sec !=0;
            if (hasNextTime &&
                (nextEventTime.tv_sec == 0 || CompareTimes(&cronTask->nextEventTime, &nextEventTime) < 0)) {
                nextEventTime = cronTask->nextEventTime;
            }
        }

        if (cronTask->state == CRON_TASK_CONNECTING ||
            cronTask->state == CRON_TASK_SENDING ||
            cronTask->state == CRON_TASK_RUNNING ||
            cronTask->state == CRON_TASK_RECEIVING
            ) {
            PGconn *connection = cronTask->conn;
            int pollEvent = 0;

            if (cronTask->pollingStatus == PGRES_POLLING_READING) {
                pollEvent = POLLERR | POLLIN ;

            }else if (cronTask->pollingStatus == PGRES_POLLING_WRITING) {
                pollEvent = POLLERR | POLLOUT ;

            }
            pollfd->fd = PQsocket(connection); // Use PQsocket(conn)
                                                // to obtain the descriptor of the
                                                // socket underlying the
                                                // database connection
            pollfd->events = pollEvent;

        }
        else {
            pollfd->fd = -1;
            pollfd->events= 0;
        }
        pollfd->revents=0;
        taskIndex++;

    }
    return 0;
}

static int CompareTimes(struct timeval *leftTime, struct timeval *rightTime) {
    int result = 0;
    if (leftTime->tv_sec < rightTime->tv_sec) {
        return -1;
    }
    else if (leftTime->tv_sec > rightTime->tv_sec) {
        return 1;
    }
    else if (leftTime->tv_usec < rightTime->tv_usec) {
        return -1;
    }
    else if (leftTime->tv_usec > rightTime->tv_usec) {
        return 1;
    }
    else {
        result = 0;
    }
    return result;
}

static int SubtractTimes(struct timeval x, struct timeval y) {
    int diffMS = 0;
    if (x.tv_usec < y.tv_usec) {
        int microsec = (y.tv_sec - x.tv_sec) * 1000000 + 1;
        y.tv_usec -= 1000000 * microsec;
        y.tv_sec += microsec;
    }
    if (x.tv_usec - y.tv_usec > 1000000) {
        int sec = (x.tv_usec - y.tv_usec) / 1000000;
        y.tv_usec += 1000000 * sec;
        y.tv_sec -= sec;

    }
    diffMS += 1000 * (x.tv_sec - y.tv_sec);
    diffMS += (x.tv_usec - y.tv_usec) / 1000;
    return diffMS;
}

static struct timeval AddTimeMillis(struct timeval base, uint32 additionalMs) {
    struct timeval res = {0, 0};
    res.tv_sec = base.tv_sec + additionalMs/1000;
    res.tv_usec = base.tv_usec + (additionalMs % 1000) * 1000;
    return res;
}

// пока тут ничего нет, но скоро она начнет писать логи
static void LogTaskResult(char * jobName, int code) {

}
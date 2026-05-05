
#include "../include/cron_task.h"
#include "cron_job.h"

List* CreateCronTasks(List* cronJobs) {
    List* cronTasks = NULL;
    ListCell *cronJobCell = NULL;
    foreach(cronJobCell, cronJobs) {
        CronJob* cronJob = lfirst(cronJobCell);
        CronTask *cronTask = CreateCronTask(cronJob);
        cronTasks = lappend(cronTasks, cronTask);

    }

    return cronTasks;
}

CronTask* CreateCronTask(CronJob *cronJob) {
    CronTask * task = NULL;


    task = palloc0(sizeof(CronTask));
    task->conn = NULL;
    task->job = cronJob;
    task->state = CRON_TASK_INITIAL;
    task->nextEventTime  = 0;

    return task;
}


void DoCronTasks(List* tasks) {
    int pendingTasksCounter;
    while (!sigtermFlag) {
        pendingTasksCounter = 0;
        ListCell * taskCell = NULL;
        TimestampTz currentTime;
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


int WaitForEvent(List *taskList) {
    ListCell * taskCell = NULL;
    int taskCount = list_length(taskList);
    int taskIndex = 0;
    struct pollfd *pollfdList = NULL;
    TimestampTz currentTime = 0;
    TimestampTz nextEventTime = 0;
    pollfdList = (struct  pollfd *) palloc0(taskCount * sizeof(struct pollfd));

    foreach (taskCell, taskList) {
        CronTask * cronTask = (CronTask *) lfirst(taskCell);
        struct pollfd *pollfd = NULL; // тут хранятся дескрипторы

        if (cronTask->state == CRON_TASK_WAITING) {
            bool hasNextTime = cronTask->nextEventTime !=0;
            if (hasNextTime &&
                (nextEventTime == 0 || CompareTimes(&cronTask->nextEventTime, &nextEventTime) < 0)) {
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

void ManageCronTask(CronTask *cronTask, TimestampTz currentTime) {
    CronJob *cronJob = cronTask->job;
    CronState taskState = cronTask->state;

    switch (taskState) {
        case CRON_TASK_INITIAL: {
            /* девственная задача ещё не начала свое грязное выполнение */
            PGconn *connection = NULL;
            ConnStatusType connStatus = CONNECTION_BAD;

            connection = PQconnectStart(cronJob->connectionString);
            connStatus = PQstatus(connection);

            if (connStatus == CONNECTION_BAD) {
                PQfinish(connection);
                //LogTaskResult(cronJob->jobName, 1);
                cronTask->conn = NULL;
                cronTask->pollingStatus = PGRES_POLLING_FAILED;

            }
            else {
                cronTask->conn = connection;
                cronTask->pollingStatus=PGRES_POLLING_WRITING;
                cronTask->state = CRON_TASK_CONNECTING;
                struct timeval timeout = {0,0};
                //timeout = AddTimeMillis(currentTime, CRON_TASK_TIMEOUT_MS);

            }
            break;
        }



        case CRON_TASK_CONNECTING: {
            /* этап установления соединения */
            PGconn *connection = cronTask->conn;
            PostgresPollingStatusType pollingStatus = PGRES_POLLING_FAILED;

            if (CompareTimes( &cronTask->nextEventTime, &currentTime) < 0 ) {
                PQfinish(connection);
                //LogTaskResult(cronJob->jobName, 1);
                cronTask->conn = NULL;
                cronTask->pollingStatus = pollingStatus;
                cronTask->state = CRON_TASK_DEAD;
                break;
            }
            if (!cronTask->readyToPollFlag) { break; }

            pollingStatus = PQconnectPoll(connection);
            if (pollingStatus == PGRES_POLLING_FAILED) {
                PQfinish(connection);
                //LogTaskResult(cronJob->jobName, 1);
                cronTask->conn = NULL;
                cronTask->pollingStatus = pollingStatus;
                cronTask->state = CRON_TASK_DEAD;
            }
            else if (pollingStatus == PGRES_POLLING_OK) {
                PQfinish(connection);
                //LogTaskResult(cronJob->jobName, 0);
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
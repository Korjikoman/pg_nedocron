//
// Created by stasyan on 4/12/26.
//

#ifndef MY_EXTENTSION_JOB_METADATA_H
#define MY_EXTENTSION_JOB_METADATA_H

#endif //MY_EXTENTSION_JOB_METADATA_H


typedef struct CronJob {
    char * jobName;
    char *cronString;
    char *query;
    char *connectionString;
} CronJob;
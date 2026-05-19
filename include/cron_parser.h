#ifndef MY_EXTENSION_NEDO_CRON_CRON_PARSER_H
#define MY_EXTENSION_NEDO_CRON_CRON_PARSER_H

#include <assert.h>
#include <ctype.h>
#include <errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>

typedef enum FieldType {
    CRON_MINUTE = 0,
    CRON_HOUR = 1,
    CRON_DOM = 2,
    CRON_M = 3,
    CRON_DOW = 4
} FieldType;

typedef struct CronSchedule {
    bool minute[60];
    bool hour[24];
    bool dayOfMonth[32];
    bool month[13];
    bool dayOfWeek[7];

    bool MIN_STAR;
    bool HOUR_STAR;
    bool DOW_STAR;
    bool DOM_STAR;
} CronSchedule;



bool parse_integer(char * string, int * resultInt);
int parse_field_item(CronSchedule* scheduler, char * item, FieldType type);
int parse_cron_field(CronSchedule* scheduler, char * field, FieldType type);
bool parse_range(char* string, int* start, int* finish);
CronSchedule* parse(char * command);

void free_cron_schedule(CronSchedule * schedule);
void free_string_array(char** array);
bool set_field_value(CronSchedule* scheduler, FieldType type, int value);
int charArrayLength(char ** charArray);
char ** str_split(char * a_str, const char a_delim);
void fillBooleanArray(bool *array, int start, int finish, int step, bool value);
bool isStar(const char * string);
bool isEnumeration(char * string);
bool set_field_range(CronSchedule * scheduler, FieldType type, int start, int finish, int step);
bool get_bounds(int *min_value, int* max_value, FieldType type);
bool is_leap_year(int year);
int days_in_month(int year, int month);

#endif //MY_EXTENSION_NEDO_CRON_CRON_PARSER_H

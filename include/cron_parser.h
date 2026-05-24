#ifndef MY_EXTENSION_NEDO_CRON_CRON_PARSER_H
#define MY_EXTENSION_NEDO_CRON_CRON_PARSER_H

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include "postgres.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <limits.h>
#include "port.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>






#define CRON_PARSE_ERROR_MESSAGE_LEN 256
#define CRON_PARSE_ERROR_TOKEN_LEN 64


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
    bool LAST_DOM;
    bool SECONDS;
    int secondsInterval;
} CronSchedule;

typedef struct CronName {
    const char * name;
    int value;
}CronName;


typedef struct CronParseError {
    char message[CRON_PARSE_ERROR_MESSAGE_LEN];
    char token[CRON_PARSE_ERROR_TOKEN_LEN];
    int fieldIndex;
    int tokenIndex;
}CronParseError;

static const CronName seconds_names[] = {
    {"JAN", 1}, {"FEB", 2}, {"MAR", 3}, {"APR", 4},
  {"MAY", 5}, {"JUN", 6}, {"JUL", 7}, {"AUG", 8},
  {"SEP", 9}, {"OCT", 10}, {"NOV", 11}, {"DEC", 12},
  {NULL, 0}
};


static const CronName month_names[] = {
    {"JAN", 1}, {"FEB", 2}, {"MAR", 3}, {"APR", 4},
  {"MAY", 5}, {"JUN", 6}, {"JUL", 7}, {"AUG", 8},
  {"SEP", 9}, {"OCT", 10}, {"NOV", 11}, {"DEC", 12},
  {NULL, 0}
};


static const CronName dow_names[] = {
    {"SUN", 0}, {"MON", 1}, {"TUE", 2}, {"WED", 3},
      {"THU", 4}, {"FRI", 5}, {"SAT", 6},
      {NULL, 0}
};


CronSchedule* parse(char * command);
CronSchedule *parse_with_error(char *command, CronParseError *error);

int parse_cron_field(CronSchedule* scheduler, char *field, FieldType type, int fieldIndex, CronParseError* error);
int parse_field_item(CronSchedule* scheduler, char * item, FieldType type, int fieldIndex,
    int tokenIndex, CronParseError *error);

bool parse_integer(char * string, int * resultInt);
bool parse_range(CronSchedule * scheduler, char* string, FieldType type, int * start, int* finish);
bool parse_field_value(CronSchedule* scheduler, FieldType type, char * token, int *value);

bool set_field_value(CronSchedule* scheduler, FieldType type, int value, int fieldIndex,
    int tokenIndex, const char * token, CronParseError * error);
void free_cron_schedule(CronSchedule * schedule);
void free_string_array(char** array);
int charArrayLength(char ** charArray);
char ** str_split(char * a_str, const char a_delim);
void fillBooleanArray(bool *array, int start, int finish, int step, bool value);
bool isStar(const char * string);
bool isEnumeration(char * string);
bool set_field_range(CronSchedule * scheduler, FieldType type, int start, int finish, int step,
    int fieldIndex,
    int tokenIndex,
    const char *token,
    CronParseError *error);
bool get_bounds(int *min_value, int* max_value, FieldType type);
bool is_leap_year(int year);
int days_in_month(int year, int month);
bool isLastDayOfMonth(int current_dom, int current_month, int current_year);

#endif //MY_EXTENSION_NEDO_CRON_CRON_PARSER_H

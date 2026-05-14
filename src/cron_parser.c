#include "../include/cron_parser.h"


#define ARRAY_OF_NUMBERS_DELIMITER ','
#define RANGE_OF_NUMBERS_DELIMITER '-'
#define SPACE_DELIMITER ' '
#define STAR "*"
#define STEP_DELIMITER '/'


bool is_leap_year(int year) {
    if (year % 400 == 0) return true;

    if (year % 100 == 0) return false;
    return year % 4 == 0;
}

int days_in_month(int year, int month) {
    static const int days[] = {
        0,
        31,
        28,
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31
    };

    if (month < 1 || month > 12) {
        return 0;
    }
    if (month == 2 && is_leap_year(year)) {
        return 29;
    }
    return days[month];
}

void free_string_array(char** array) {
    if (array == NULL) return;
    for (size_t i =0; array[i] != NULL; i++) {
        free(array[i]);
    }
    free(array);
}


int charArrayLength(char ** charArray) {
    if (!charArray) return 0;
    int counter = 0;
    while (charArray[counter] != NULL) {
        counter++;
    }
    return counter;
}

char ** str_split(char * a_str, const char a_delim) {

    if (a_str == NULL)
    {
        return NULL;
    }

    char ** result = 0;
    char * tmp = a_str;
    char * last_delim = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    char *copy = strdup(a_str);
    if (copy == NULL) {
        return NULL;
    }
    size_t count = 0;
    char *token= strtok(copy, delim);

    while (token) {
        count ++;
        token= strtok(NULL, delim);
    }
    free(copy);

    result = malloc(sizeof(char *) * (count+1));

    if (result == NULL) return NULL;

    copy = strdup(a_str);
    if (copy == NULL) {
        free(result);
        return NULL;
    }


    size_t idx = 0;
    token = strtok(copy, delim);

    while (token) {
        result[idx] = strdup(token);

        if (result[idx] == NULL) {
            for (size_t i = 0; i < idx; i++) {
                free(result[i]);
            }
            free(result);
            free(copy);
            return NULL;
        }

        idx++;
        token = strtok(NULL, delim);
    }

    result[idx] = 0;


    free(copy);
    return result;
}

void fillBooleanArray(bool *array, int start, int finish, int step, bool value)
{
    if (array == NULL)
    {
        return;
    }

    if (step <= 0)
    {
        return;
    }

    if (start < 0){array[0] = value;}
    for (int i = start; i < finish; i += step)
    {
        array[i] = value;
    }
}


bool parse_integer(char * string, int * resultInt) {
    char * endpointer;
    errno = 0;
    long result;
    if (string == NULL || *string == '\0') {
        return false;
    }

    for (const unsigned char *p = (const unsigned char *)string; *p != '\0'; p++) {
        if (!isdigit(*p)) {
            return false;
        }
    }

    result = strtol(string, &endpointer, 10);

    if (errno == ERANGE || *endpointer != '\0' || result > INT_MAX) {
        return false;
    }
    *resultInt = (int) result;

    return true;
}

bool isStar(const char * string) {
    return strcmp(string, STAR) == 0;
}



bool parse_range(char* string, int* start, int* finish) {

    if (!string || !start || !finish) return false;
    char ** splittedString = str_split(string, RANGE_OF_NUMBERS_DELIMITER);
    if (!splittedString) return false;


    if (charArrayLength(splittedString) != 2) {
        free_string_array(splittedString);
        return false;
    }

    if (!parse_integer(splittedString[0], start) || !parse_integer(splittedString[1], finish) ) {
        free_string_array(splittedString);
        return false;

    }
    free_string_array(splittedString);
    return true;
}

bool isEnumeration(char * string) {
    char ** splitted = str_split(string, ARRAY_OF_NUMBERS_DELIMITER);
    if (!splitted) return false;
    if (charArrayLength(splitted) < 2) {
        free_string_array(splitted);
        return false;
    }
    free_string_array(splitted);
    return true;
}

bool set_field_range(CronSchedule * scheduler, FieldType type, int start, int finish, int step) {

    if (!scheduler) return false;
    if (step <= 0) return false;
    if (start> finish) return false;

    for (int value=start; value <= finish; value+= step) {
        if (!set_field_value(scheduler, type, value)) {
            return false;
        }
    }
    return true;
}


bool get_bounds(int *min_value, int* max_value, FieldType type) {
    if (min_value == NULL || max_value == NULL) {
        return false;
    }

    switch (type) {
        case CRON_MINUTE:
            *min_value = 0;
            *max_value = 59;
            return true;
        case CRON_HOUR:
            *min_value = 0;
            *max_value = 23;
            return true;
        case CRON_M:
            *min_value = 1;
            *max_value = 12;
            return true;
        case CRON_DOM:
            *min_value = 1;
            *max_value = 31;
            return true;
        case CRON_DOW:
            *min_value = 1;
            *max_value = 7;
            return true;
    }
    return false;

}

bool set_field_value(CronSchedule* scheduler, FieldType type, int value) {

    int min_value;
    int max_value;

    if (!scheduler) return false;

    if (!get_bounds(&min_value, &max_value, type)) {
        return false;
    }

    if (value < min_value || value > max_value) return false;

    switch (type) {
        case CRON_MINUTE:
            scheduler->minute[value] = true;
            return true;
        case CRON_HOUR:
            scheduler->hour[value] = true;
            return true;
        case CRON_M:
            scheduler->month[value] = true;
            return true;
        case CRON_DOM:
            scheduler->dayOfMonth[value] = true;
            return true;
        case CRON_DOW:
            if (value == 7) {
                value = 0;
            }
            scheduler->dayOfWeek[value] = true;
            return true;
    }
    return false;
}

int parse_field_item(CronSchedule* scheduler, char * item, FieldType type) {

    int min_value;
    int max_value;
    int number;
    int start;
    int finish;
    int step;


    if (scheduler == NULL || item == NULL || *item == '\0') return false;

    if (!get_bounds(&min_value, &max_value, type)) return false;

    if (isStar(item)) {

        return set_field_range(scheduler, type, min_value, max_value, 1);

    }

    if (strchr(item, STEP_DELIMITER) != NULL) {
        char ** step_parts = str_split(item, STEP_DELIMITER);
        if (step_parts == NULL) return false;

        if (charArrayLength(step_parts) != 2) {
            free_string_array(step_parts);
            return false;

        }

        if (!parse_integer(step_parts[1], &step) || step <= 0){
            free_string_array(step_parts);
            return false;

        }

        if (isStar(step_parts[0])) {
            bool ok;

            ok = set_field_range(scheduler, type, min_value, max_value, step);
            free_string_array(step_parts);
            return ok;
        }

        if (strchr(step_parts[0], RANGE_OF_NUMBERS_DELIMITER) != NULL) {
            bool ok;

            ok = parse_range(step_parts[0], &start, &finish) && set_field_range(scheduler, type, start, finish, step);
            free_string_array(step_parts);
            return ok;
        }
        free_string_array(step_parts);
        return false;

    }

    if (strchr(item, RANGE_OF_NUMBERS_DELIMITER) != NULL) {

        return parse_range(item, &start, &finish) && set_field_range(scheduler, type, start, finish, 1);
    }

    if (parse_integer(item, &number)) {
        return set_field_value(scheduler, type, number);
    }

    return false;


}

int parse_cron_field(CronSchedule* scheduler, char *field, FieldType type ) {
    char **items;
    int count;

    items = str_split(field, ARRAY_OF_NUMBERS_DELIMITER);
    if (items == NULL) return 0;


    count = charArrayLength(items);

    if (count ==0) {
        free_string_array(items);
        return 0;
    }
    for (int i=0; i < count; i++) {
        if (!parse_field_item(scheduler, items[i], type)) {
            free_string_array(items);
            return 0;
        }
    }

    free_string_array(items);
    return 1;

}

CronSchedule* parse(char * command) {
    CronSchedule* scheduler = {0};

    if (command == NULL) return NULL;
    CronSchedule temp = {0};
    char ** fields = str_split(command, SPACE_DELIMITER);
    int countFields = charArrayLength(fields);
    if (countFields != 5) {
        free(fields);
        return NULL;
    }

    if (!parse_cron_field(&temp, fields[0], CRON_MINUTE) ||
        !parse_cron_field(&temp, fields[1], CRON_HOUR) ||
        !parse_cron_field(&temp, fields[2], CRON_DOM) ||
        !parse_cron_field(&temp, fields[3], CRON_M) ||
        !parse_cron_field(&temp, fields[4], CRON_DOW))
    {
        free_string_array(fields);

        return NULL;
    }

    *scheduler = temp;

    free_string_array(fields);
    return scheduler;

}

#include "../include/cron_parser.h"


#define ARRAY_OF_NUMBERS_DELIMITER ','
#define RANGE_OF_NUMBERS_DELIMITER '-'
#define SPACE_DELIMITER ' '
#define STAR "*"
#define STEP_DELIMITER '/'
#define LAST_DOM_TOKEN "$"
#define MAX_DAYS_IN_MONTH 31
#define  SECONDS_IDENTIFIER "seconds"

static void clear_parse_error(CronParseError *error) {
    if (error == NULL) {
        return;
    }
    memset(error, 0, sizeof(*error));
}

static void set_parse_error(CronParseError* error, int fieldIndex, int tokenIndex, const char * token,
    const char * fmt, ...) {
    va_list args;
    if (error == NULL) return;

    error->fieldIndex = fieldIndex;
    error->tokenIndex = tokenIndex;

    if (token != NULL) {
        snprintf(error->token, sizeof(error->token), "%s", token);
    }

    va_start(args, fmt);
    vsnprintf(error->message, sizeof(error->message), fmt, args);
    va_end(args);
}

static const char *
  field_name(FieldType type)
{
    switch (type)
    {
        case CRON_MINUTE:
            return "minute";
        case CRON_HOUR:
            return "hour";
        case CRON_DOM:
            return "day-of-month";
        case CRON_M:
            return "month";
        case CRON_DOW:
            return "day-of-week";
    }

    return "unknown";
}

static bool
has_empty_part(const char *s, char delimiter)
{
    size_t len;

    if (s == NULL || s[0] == '\0')
        return true;

    len = strlen(s);

    if (s[0] == delimiter || s[len - 1] == delimiter)
        return true;

    for (size_t i = 1; i < len; i++)
    {
        if (s[i] == delimiter && s[i - 1] == delimiter)
            return true;
    }

    return false;
}




bool is_leap_year(int year) {
    if (year % 400 == 0) return true;

    if (year % 100 == 0) return false;
    return year % 4 == 0;
}

bool search_for_cron_name(const CronName * names, const char * token , int * value) {
    for (int i = 0; names[i].name != NULL; i++) {
        if (pg_strcasecmp(names[i].name, token) == 0) {
            *value = names[i].value;
            return true;
        }
    }
    return false;
}
bool isLastDayOfMonth(int current_dom, int current_month, int current_year) {
    int daysInCurrentMonth = days_in_month(current_year, current_month);
    return daysInCurrentMonth == current_dom;
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

bool is_last_dom_token(const char * token) {
    return token != NULL && strcmp(token, LAST_DOM_TOKEN) == 0;
}


bool parse_range(CronSchedule * scheduler, char* string, FieldType type, int * start, int* finish) {

    if (!string || !start || !finish) return false;
    if (has_empty_part(string, RANGE_OF_NUMBERS_DELIMITER)) return false;

    char ** splittedString = str_split(string, RANGE_OF_NUMBERS_DELIMITER);

    if (!splittedString) return false;


    if (charArrayLength(splittedString) != 2) {
        free_string_array(splittedString);
        return false;
    }
    if (!parse_field_value(scheduler, type, splittedString[0], start)) {
        free_string_array(splittedString);
        return false;
    }

    if (type == CRON_DOM && is_last_dom_token(splittedString[1])) {
        *finish = MAX_DAYS_IN_MONTH;
    }else if (!parse_field_value(scheduler, type, splittedString[1], finish) ) {
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

bool set_field_range(CronSchedule * scheduler, FieldType type, int start, int finish, int step,
    int fieldIndex,
    int tokenIndex,
    const char *token,
    CronParseError *error) {

    if (!scheduler) return false;
    if (step <= 0)
    {
        set_parse_error(error, fieldIndex, tokenIndex, token, "step must be greater than 0");
        return false;
    }
    if (start > finish)
    {
        set_parse_error(error, fieldIndex, tokenIndex, token, "range start must be less than or equal to range end");
        return false;
    }

    for (int value = start; value <= finish; value += step)
    {
        if (!set_field_value(scheduler, type, value,
                             fieldIndex, tokenIndex, token, error))
            return false;
    }
    return true;
}

static void mark_star_field(CronSchedule *scheduler, FieldType type) {
    switch (type) {
        case CRON_MINUTE:
            scheduler->MIN_STAR = true;
            break;
        case CRON_HOUR:
            scheduler->HOUR_STAR = true;
            break;
        case CRON_DOM:
            scheduler->DOM_STAR = true;
            break;
        case CRON_DOW:
            scheduler->DOW_STAR = true;
            break;
        case CRON_M:
            break;
    }
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
            *min_value = 0;
            *max_value = 7;
            return true;
    }
    return false;

}

bool set_field_value(CronSchedule* scheduler, FieldType type, int value, int fieldIndex,
    int tokenIndex, const char * token, CronParseError * error) {

    int min_value;
    int max_value;

    if (!scheduler) return false;

    if (!get_bounds(&min_value, &max_value, type)) {
        return false;
    }

    if (value < min_value || value > max_value){
        set_parse_error(error,
            fieldIndex,
            tokenIndex,
            token,
            "%s must be %d-%d",
            field_name(type),
            min_value,
            max_value);
        return false;
    }

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

bool parse_field_value(CronSchedule* scheduler, FieldType type, char * token, int *value) {
    if (parse_integer(token, value)) {
        return true;
    }

    if (type == CRON_M) {
        return search_for_cron_name(month_names, token, value);
    }
    if (type == CRON_DOW) {
        return search_for_cron_name(dow_names, token, value);
    }
    return false;
}

int parse_field_item(CronSchedule* scheduler, char * item, FieldType type, int fieldIndex,
    int tokenIndex, CronParseError *error) {

    int min_value;
    int max_value;
    int number;
    int start;
    int finish;
    int step;


    if (scheduler == NULL || item == NULL || *item == '\0'){
        set_parse_error(error,
                        fieldIndex,
                        tokenIndex,
                        item,
                        "empty %s value",
                        field_name(type));
        return false;
    }

    if (!get_bounds(&min_value, &max_value, type)) return false;

    if (is_last_dom_token(item)) {
        if (type != CRON_DOM)
        {
            set_parse_error(error, fieldIndex, tokenIndex, item,
                            "$ is only allowed in day-of-month");
            return false;
        }

        scheduler->LAST_DOM = true;

        return true;
    }

    if (isStar(item)) {
        bool ok = set_field_range(scheduler, type, min_value, max_value, 1,fieldIndex,tokenIndex, item, error);
        if (ok) {
            mark_star_field(scheduler, type);
        }
        return ok;

    }

    if (strchr(item, STEP_DELIMITER) != NULL) {

        if (has_empty_part(item, STEP_DELIMITER)) {
            set_parse_error(error, fieldIndex, tokenIndex, item, "invalid step syntax");
            return false;
        }

        char ** step_parts = str_split(item, STEP_DELIMITER);
        if (step_parts == NULL) {
            set_parse_error(error, fieldIndex, tokenIndex, item, "out of memory");
            return false;
        }

        if (charArrayLength(step_parts) != 2) {
            set_parse_error(error, fieldIndex, tokenIndex, item, "invalid step syntax");
            free_string_array(step_parts);
            return false;

        }

        if (!parse_integer(step_parts[1], &step) || step <= 0){
            set_parse_error(error,
                            fieldIndex,
                            tokenIndex,
                            item,
                            "step must be a positive integer");
            free_string_array(step_parts);
            return false;

        }

        if (isStar(step_parts[0])) {
            bool ok = set_field_range(scheduler, type, min_value, max_value, step, fieldIndex, tokenIndex, item, error);
            free_string_array(step_parts);
            return ok;
        }
        if (type == CRON_DOM && is_last_dom_token(step_parts[0])) {
            bool ok = set_field_range(scheduler, type, min_value, max_value, step, fieldIndex, tokenIndex, item, error);
            free_string_array(step_parts);
            return ok;
        }

        if (strchr(step_parts[0], RANGE_OF_NUMBERS_DELIMITER) != NULL) {
            bool ok;

            ok = parse_range(scheduler, step_parts[0], type, &start, &finish);
            if (!ok) {
                set_parse_error(error,
                                fieldIndex,
                                tokenIndex,
                                item,
                                "invalid %s range",
                                field_name(type));
                free_string_array(step_parts);
                return false;
            }

            ok = set_field_range(scheduler, type, start, finish, step, fieldIndex, tokenIndex, item, error);
            free_string_array(step_parts);
            return ok;
        }

        set_parse_error(error,
                        fieldIndex,
                        tokenIndex,
                        item,
                        "invalid step base for %s",
                        field_name(type));
        free_string_array(step_parts);
        return false;

    }

    if (strchr(item, RANGE_OF_NUMBERS_DELIMITER) != NULL) {
        if (!parse_range(scheduler, item, type, &start, &finish)) {
            set_parse_error(error,
                            fieldIndex,
                            tokenIndex,
                            item,
                            "invalid %s range",
                            field_name(type));
            return false;
        }

        return set_field_range(scheduler, type, start, finish, 1, fieldIndex, tokenIndex, item, error);
    }

    if (parse_field_value(scheduler, type, item, &number)) {
        return set_field_value(scheduler, type, number, fieldIndex, tokenIndex, item, error);
    }

    set_parse_error(error,
                    fieldIndex,
                    tokenIndex,
                    item,
                    "invalid %s value",
                    field_name(type));
    return false;
}

int parse_cron_field(CronSchedule* scheduler, char *field, FieldType type, int fieldIndex, CronParseError* error) {
    char **items;
    int count;

    if (has_empty_part(field, ARRAY_OF_NUMBERS_DELIMITER)) {
        set_parse_error(error, fieldIndex, 0, field, "invalid list syntax");
        return 0;
    }

    items = str_split(field, ARRAY_OF_NUMBERS_DELIMITER);
    if (items == NULL) {
        set_parse_error(error, fieldIndex, 0, field, "empty %s field", field_name(type));
        return 0;
    }


    count = charArrayLength(items);

    if (count ==0) {
        set_parse_error(error, fieldIndex, 0, field, "empty %s field", field_name(type));
        free_string_array(items);
        return 0;
    }
    for (int i = 0; i < count; i++) {
        if (!parse_field_item(scheduler, items[i], type, fieldIndex, i + 1, error)) {
            if (error != NULL && error->message[0] == '\0') {
                set_parse_error(error,
                                fieldIndex,
                                i + 1,
                                items[i],
                                "invalid %s value",
                                field_name(type));
            }

            free_string_array(items);
            return 0;
        }
    }

    free_string_array(items);
    return 1;

}

CronSchedule *parse_with_error(char *command, CronParseError *error) {
    CronSchedule temp = {0};
    char ** fields;
    int countFields;

    clear_parse_error(error);

    if (command == NULL || command[0] == '\0') {
        set_parse_error(error, 0, 0, NULL, "schedule is empty");
        return NULL;
    }

    fields = str_split(command, SPACE_DELIMITER);
    countFields = charArrayLength(fields);

    if (countFields == 2 && strcasecmp(fields[1], SECONDS_IDENTIFIER ) == 0) {
        int seconds = 0;
        CronSchedule* scheduler = NULL;
        if (!parse_integer(fields[0], &seconds)||
            seconds < 1 || seconds > 59) {
            set_parse_error(error, 1, 1, fields[0], "seconds interval must be 1-59");
            free_string_array(fields);
            return NULL;
            }
        scheduler = malloc(sizeof(*scheduler));
        if (scheduler == NULL) {
            set_parse_error(error, 0, 0, NULL, "out of memory");
            free_string_array(fields);
            return NULL;
        }
        memset(scheduler, 0, sizeof(*scheduler));
        scheduler->SECONDS = true;
        scheduler->secondsInterval = seconds;
        free_string_array(fields);
        return scheduler;
    }

    if (countFields != 5) {
        set_parse_error(error, 0, 0, NULL, "expected 5 cron fields or \"N seconds\"");
        free_string_array(fields);
        return NULL;
    }

    if (!parse_cron_field(&temp, fields[0], CRON_MINUTE,1, error) ||
        !parse_cron_field(&temp, fields[1], CRON_HOUR,2,error) ||
        !parse_cron_field(&temp, fields[2], CRON_DOM,3,error) ||
        !parse_cron_field(&temp, fields[3], CRON_M,4,error) ||
        !parse_cron_field(&temp, fields[4], CRON_DOW, 5, error))
    {
        free_string_array(fields);

        return NULL;
    }

    CronSchedule *scheduler = malloc(sizeof(*scheduler));

    if (scheduler == NULL) {
        set_parse_error(error, 0, 0, NULL, "out of memory: scheduler is NULL");
        free_string_array(fields);
        return NULL;
    }

    *scheduler = temp;

    free_string_array(fields);
    return scheduler;

}

CronSchedule* parse(char * command) {
    return parse_with_error(command, NULL);
}

void free_cron_schedule(CronSchedule * schedule) {
    free(schedule);
}

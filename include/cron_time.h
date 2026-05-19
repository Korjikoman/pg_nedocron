#include "postgres.h"
#include <sys/time.h>
#include "datatype/timestamp.h"
#include "utils/timestamp.h"

TimestampTz StartOfMinute(TimestampTz time);
TimestampTz EndOfMinute(TimestampTz time);
int MinutesPassed(TimestampTz start, TimestampTz end);


typedef enum {
    CLOCK_JUMP_BACKWARD = 0,
    CLOCK_PROGRESSED = 1,
    CLOCK_JUMP_FORWARD = 2,
    CLOCK_CHANGE = 3
} ClockStatus;

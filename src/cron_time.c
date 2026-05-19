#include "../include/cron_time.h"

TimestampTz StartOfMinute(TimestampTz time) {
    TimestampTz result = 0;

#ifdef HAVE_INT64_TIMESTAMP
    result = time - time % 60000000;
#else
    result = time - time % 60;
#endif

    return result;
}


TimestampTz EndOfMinute(TimestampTz time) {
    TimestampTz result = StartOfMinute(time);

#ifdef HAVE_INT64_TIMESTAMP
    result = result + 60000000;
#else
    result = result + 60;
#endif

    return result;
}

int MinutesPassed(TimestampTz start, TimestampTz end) {
    long secs = 0;
    int microsecs = 0; // :^)
    TimestampDifference(start, end, &secs, &microsecs);
    return (int) secs / 60;
}

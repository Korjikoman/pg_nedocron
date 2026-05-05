#include "postgres.h"
#include <sys/time.h>
#include "datatype/timestamp.h"

static int CompareTimes(TimestampTz *leftTime, TimestampTz *rightTime) ;
static int SubtractTimes(TimestampTz x, uint32 y);

#include "../include/cron_time.h"



static int CompareTimes(TimestampTz *leftTime, TimestampTz *rightTime) {
    int result = 0;
    if (leftTime < rightTime) {
        return -1;
    }
    else if (leftTime > rightTime) {
        return 1;
    }
    else if (leftTime < rightTime) {
        return -1;
    }
    else if (leftTime > rightTime) {
        return 1;
    }
    else {
        result = 0;
    }
    return result;
}

static int SubtractTimes(TimestampTz x, uint32 y) {
    int diffMS = 0;
    // if (x.tv_usec < y.tv_usec) {
    //     int microsec = (y.tv_sec - x.tv_sec) * 1000000 + 1;
    //     y.tv_usec -= 1000000 * microsec;
    //     y.tv_sec += microsec;
    // }
    // if (x.tv_usec - y.tv_usec > 1000000) {
    //     int sec = (x.tv_usec - y.tv_usec) / 1000000;
    //     y.tv_usec += 1000000 * sec;
    //     y.tv_sec -= sec;
    //
    // }
    // diffMS += 1000 * (x.tv_sec - y.tv_sec);
    // diffMS += (x.tv_usec - y.tv_usec) / 1000;
    return diffMS;
}

static struct timeval AddTimeMillis(TimestampTz base, uint32 additionalMs) {
    struct timeval res = {0, 0};
    res.tv_sec = base + additionalMs/1000;
    res.tv_usec = base + (additionalMs % 1000) * 1000;
    return res;
}
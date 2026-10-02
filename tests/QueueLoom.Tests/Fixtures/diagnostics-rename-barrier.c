// Linux-only witness for the .NET check-then-rename race. The real rename is unchanged;
// hold it until both independent exporters have passed .NET's destination existence check.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

int rename(const char *source, const char *destination)
{
    int (*real_rename)(const char *, const char *) = dlsym(RTLD_NEXT, "rename");
    const char *target = getenv("QUEUELOOM_EXPORT_RACE_DESTINATION");
    const char *ready = getenv("QUEUELOOM_EXPORT_RENAME_READY");
    const char *release = getenv("QUEUELOOM_EXPORT_RENAME_RELEASE");
    if (target && ready && release && strcmp(target, destination) == 0)
    {
        int marker = open(ready, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (marker < 0) return -1;
        close(marker);
        struct timespec pause = {0, 5000000};
        int attempts = 0;
        while (access(release, F_OK) != 0)
        {
            if (++attempts > 4000) { errno = ETIMEDOUT; return -1; }
            nanosleep(&pause, NULL);
        }
    }
    return real_rename(source, destination);
}

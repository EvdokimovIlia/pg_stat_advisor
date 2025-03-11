#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"

#include "cmd_dispatcher.h"
#include <stddef.h>

typedef struct BackgroundTaskManagerSharedState BackgroundTaskManagerSharedState;

extern shmem_request_hook_type prev_shmem_request_hook;
extern shmem_startup_hook_type prev_shmem_startup_hook;

void bgtm_shmem_request(void);
void bgtm_shmem_startup(void);

void RegisterBackgroundTaskManager(void);
PGDLLEXPORT void BackgroundTaskManagerMain(Datum main_arg);
PGDLLEXPORT void DatabaseWorkerMain(Datum main_arg);

bool ExecuteCommandInBackground(StringInfo command_data);

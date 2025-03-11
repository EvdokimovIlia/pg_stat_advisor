#include "include/pg_stat_advisor_bgw.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/dsm.h"
#include "executor/spi.h"
#include "utils/wait_event.h"
#include "lib/ilist.h"
#include "common/hashfn.h"
#include "include/ringbuffer.h"

static StringInfo exit_cmd = NULL;

#define DB_WORKER_KEY_WORKER_ID 0
#define DB_WORKER_KEY_COMMAND_QUEUE 1
#define DB_WORKER_QUEUE_SIZE 0x5000
#define DB_WORKER_MAGIC 0x57a7ad71
#define DB_WORKER_EXIT_TIMEOUT_SECONDS 30
#define DEFERRED_JOBS_MAX 30

extern int ring_buffer_capacity;

typedef struct WorkerKey
{
    Oid dbid;
    Oid userid;
} WorkerKey;

typedef struct WorkerEntry
{
    WorkerKey key;
    BackgroundWorkerHandle *worker_handle;
    shm_mq_handle *command_qh;
    dsm_segment *seg;
    pid_t pid;
    TimestampTz last_activity;
} WorkerEntry;

typedef struct RemovedWorkerEntryNode
{
    slist_node node; /* Linked list node (mandatory) */
    WorkerEntry entry;
} RemovedWorkerEntryNode;

typedef struct BackgroundTaskManagerSharedState
{
    Latch latch;
    pg_atomic_uint32 deferred_job_count;
    RingBuffer ring_buffer;
} BackgroundTaskManagerSharedState;

typedef struct DeferredDatabaseJobsEntry
{
    WorkerKey key;
    slist_head commands;
} DeferredDatabaseJobsEntry;

typedef struct DeferredNode
{
    slist_node node; /* Linked list node (mandatory) */
    StringInfo data;
} DeferredNode;

static BackgroundTaskManagerSharedState *bgtm_shared = NULL;

static HTAB *db_workers_htable = NULL;
static HTAB *db_deferred_tasks_htable = NULL;

static slist_head exited_workers;
static volatile sig_atomic_t got_sigusr1_signal = 0;

static void sigusr1_handler(SIGNAL_ARGS);
static void CheckSigusr1(void);
static void WaitForExitedWorkers(void);

static void HandleGlobalQueue(void);

static void PushDeferredCommand(StringInfo data, const WorkerKey *key);
static bool HandleDeferredMessageEntry(DeferredDatabaseJobsEntry *deferred_entry, WorkerEntry *entry);
static void HandleDeferredMessages(void);

static bool SendCommandToDatabaseWorker(WorkerEntry *entry, StringInfo command_data);
static WorkerEntry *GetOrCreateDatabaseWorker(const WorkerKey *key);
static WorkerEntry *CreateDatabaseWorker(const WorkerKey *key);
static void WorkerExitOnTimeout(void);

static uint32 WorkerKeyHash(const void *key, Size keysize);
static int WorkerKeyCompare(const void *key1, const void *key2, Size keysize);
static void InitWorkersHashTable(void);
static void InitDeferredDbTasksHashTable(void);

static void PushExitedWorker(const WorkerEntry *entry);

// -------------------------------------

bool ExecuteCommandInBackground(StringInfo command_data)
{
    bool res;
    uint32 deferred_background_jobs;

    if (bgtm_shared == NULL)
    {
        bool found;
        bgtm_shared = ShmemInitStruct("bgtm_shared", sizeof(BackgroundTaskManagerSharedState), &found);

        if (!found)
            return false;
    }

    deferred_background_jobs = pg_atomic_read_u32(&bgtm_shared->deferred_job_count);
    if (deferred_background_jobs >= DEFERRED_JOBS_MAX)
    {
        ereport(WARNING,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("Deferred jobs in the background worker have reached max count"),
                 errhint("You might need to increase max_worker_processes.")));
        return false;
    }

    res = RingBufferPush(&bgtm_shared->ring_buffer, command_data);

    if (res)
        SetLatch(&bgtm_shared->latch);

    return res;
}

void bgtm_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(sizeof(BackgroundTaskManagerSharedState) + ring_buffer_capacity);
}

void bgtm_shmem_startup(void)
{
    bool found;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    bgtm_shared = ShmemInitStruct("bgtm_shared", sizeof(BackgroundTaskManagerSharedState), &found);

    if (!found)
        InitRingBuffer(&bgtm_shared->ring_buffer);

    pg_atomic_init_u32(&bgtm_shared->deferred_job_count, 0);
}

void RegisterBackgroundTaskManager(void)
{
    BackgroundWorker worker;
    memset(&worker, 0, sizeof(BackgroundWorker));
    strncpy(worker.bgw_name, "BackgroundTaskManager", BGW_MAXLEN);
    strncpy(worker.bgw_type, "BackgroundTaskManager", BGW_MAXLEN);
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    worker.bgw_main_arg = (Datum)0;
    worker.bgw_notify_pid = 0;
    strncpy(worker.bgw_library_name, "pg_stat_advisor", BGW_MAXLEN);
    strncpy(worker.bgw_function_name, "BackgroundTaskManagerMain", BGW_MAXLEN);

    RegisterBackgroundWorker(&worker);
}

void BackgroundTaskManagerMain(Datum main_arg)
{
    bool found;
    bgtm_shared = ShmemInitStruct("bgtm_shared", sizeof(BackgroundTaskManagerSharedState), &found);

    if (!found)
        return;

    InitLatch(&bgtm_shared->latch);
    InitWorkersHashTable();
    InitDeferredDbTasksHashTable();
    slist_init(&exited_workers);

    pqsignal(SIGUSR1, sigusr1_handler);
    BackgroundWorkerUnblockSignals();

    while (true)
    {
        int rc = WaitLatch(&bgtm_shared->latch,
                           WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                           20000, /* Timeout set to 20,000 milliseconds (20 seconds) */
                           PG_WAIT_EXTENSION);

        ResetLatch(&bgtm_shared->latch);

        if (rc & WL_LATCH_SET)
        {
            CheckSigusr1();
            HandleDeferredMessages();
            HandleGlobalQueue();
        }

        if (rc & WL_TIMEOUT)
            WorkerExitOnTimeout();
    }
}

static void sigusr1_handler(SIGNAL_ARGS)
{
    got_sigusr1_signal = 1;
    SetLatch(&bgtm_shared->latch);
}

/*
 * CheckSigusr1
 *
 * Whenever a dynamic database worker is just created or exits, a SIGUSR1 signal is sent.
 * This function handles the signal by checking for any exited workers and waiting for them,
 * so that deferred commands can start their workers immediately.
 */
static void CheckSigusr1(void)
{
    if (got_sigusr1_signal == 1)
    {
        WaitForExitedWorkers();
        got_sigusr1_signal = 0;
    }
}

static void WaitForExitedWorkers(void)
{
    slist_mutable_iter iter;
    slist_foreach_modify(iter, &exited_workers)
    {
        RemovedWorkerEntryNode *node = slist_container(RemovedWorkerEntryNode, node, iter.cur);
        BgwHandleStatus status = GetBackgroundWorkerPid(node->entry.worker_handle, &node->entry.pid);

        if (status == BGWH_STOPPED)
        {
            WaitForBackgroundWorkerShutdown(node->entry.worker_handle);

            dsm_detach(node->entry.seg);
            pfree(node->entry.worker_handle);
            pfree(node);
            slist_delete_current(&iter);
        }
    }
}

/*
 * HandleGlobalQueue
 *
 * Processes commands from the global ring buffer, dispatching them to the appropriate
 * database worker or handling them directly if they are not database-specific commands.
 *
 * If the command cannot be sent immediately due to the detached message queue(SHM_MQ_DETACHED), or if a database worker
 * cannot be created to handle the command, the command is deferred.
 *
 * Deferred commands will be retried later when HandleDeferredMessages() is called.
 */
static void HandleGlobalQueue(void)
{
    StringInfo data;

    while ((data = RingBufferPop(&bgtm_shared->ring_buffer)) != NULL)
    {
        WorkerKey key;

        if (IsDatabaseCommand(data, &key.dbid, &key.userid))
        {
            WorkerEntry *entry;

            entry = GetOrCreateDatabaseWorker(&key);

            if (entry == NULL) 
            {
                PushDeferredCommand(data, &key); /* postpone command, retry send later */
                continue;
            }

            if (!SendCommandToDatabaseWorker(entry, data))
            {
                //PushDeferredCommand(data, &key); /* postpone command, retry send later */
                PushExitedWorker(entry);
                hash_search(db_workers_htable, &entry->key, HASH_REMOVE, NULL);
            }
        }
        else
            HandleCommand(data);

        pfree(data->data);
        pfree(data);
    }
}

static void PushDeferredCommand(StringInfo data, const WorkerKey *key)
{
    DeferredNode *new_node;
    DeferredDatabaseJobsEntry *entry;
    bool found;

    entry = (DeferredDatabaseJobsEntry *)hash_search(db_deferred_tasks_htable, key, HASH_ENTER, &found);

    if (!found)
    {
        entry->key = *key;
        slist_init(&entry->commands);
    }

    new_node = (DeferredNode *)palloc(sizeof(DeferredNode));
    new_node->data = data;

    slist_push_head(&entry->commands, &new_node->node);
    pg_atomic_add_fetch_u32(&bgtm_shared->deferred_job_count, 1);
}

static bool HandleDeferredMessageEntry(DeferredDatabaseJobsEntry *deferred_entry, WorkerEntry *entry)
{
    slist_mutable_iter iter;

    slist_foreach_modify(iter, &deferred_entry->commands)
    {
        DeferredNode *node = slist_container(DeferredNode, node, iter.cur);
        bool res = SendCommandToDatabaseWorker(entry, node->data);

        slist_delete_current(&iter);

        pfree(node->data->data);
        pfree(node->data);
        pfree(node);

        pg_atomic_add_fetch_u32(&bgtm_shared->deferred_job_count, -1);

        if (!res)
        {
            PushExitedWorker(entry);
            hash_search(db_workers_htable, &entry->key, HASH_REMOVE, NULL);
            return false;
        }
    }

    return true;
}

static void HandleDeferredMessages(void)
{
    bool can_start_worker;
    HASH_SEQ_STATUS hash_seq;
    DeferredDatabaseJobsEntry *deferred_entry;

    List *entries_to_remove = NULL;
    ListCell *lc;

    can_start_worker = true;
    hash_seq_init(&hash_seq, db_deferred_tasks_htable);

    while ((deferred_entry = (DeferredDatabaseJobsEntry *)hash_seq_search(&hash_seq)) != NULL)
    {
        WorkerEntry *entry;
        bool found;

        entry = hash_search(db_workers_htable, &deferred_entry->key, HASH_FIND, &found);

        if (found)
        {
            if (!HandleDeferredMessageEntry(deferred_entry, entry))
                continue;

            entries_to_remove = lappend(entries_to_remove, deferred_entry);
        }
        else if (can_start_worker)
        {
            /* handle deferred commands postponed by worker limit */
            entry = CreateDatabaseWorker(&deferred_entry->key);

            if (entry == NULL)
            {
                can_start_worker = false;
                continue;
            }

            if (!HandleDeferredMessageEntry(deferred_entry, entry))
                continue;

            entries_to_remove = lappend(entries_to_remove, deferred_entry);
        }
    }

    foreach (lc, entries_to_remove)
    {
        bool found;
        DeferredDatabaseJobsEntry *cur = (DeferredDatabaseJobsEntry *)lfirst(lc);
        hash_search(db_deferred_tasks_htable, &cur->key, HASH_REMOVE, &found);
    }

    list_free(entries_to_remove);
}

bool SendCommandToDatabaseWorker(WorkerEntry *entry, StringInfo data)
{
    shm_mq_result res;
    do
    {
        res = shm_mq_send(entry->command_qh, data->len, data->data, true, true);

        if (res == SHM_MQ_DETACHED)
            return false;

        WaitLatch(MyLatch,
                  WL_LATCH_SET | WL_POSTMASTER_DEATH,
                  -1, /* Infinite timeout */
#if PG_VERSION_NUM < 170000
                  WAIT_EVENT_MQ_SEND);
#else
                  WAIT_EVENT_MESSAGE_QUEUE_SEND);
#endif

        ResetLatch(MyLatch);
    } while (res == SHM_MQ_WOULD_BLOCK); /* add retry count? */

    return res == SHM_MQ_SUCCESS;
}

static WorkerEntry *GetOrCreateDatabaseWorker(const WorkerKey *key)
{
    bool found;
    WorkerEntry *entry;

    entry = (WorkerEntry *)hash_search(db_workers_htable, key, HASH_FIND, &found);

    if (!found)
        entry = CreateDatabaseWorker(key);

    return entry;
}

static void InitDatabaseWorkerSharedMemory(const WorkerKey *key, dsm_segment **seg, shm_mq_handle **command_qh)
{
    shm_toc_estimator e;
    Size segsize;
    shm_toc *toc;

    shm_mq *command_mq;
    WorkerKey *id;

    shm_toc_initialize_estimator(&e);
    shm_toc_estimate_chunk(&e, sizeof(WorkerEntry));
    shm_toc_estimate_chunk(&e, sizeof(command_mq));
    shm_toc_estimate_chunk(&e, DB_WORKER_QUEUE_SIZE);
    segsize = shm_toc_estimate(&e);

    *seg = dsm_create(segsize, 0);
    toc = shm_toc_create(DB_WORKER_MAGIC, dsm_segment_address(*seg), segsize);

    id = shm_toc_allocate(toc, sizeof(WorkerKey));
    memcpy(id, key, sizeof(WorkerEntry));

    command_mq = shm_mq_create(shm_toc_allocate(toc, DB_WORKER_QUEUE_SIZE),
                               DB_WORKER_QUEUE_SIZE);

    shm_toc_insert(toc, DB_WORKER_KEY_COMMAND_QUEUE, command_mq);
    shm_toc_insert(toc, DB_WORKER_KEY_WORKER_ID, id);
    shm_mq_set_sender(command_mq, MyProc);

    *command_qh = shm_mq_attach(command_mq, *seg, NULL);
}

static BackgroundWorkerHandle *StartDatabaseWorker(const WorkerKey *key, dsm_segment *seg, pid_t *pid)
{
    BgwHandleStatus bgwstatus;
    BackgroundWorkerHandle *worker_handle;
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(BackgroundWorker));

    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "pg_stat_advisor");
    sprintf(worker.bgw_function_name, "DatabaseWorkerMain");
    snprintf(worker.bgw_name, BGW_MAXLEN,
             "database worker (dbid: %d, userid: %d) by PID %d", key->dbid, key->userid, MyProcPid);
    strncpy(worker.bgw_type, "database background executor", BGW_MAXLEN);
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
    worker.bgw_notify_pid = MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&worker, &worker_handle))
    {
        ereport(WARNING,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not register background process"),
                 errhint("You might need to increase max_worker_processes.")));
        return NULL;
    }

    bgwstatus = WaitForBackgroundWorkerStartup(worker_handle, pid);

    if (bgwstatus != BGWH_STARTED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not start background worker")));

    return worker_handle;
}

static WorkerEntry *CreateDatabaseWorker(const WorkerKey *key)
{
    dsm_segment *seg;
    shm_mq_handle *command_qh;
    WorkerEntry *entry;
    BackgroundWorkerHandle *worker_handle;
    bool found;
    pid_t pid;

    InitDatabaseWorkerSharedMemory(key, &seg, &command_qh);
    worker_handle = StartDatabaseWorker(key, seg, &pid);

    if (worker_handle == NULL)
    {
        dsm_detach(seg);
        return NULL;
    }

    shm_mq_set_handle(command_qh, worker_handle);

    entry = (WorkerEntry *)hash_search(db_workers_htable, key, HASH_ENTER, &found);

    Assert(!found);

    memcpy(&entry->key, key, sizeof(WorkerEntry));
    entry->command_qh = command_qh;
    entry->worker_handle = worker_handle;
    entry->seg = seg;
    entry->last_activity = GetCurrentTimestamp();
    entry->pid = pid;

    elog(LOG, "Registered database worker [%d] for dbid: %d userid: %d", pid, key->dbid, key->userid);
    return entry;
}

void DatabaseWorkerMain(Datum main_arg)
{
    dsm_segment *seg;
    shm_toc *toc;
    shm_mq *command_mq;
    shm_mq_handle *command_qh;
    WorkerKey const *id;
    bool found;

    seg = dsm_attach(DatumGetInt32(main_arg));

    if (seg == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("could not map dynamic shared memory segment")));

    toc = shm_toc_attach(DB_WORKER_MAGIC, dsm_segment_address(seg));

    if (toc == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("bad magic number in dynamic shared memory segment")));

    command_mq = shm_toc_lookup(toc, DB_WORKER_KEY_COMMAND_QUEUE, false);
    id = shm_toc_lookup(toc, DB_WORKER_KEY_WORKER_ID, false);

    shm_mq_set_receiver(command_mq, MyProc);
    command_qh = shm_mq_attach(command_mq, seg, NULL);

    BackgroundWorkerInitializeConnectionByOid(id->dbid, id->userid, 0);
    bgtm_shared = ShmemInitStruct("bgtm_shared", sizeof(BackgroundTaskManagerSharedState), &found);

    Assert(found);

    while (true)
    {
        Size nbytes;
        void *data;

        shm_mq_result res = shm_mq_receive(command_qh, &nbytes, &data, true);
        if (res == SHM_MQ_SUCCESS && nbytes > 0)
        {
            StringInfoData command_data;

            command_data.cursor = 0;
            command_data.data = data;
            command_data.len = nbytes;
            command_data.maxlen = nbytes;
            HandleCommand(&command_data);
        }
        else if (res == SHM_MQ_DETACHED)
        {
            elog(LOG, "%d | Message queue detached", MyProcPid);
            break;
        }
        else if (res == SHM_MQ_WOULD_BLOCK)
        {
            SetLatch(&bgtm_shared->latch); /* shm_mq is empty, notify main worker */

            WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_POSTMASTER_DEATH,
                      -1, /* Infinite timeout */
#if PG_VERSION_NUM < 170000
                      WAIT_EVENT_MQ_RECEIVE);
#else
                      WAIT_EVENT_MESSAGE_QUEUE_RECEIVE);
#endif

            ResetLatch(MyLatch);
        }
    }

    elog(LOG, "%d | DatabaseWorkerMain exiting...", MyProcPid);
}

static void WorkerExitOnTimeout(void)
{
    HASH_SEQ_STATUS hash_seq;
    WorkerEntry *entry;
    List *entries_to_remove = NULL;
    ListCell *lc;

    hash_seq_init(&hash_seq, db_workers_htable);

    while ((entry = (WorkerEntry *)hash_seq_search(&hash_seq)) != NULL)
    {
        if (TimestampDifferenceExceeds(entry->last_activity, GetCurrentTimestamp(), DB_WORKER_EXIT_TIMEOUT_SECONDS * 1000))
        {
            if (exit_cmd == NULL)
                exit_cmd = CreateExitCommand();

            PushExitedWorker(entry);
            SendCommandToDatabaseWorker(entry, exit_cmd);
            entries_to_remove = lappend(entries_to_remove, entry);
        }
    }

    foreach (lc, entries_to_remove)
    {
        WorkerEntry *cur = (WorkerEntry *)lfirst(lc);
        hash_search(db_workers_htable, &cur->key, HASH_REMOVE, NULL);
    }

    list_free(entries_to_remove);
}

static void PushExitedWorker(const WorkerEntry *entry)
{
    RemovedWorkerEntryNode *new_node = (RemovedWorkerEntryNode *)palloc(sizeof(RemovedWorkerEntryNode));
    memcpy(&new_node->entry, entry, sizeof(WorkerEntry));
    slist_push_head(&exited_workers, &new_node->node);
}

/* Hash function for WorkerKey. */
static uint32
WorkerKeyHash(const void *key, Size keysize)
{
    const WorkerKey *workerKey = (const WorkerKey *)key;
    return hash_uint32(workerKey->dbid) ^ hash_uint32(workerKey->userid);
}

/* Comparison function for WorkerKey. */
static int
WorkerKeyCompare(const void *key1, const void *key2, Size keysize)
{
    const WorkerKey *k1 = (const WorkerKey *)key1;
    const WorkerKey *k2 = (const WorkerKey *)key2;

    return (k1->dbid == k2->dbid && k1->userid == k2->userid) ? 0 : 1;
}

static void
InitWorkersHashTable(void)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(HASHCTL));

    ctl.keysize = sizeof(WorkerKey);
    ctl.entrysize = sizeof(WorkerEntry);
    ctl.hash = WorkerKeyHash;
    ctl.match = WorkerKeyCompare;

    db_workers_htable = hash_create("Worker Table", 128, &ctl,
                                    HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

static void InitDeferredDbTasksHashTable(void)
{
    HASHCTL ctl;
    memset(&ctl, 0, sizeof(HASHCTL));
    ctl.keysize = sizeof(WorkerKey);
    ctl.entrysize = sizeof(DeferredDatabaseJobsEntry);
    ctl.hash = WorkerKeyHash;
    ctl.match = WorkerKeyCompare;

    db_deferred_tasks_htable = hash_create("Deferred Table", 128, &ctl,
                                           HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

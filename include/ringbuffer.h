#include "postgres.h"
#include "port/atomics.h"

/* Ring buffer structure */
typedef struct RingBuffer
{
    pg_atomic_uint32 head; /* Write index */
    pg_atomic_uint32 tail; /* Read index */
    char buffer[FLEXIBLE_ARRAY_MEMBER]; /* size = ring_buffer_capacity */
} RingBuffer;

void InitRingBuffer(RingBuffer *ring_buffer);
bool RingBufferPush(RingBuffer *ring_buffer, StringInfo data);
StringInfo RingBufferPop(RingBuffer *ring_buffer);

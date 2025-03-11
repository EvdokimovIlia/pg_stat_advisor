#include "include/ringbuffer.h"

/* Define ring buffer size */
int ring_buffer_capacity = 10240;

void InitRingBuffer(RingBuffer *ring_buffer)
{
    pg_atomic_init_u32(&ring_buffer->head, 0);
    pg_atomic_init_u32(&ring_buffer->tail, 0);
    memset(ring_buffer->buffer, 0, ring_buffer_capacity);
}

bool RingBufferPush(RingBuffer *ring_buffer, StringInfo data)
{
    uint32 length;
    uint32 data_start;

    uint32 head;
    uint32 data_size = data->len;
    uint32 total_size = sizeof(uint32) + data_size; /* Size field + data */

    if (total_size > ring_buffer_capacity)
    {
        ereport(WARNING, (errmsg("Data too large for the ring buffer")));
        return false;
    }

    while (true)
    {
        uint32 free_space;
        uint32 expected_head;
        uint32 tail, next_head;

        head = pg_atomic_read_u32(&ring_buffer->head);
        tail = pg_atomic_read_u32(&ring_buffer->tail);

        free_space = (tail <= head)
                         ? ring_buffer_capacity - head + tail
                         : tail - head;

        if (free_space <= total_size)
        {
            ereport(WARNING, (errmsg("Not enough space in the ring buffer")));
            return false;
        }

        next_head = (head + total_size) % ring_buffer_capacity;

        /* Attempt to reserve space */
        expected_head = head;
        if (pg_atomic_compare_exchange_u32(&ring_buffer->head, &expected_head, next_head))
        {
            /* Successfully reserved space */
            break;
        }
        /* Otherwise, expected_head has been updated; retry */
    }

    /* Write data length */
    length = data_size;

    /* Write length to buffer, handling wrap-around */
    if (head + sizeof(uint32) <= ring_buffer_capacity)
        memcpy(&ring_buffer->buffer[head], &length, sizeof(uint32));
    else
    {
        uint32 first_part = ring_buffer_capacity - head;
        memcpy(&ring_buffer->buffer[head], &length, first_part);
        memcpy(&ring_buffer->buffer[0], ((char *)&length) + first_part, sizeof(uint32) - first_part);
    }

    /* Write data to buffer, handling wrap-around */
    data_start = (head + sizeof(uint32)) % ring_buffer_capacity;
    if (data_start + data_size <= ring_buffer_capacity)
        memcpy(&ring_buffer->buffer[data_start], data->data, data_size);
    else
    {
        uint32 first_part = ring_buffer_capacity - data_start;
        memcpy(&ring_buffer->buffer[data_start], data->data, first_part);
        memcpy(&ring_buffer->buffer[0], data->data + first_part, data_size - first_part);
    }

    return true;
}

StringInfo RingBufferPop(RingBuffer *ring_buffer)
{
    StringInfo result;
    uint32 data_start;
    uint32 data_size;

    while (true)
    {
        uint32 total_size;
        uint32 expected_tail;
        uint32 head, tail, next_tail;

        tail = pg_atomic_read_u32(&ring_buffer->tail);
        head = pg_atomic_read_u32(&ring_buffer->head);

        if (tail == head)
        {
            /* Buffer is empty */
            return NULL;
        }

        /* Read data length */
        if (tail + sizeof(uint32) <= ring_buffer_capacity)
            memcpy(&data_size, &ring_buffer->buffer[tail], sizeof(uint32));
        else
        {
            uint32 first_part = ring_buffer_capacity - tail;
            memcpy(&data_size, &ring_buffer->buffer[tail], first_part);
            memcpy(((char *)&data_size) + first_part, &ring_buffer->buffer[0], sizeof(uint32) - first_part);
        }

        total_size = sizeof(uint32) + data_size;
        next_tail = (tail + total_size) % ring_buffer_capacity;

        /* Attempt to advance tail */
        expected_tail = tail;
        if (pg_atomic_compare_exchange_u32(&ring_buffer->tail, &expected_tail, next_tail))
        {
            /* Successfully reserved the data */
            data_start = (tail + sizeof(uint32)) % ring_buffer_capacity;
            break;
        }
        /* Otherwise, expected_tail has been updated; retry */
    }

    /* Read data from buffer */
    result = makeStringInfo();
    enlargeStringInfo(result, data_size);

    if (data_start + data_size <= ring_buffer_capacity)
        memcpy(result->data, &ring_buffer->buffer[data_start], data_size);
    else
    {
        uint32 first_part = ring_buffer_capacity - data_start;
        memcpy(result->data, &ring_buffer->buffer[data_start], first_part);
        memcpy(result->data + first_part, &ring_buffer->buffer[0], data_size - first_part);
    }

    result->len = data_size;
    return result;
}

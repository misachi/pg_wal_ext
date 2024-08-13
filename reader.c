// Functions copied from backend/access/transam/xlogreader.c -- not exported

#include "postgres.h"

#include "access/xlogreader.h"

#define DEFAULT_DECODE_BUFFER_SIZE (64 * 1024)

DecodedXLogRecord *XLogReadRecordAlloc(XLogReaderState *state, size_t xl_tot_len, bool allow_oversized);
void report_invalid_record(XLogReaderState *state, const char *fmt, ...);
bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr, XLogRecPtr PrevRecPtr, XLogRecord *record, bool randAccess);
bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr);

DecodedXLogRecord *XLogReadRecordAlloc(XLogReaderState *state, size_t xl_tot_len, bool allow_oversized)
{
    size_t required_space = DecodeXLogRecordRequiredSpace(xl_tot_len);
    DecodedXLogRecord *decoded = NULL;

    /* Allocate a circular decode buffer if we don't have one already. */
    if (unlikely(state->decode_buffer == NULL))
    {
        if (state->decode_buffer_size == 0)
            state->decode_buffer_size = DEFAULT_DECODE_BUFFER_SIZE;
        state->decode_buffer = palloc(state->decode_buffer_size);
        state->decode_buffer_head = state->decode_buffer;
        state->decode_buffer_tail = state->decode_buffer;
        state->free_decode_buffer = true;
    }

    /* Try to allocate space in the circular decode buffer. */
    if (state->decode_buffer_tail >= state->decode_buffer_head)
    {
        /* Empty, or tail is to the right of head. */
        if (state->decode_buffer_tail + required_space <=
            state->decode_buffer + state->decode_buffer_size)
        {
            /* There is space between tail and end. */
            decoded = (DecodedXLogRecord *)state->decode_buffer_tail;
            decoded->oversized = false;
            return decoded;
        }
        else if (state->decode_buffer + required_space <
                 state->decode_buffer_head)
        {
            /* There is space between start and head. */
            decoded = (DecodedXLogRecord *)state->decode_buffer;
            decoded->oversized = false;
            return decoded;
        }
    }
    else
    {
        /* Tail is to the left of head. */
        if (state->decode_buffer_tail + required_space <
            state->decode_buffer_head)
        {
            /* There is space between tail and head. */
            decoded = (DecodedXLogRecord *)state->decode_buffer_tail;
            decoded->oversized = false;
            return decoded;
        }
    }

    /* Not enough space in the decode buffer.  Are we allowed to allocate? */
    if (allow_oversized)
    {
        decoded = palloc_extended(required_space, MCXT_ALLOC_NO_OOM);
        if (decoded == NULL)
            return NULL;
        decoded->oversized = true;
        return decoded;
    }

    return NULL;
}

void report_invalid_record(XLogReaderState *state, const char *fmt, ...)
{
#define MAX_ERRORMSG_LEN 1000
    va_list args;

    fmt = _(fmt);

    va_start(args, fmt);
    vsnprintf(state->errormsg_buf, MAX_ERRORMSG_LEN, fmt, args);
    va_end(args);

    state->errormsg_deferred = true;
}

bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr, XLogRecPtr PrevRecPtr, XLogRecord *record, bool randAccess)
{
    if (record->xl_tot_len < SizeOfXLogRecord)
    {
        report_invalid_record(state,
                              "invalid record length at %X/%X: expected at least %u, got %u",
                              LSN_FORMAT_ARGS(RecPtr),
                              (uint32)SizeOfXLogRecord, record->xl_tot_len);
        return false;
    }
    if (!RmgrIdIsValid(record->xl_rmid))
    {
        report_invalid_record(state,
                              "invalid resource manager ID %u at %X/%X",
                              record->xl_rmid, LSN_FORMAT_ARGS(RecPtr));
        return false;
    }
    if (randAccess)
    {
        /*
         * We can't exactly verify the prev-link, but surely it should be less
         * than the record's own address.
         */
        if (!(record->xl_prev < RecPtr))
        {
            report_invalid_record(state,
                                  "record with incorrect prev-link %X/%X at %X/%X",
                                  LSN_FORMAT_ARGS(record->xl_prev),
                                  LSN_FORMAT_ARGS(RecPtr));
            return false;
        }
    }
    else
    {
        /*
         * Record's prev-link should exactly match our previous location. This
         * check guards against torn WAL pages where a stale but valid-looking
         * WAL record starts on a sector boundary.
         */
        if (record->xl_prev != PrevRecPtr)
        {
            report_invalid_record(state,
                                  "record with incorrect prev-link %X/%X at %X/%X",
                                  LSN_FORMAT_ARGS(record->xl_prev),
                                  LSN_FORMAT_ARGS(RecPtr));
            return false;
        }
    }

    return true;
}

bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr)
{
    pg_crc32c crc;

    Assert(record->xl_tot_len >= SizeOfXLogRecord);

    /* Calculate the CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, ((char *)record) + SizeOfXLogRecord, record->xl_tot_len - SizeOfXLogRecord);
    /* include the record header last */
    COMP_CRC32C(crc, (char *)record, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(record->xl_crc, crc))
    {
        report_invalid_record(state,
                              "incorrect resource manager data checksum in record at %X/%X",
                              LSN_FORMAT_ARGS(recptr));
        return false;
    }

    return true;
}

#include <unistd.h>

#include "postgres.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogstats.h"
#include "access/xlogutils.h"
#include "access/xlog_internal.h"
#include "common/logging.h"
#include "access/htup_details.h"
#include "access/heapam_xlog.h"
#include "funcapi.h"
#include "fmgr.h"
#include "common/fe_memutils.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

static char *record_type(XLogRecord *record);
static XLogRecord *get_xlog_record(XLogReaderState *xlogreader, XLogRecPtr targetRecPtr);

Datum pg_xlog_records(PG_FUNCTION_ARGS);

static void
report_invalid_record(XLogReaderState *state, const char *fmt, ...) // xlogreader.c
{
#define MAX_ERRORMSG_LEN 1000
    va_list args;

    fmt = _(fmt);

    va_start(args, fmt);
    vsnprintf(state->errormsg_buf, MAX_ERRORMSG_LEN, fmt, args);
    va_end(args);

    state->errormsg_deferred = true;
}

static bool
ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
                      XLogRecPtr PrevRecPtr, XLogRecord *record,
                      bool randAccess) // xlogreader.c
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

static bool
ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr) // xlogreader.c
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

typedef struct XLogPageReadPrivate
{
    XLogSegNo seg_no;
    char path_name[MAXPGPATH];
} XLogPageReadPrivate;

static void split_path(const char *path, char **dir, char **fname)
{
    char *sep;

    /* split filepath into directory & filename */
    sep = strrchr(path, '/');

    /* directory path */
    if (sep != NULL)
    {
        *dir = pnstrdup(path, sep - path);
        *fname = strdup(sep + 1);
    }
    /* local directory */
    else
    {
        *dir = NULL;
        *fname = strdup(path);
    }
}

static char *record_type(XLogRecord *record)
{
    RmgrData rmgr = RmgrTable[record->xl_rmid];
    uint8 info = record->xl_info & ~XLR_INFO_MASK;
    char *ret = NULL;
    int size;
    const char *val = rmgr.rm_identify(info);

    if (!val)
    {
        val = "unknown op code";
    }

    size = strlen(val);
    ret = palloc(size + 1);
    if (!ret)
    {
        return NULL;
    }
    ret[size] = '\0';
    memcpy(ret, val, size);
    return ret;
}

static int read_xlog_page(XLogReaderState *xlogreader, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
    XLogPageReadPrivate *private = (XLogPageReadPrivate *)xlogreader->private_data;

    uint32 target_page_off = XLogSegmentOffset(targetPagePtr, wal_segment_size);
    int read_len;
    XLogPageHeader hdr;

    int fd = open(private->path_name, O_RDONLY | PG_BINARY, 0);

    if (lseek(fd, (off_t)target_page_off, SEEK_SET) < 0)
    {
        elog(ERROR, "unable to seek file \"%s\" desc %i", private->path_name, fd);
    }

    read_len = read(fd, readBuf, reqLen);
    hdr = (XLogPageHeader)readBuf;

    if (!XLogReaderValidatePageHeader(xlogreader, targetRecPtr, (char *)hdr))
    {
        elog(ERROR, "Invalid page header errorMsg: %s, page off: %u, recPtr: %zu, pagePtr: %zu, pageArr: %zu, info: %u, rem length: %u", xlogreader->errormsg_buf, target_page_off, targetRecPtr, targetPagePtr, hdr->xlp_pageaddr, hdr->xlp_info, hdr->xlp_rem_len);
    }

    Assert(read_len == XLOG_BLCKSZ);

    xlogreader->seg.ws_segno = private->seg_no;
    xlogreader->segoff = target_page_off;
    xlogreader->readLen = read_len;

    close(fd);
    return read_len;
}

static void xlog_saved_info(XLogReaderState *xlog_reader, FunctionCallInfo fcinfo, XLogSegNo seg_no, Datum *values, bool *nulls)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
    XLogRecPtr start_lsn = InvalidXLogRecPtr;
    XLogRecPtr end_lsn = InvalidXLogRecPtr;
    XLogRecPtr temp_lsn;
    XLogRecPtr page_ptr;
    XLogRecord *record;
    int num_pages;
    char *temp_type;

    InitMaterializedSRF(fcinfo, 0);

    XLogSegNoOffsetToRecPtr(seg_no, 0, wal_segment_size, start_lsn);
    temp_lsn = start_lsn;

    num_pages = wal_segment_size / XLOG_BLCKSZ;
    for (size_t i = 0; i < num_pages; i++)
    {
        page_ptr = temp_lsn - (temp_lsn % XLOG_BLCKSZ);
        end_lsn = page_ptr + XLOG_BLCKSZ;
        xlog_reader->routine.page_read(xlog_reader, page_ptr, XLOG_BLCKSZ, page_ptr, xlog_reader->readBuf);
        while (temp_lsn < end_lsn)
        {
            record = get_xlog_record(xlog_reader, temp_lsn);
            if (!record)
            {
                elog(INFO, "Current_LSN: %zu END_LSN: %zu", temp_lsn, end_lsn);
                goto end;
            }

            values[0] = Int32GetDatum(i);
            temp_type = record_type(record);
            values[1] = CStringGetTextDatum(temp_type);

            tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

            temp_lsn = xlog_reader->NextRecPtr;
            pfree(temp_type);
            Assert(!XLogRecPtrIsInvalid(temp_lsn));
        }
    }
end:
}

static XLogRecord *get_xlog_record(XLogReaderState *xlogreader, XLogRecPtr targetRecPtr)
{
    XLogRecord *record;
    uint32 rec_off = targetRecPtr % XLOG_BLCKSZ;
    XLogPageHeader hdr = (XLogPageHeader)xlogreader->readBuf;
    uint32 page_hdr_size = XLogPageHeaderSize(hdr);
    uint32 len;
    XLogRecPtr page_ptr;
    uint32 rec_len;
    char *buf;

    if (rec_off == 0)
    {
        targetRecPtr += page_hdr_size;
        rec_off = page_hdr_size;
    }
    else if (rec_off < page_hdr_size)
    {
        elog(INFO, "invalid record offset");
        return NULL;
    }

    len = XLOG_BLCKSZ - (targetRecPtr % XLOG_BLCKSZ); // Get remaining size of page
    record = (XLogRecord *)(xlogreader->readBuf + (targetRecPtr % XLOG_BLCKSZ));

start:
    page_ptr = targetRecPtr - (targetRecPtr % XLOG_BLCKSZ);
    rec_len = record->xl_tot_len;
    xlogreader->currRecPtr = targetRecPtr;
    Assert(record->xl_tot_len > 0);
    xlogreader->NextRecPtr = targetRecPtr + MAXALIGN(rec_len);
    len = XLOG_BLCKSZ - (targetRecPtr % XLOG_BLCKSZ); // Get remaining size of page

    if (rec_len > len)
    {
        elog(INFO, "Record crosses page boundary");
        page_ptr += XLOG_BLCKSZ; // Get next page to retrieve remaining data
        memcpy(xlogreader->readRecordBuf, (char *)record, len);
        rec_len = record->xl_tot_len;
        xlogreader->routine.page_read(xlogreader, page_ptr, XLOG_BLCKSZ, targetRecPtr + len, xlogreader->readBuf);
        hdr = (XLogPageHeader)xlogreader->readBuf;

        if (hdr->xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD)
        {
            xlogreader->overwrittenRecPtr = targetRecPtr;
            targetRecPtr = page_ptr;
            goto start;
        }

        if (!(hdr->xlp_info & XLP_FIRST_IS_CONTRECORD))
        {
            report_invalid_record(xlogreader,
                                  "Invalid contrecord flag at %X/%X",
                                  LSN_FORMAT_ARGS(targetRecPtr));
            elog(INFO, "contrecord: %s", xlogreader->errormsg_buf);
            return NULL;
        }
        page_hdr_size = XLogPageHeaderSize(hdr);
        Assert(xlogreader->readRecordBufSize >= len);
        Assert(hdr->xlp_rem_len == (rec_len - len));
        if (hdr->xlp_rem_len != (rec_len - len))
        {
            elog(INFO, "Header remaining size and record size do not match ==> Header: %u, Record: %u", hdr->xlp_rem_len, len);
            return NULL;
        }

        buf = xlogreader->readBuf + page_hdr_size;
        memcpy((char *)&xlogreader->readRecordBuf[len], buf, hdr->xlp_rem_len);
        record = (XLogRecord *)xlogreader->readRecordBuf;
        xlogreader->NextRecPtr = targetRecPtr + len + page_hdr_size + MAXALIGN(hdr->xlp_rem_len);
    }

    if (!record->xl_tot_len)
    {
        elog(INFO, "Empty record");
        return NULL;
    }

    if (!ValidXLogRecordHeader(xlogreader, targetRecPtr, xlogreader->DecodeRecPtr, record, false))
    {
        elog(INFO, "Invalid record header ==> errorMsg: %s", xlogreader->errormsg_buf);
    }

    if (!ValidXLogRecord(xlogreader, record, targetRecPtr))
    {
        elog(INFO, "Invalid record ==> errorMsg: %s", xlogreader->errormsg_buf);
    }

    xlogreader->DecodeRecPtr = targetRecPtr;

    // elog(INFO, "TargetPTR: %zu NextPTR: %zu RecordLen: %u", targetRecPtr, xlogreader->NextRecPtr, record->xl_tot_len);

    return record;
}

Datum pg_xlog_records(PG_FUNCTION_ARGS)
{
#define XLOG_FIELD_NUM 2
    text *xlog_file_name = PG_GETARG_TEXT_PP(0);
    TimeLineID tli;
    XLogSegNo seg_no;
    XLogReaderState *xlog_reader;
    XLogPageReadPrivate private;

    char *directory = NULL;
    char *fname = NULL;
    Datum values[XLOG_FIELD_NUM];
    bool nulls[XLOG_FIELD_NUM] = {0};
    char *path = NULL;

    path = text_to_cstring(xlog_file_name);
    split_path(path, &directory, &fname);

    XLogFromFileName(fname, &tli, &seg_no, wal_segment_size);

    private.seg_no = seg_no;
    snprintf(private.path_name, MAXPGPATH, "%s", path);

    xlog_reader = XLogReaderAllocate(wal_segment_size, directory,
                                     XL_ROUTINE(.page_read = &read_xlog_page,
                                                .segment_open = wal_segment_open,
                                                .segment_close = wal_segment_close),
                                     &private);

    if (xlog_reader == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed while allocating a WAL reading processor.")));

    xlog_saved_info(xlog_reader, fcinfo, seg_no, values, nulls);
    XLogReaderFree(xlog_reader);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_xlog_records);

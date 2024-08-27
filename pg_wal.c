#include <unistd.h>

#include "postgres.h"
#include "access/commit_ts.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogstats.h"
#include "access/xlogutils.h"
#include "access/relation.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "common/logging.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "fmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

static char *record_type(XLogRecord *record);
static XLogRecord *get_xlog_record(XLogReaderState *xlogreader, XLogRecPtr targetRecPtr);
extern DecodedXLogRecord *XLogReadRecordAlloc(XLogReaderState *state, size_t xl_tot_len, bool allow_oversized);
extern void report_invalid_record(XLogReaderState *state, const char *fmt, ...);
extern bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr, XLogRecPtr PrevRecPtr, XLogRecord *record, bool randAccess);
extern bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr);

Datum pg_xlog_records(PG_FUNCTION_ARGS);

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
    char *temp_type = NULL;
    TransactionId xid;
    uint8 info, info2;
    xl_xact_commit *xlrec;
    // Oid relid = DatumGetObjectId(fcinfo->args[1].value);

    // if (!OidIsValid(get_rel_namespace(relid)))
    //     elog(ERROR, "Invalid table Oid %u", relid);

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

            xid = record->xl_xid;
            temp_type = record_type(record);

            info = record->xl_info & ~XLR_INFO_MASK;
            info2 = record->xl_info & XLOG_XACT_OPMASK;
            if ((xlog_reader->record && record->xl_rmid == RM_HEAP_ID && info == XLOG_HEAP_INSERT) ||
                (record->xl_rmid == RM_XACT_ID && (info2 == XLOG_XACT_COMMIT || info2 == XLOG_XACT_COMMIT_PREPARED)))
            {
                TupleDesc tup_desc;
                Relation relation, rel_rel;
                DecodedBkpBlock *bkp_blk;
                HeapTupleData tup;
                HeapTupleHeader header;
                HeapTupleTableSlot *hslot;
                size_t datalen;
                char *tupledata;
                xl_heap_header xlhdr; // on-disk header
                int len;
                StringInfoData buf;
                TupleTableSlot *slot;
                char *nspname;
                ScanKeyData skey;
                SysScanDesc sscan;
                Form_pg_class classForm = NULL;
                HeapTuple tuple = NULL;

                bkp_blk = &xlog_reader->record->blocks[0];

                rel_rel = table_open(RelationRelationId, AccessShareLock);
                ScanKeyInit(&skey,
                            Anum_pg_class_relfilenode,
                            BTEqualStrategyNumber, F_OIDEQ,
                            ObjectIdGetDatum(bkp_blk->rlocator.relNumber));
                sscan = systable_beginscan(rel_rel, ClassTblspcRelfilenodeIndexId, true,
                                           SnapshotSelf, 1, &skey);

                tuple = systable_getnext(sscan);
                if (!HeapTupleIsValid(tuple))
                {
                    systable_endscan(sscan);
                    table_close(rel_rel, AccessShareLock);
                    goto l1;
                }

                classForm = (Form_pg_class)GETSTRUCT(tuple);
                relation = table_open(classForm->oid, AccessShareLock);
                values[0] = Int32GetDatum(i);
                values[1] = TransactionIdGetDatum(xid);
                values[2] = CStringGetTextDatum(temp_type);

                if (record->xl_rmid == RM_XACT_ID)
                {
                    table_close(relation, AccessShareLock);
                    xlrec = (xl_xact_commit *)XLogRecGetData(xlog_reader);
                    values[3] = xlrec->xact_time;
                    nulls[3] = false;
                    values[4] = (Datum)0;
                    nulls[4] = true;
                }
                else
                {
                    values[3] = (Datum)0;
                    nulls[3] = true;

                    // values[4] = Int32GetDatum(xlog_reader->record->max_block_id);

                    tup_desc = RelationGetDescr(relation);
                    slot = MakeSingleTupleTableSlot(tup_desc, &TTSOpsHeapTuple);
                    hslot = (HeapTupleTableSlot *)slot;
                    hslot->tuple = &tup;
                    tupledata = XLogRecGetBlockData(xlog_reader, 0, &datalen);

                    if (!tupledata)
                    {
                        // We check if we have a back'd up block image; we try to restore it back
                        PGAlignedBlock buf;
                        Page page;
                        xl_heap_insert *xlrec;
                        ItemId lp;

                        page = (Page)buf.data;
                        if (bkp_blk->has_image)
                        {
                            if (!RestoreBlockImage(xlog_reader, 0, page))
                                ereport(ERROR,
                                        (errcode(ERRCODE_INTERNAL_ERROR),
                                         errmsg_internal("Error restoring block image: %s", xlog_reader->errormsg_buf)));
                        }
                        xlrec = (xl_heap_insert *)XLogRecGetData(xlog_reader);
                        lp = PageGetItemId(page, xlrec->offnum);
                        header = (HeapTupleHeader)PageGetItem(page, lp);
                        hslot->tuple->t_data = header;
                    }
                    else
                    {
                        len = datalen - SizeOfHeapHeader;
                        hslot->tuple->t_len = len + SizeofHeapTupleHeader;
                        hslot->tuple->t_data = palloc(hslot->tuple->t_len);
                        header = hslot->tuple->t_data;
                        hslot->tuple->t_tableOid = classForm->oid;

                        memcpy((char *)&xlhdr, tupledata, SizeOfHeapHeader);

                        memset(header, 0, SizeofHeapTupleHeader);

                        memcpy(((char *)hslot->tuple->t_data) + SizeofHeapTupleHeader, tupledata + SizeOfHeapHeader, len);

                        header->t_infomask = xlhdr.t_infomask;
                        header->t_infomask2 = xlhdr.t_infomask2;
                        header->t_hoff = xlhdr.t_hoff;
                        pfree(hslot->tuple->t_data);
                    }

                    slot->tts_ops->getsomeattrs(slot, tup_desc->natts);

                    if (RelationIsVisible(classForm->oid))
                        nspname = NULL;
                    else
                        nspname = get_namespace_name(relation->rd_rel->relnamespace);

                    initStringInfo(&buf);
                    appendStringInfo(&buf, "INSERT INTO %s(", quote_qualified_identifier(nspname, RelationGetRelationName(relation)));

                    for (size_t attnum = 0; attnum < tup_desc->natts; attnum++)
                    {
                        Form_pg_attribute thisatt = TupleDescAttr(tup_desc, attnum);
                        if (!slot->tts_isnull[attnum])
                        {
                            appendStringInfo(&buf, "%s", NameStr(thisatt->attname));

                            if ((attnum + 1) < tup_desc->natts && !slot->tts_isnull[attnum + 1])
                            {
                                appendStringInfo(&buf, ",");
                            }
                        }
                    }
                    appendStringInfo(&buf, ") VALUES(");

                    for (size_t attnum = 0; attnum < tup_desc->natts; attnum++)
                    {
                        Form_pg_attribute thisatt = TupleDescAttr(tup_desc, attnum);
                        if (!slot->tts_isnull[attnum])
                        {
                            if (thisatt->attbyval)
                            {
                                switch (thisatt->attlen)
                                {
                                case sizeof(char):
                                    appendStringInfo(&buf, "%d", DatumGetChar(slot->tts_values[attnum]));
                                    break;
                                case sizeof(int16):
                                    appendStringInfo(&buf, "%i", DatumGetInt16(slot->tts_values[attnum]));
                                    break;
                                case sizeof(int32):
                                    appendStringInfo(&buf, "%i", DatumGetInt32(slot->tts_values[attnum]));
                                    break;
#if SIZEOF_DATUM == 8
                                case sizeof(Datum):
                                    appendStringInfo(&buf, "%s", (char *)slot->tts_values[attnum]);
                                    break;
#endif
                                default:
                                    elog(ERROR, "unsupported attribute length: %d", thisatt->attlen);
                                }
                            }
                            else
                                appendStringInfo(&buf, "%s", (char *)slot->tts_values[attnum]);

                            if ((attnum + 1) < tup_desc->natts && !slot->tts_isnull[attnum + 1])
                            {
                                appendStringInfo(&buf, ",");
                            }
                        }
                    }
                    appendStringInfo(&buf, ");");
                    values[4] = CStringGetTextDatum(buf.data);
                    nulls[4] = false;

                    DecrTupleDescRefCount(tup_desc);
                    table_close(relation, AccessShareLock);
                }
                tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
                systable_endscan(sscan);
                table_close(rel_rel, AccessShareLock);
            }

        l1:
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
    uint32 rec_off;
    XLogPageHeader hdr = (XLogPageHeader)xlogreader->readBuf;
    uint32 page_hdr_size = XLogPageHeaderSize(hdr);
    uint32 len;
    XLogRecPtr page_ptr;
    uint32 rec_len;
    char *buf;
    DecodedXLogRecord *decoded;
    char *errormsg;
    xlogreader->record = NULL;

start:
    rec_off = targetRecPtr % XLOG_BLCKSZ;
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

    record = (XLogRecord *)(xlogreader->readBuf + (targetRecPtr % XLOG_BLCKSZ));

    page_ptr = targetRecPtr - (targetRecPtr % XLOG_BLCKSZ);
    rec_len = record->xl_tot_len;
    xlogreader->currRecPtr = targetRecPtr;
    Assert(record->xl_tot_len > 0);
    xlogreader->NextRecPtr = targetRecPtr + MAXALIGN(rec_len);
    len = XLOG_BLCKSZ - (targetRecPtr % XLOG_BLCKSZ);

    if (rec_len > len)
    {
        uint32 rem_size = rec_len - len; // Remaining record length we still need to retrieve
        XLogRecPtr next_ptr = targetRecPtr + len;

        // elog(INFO, "Record crosses page boundary");
        memcpy(xlogreader->readRecordBuf, (char *)record, len);
        Assert(xlogreader->readRecordBufSize >= rec_len);
        while (rem_size > 0)
        {
            page_ptr += XLOG_BLCKSZ; // Get next page to retrieve remaining data
            xlogreader->routine.page_read(xlogreader, page_ptr, XLOG_BLCKSZ, next_ptr, xlogreader->readBuf);
            hdr = (XLogPageHeader)xlogreader->readBuf;

            // Handling overwritten continuation records
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
            if (rem_size <= (XLOG_BLCKSZ - page_hdr_size) && hdr->xlp_rem_len != (rec_len - len))
            {
                elog(INFO, "Header remaining size and record size do not match ==> Header: %u, Record: %u", hdr->xlp_rem_len, len);
                return NULL;
            }

            buf = xlogreader->readBuf + page_hdr_size;
            memcpy((char *)&xlogreader->readRecordBuf[len], buf, hdr->xlp_rem_len);

            // xlp_rem_len holds the remaining size of record, even if it exceeds the size of current page
            // We need to handle both cases: remaining size fits in current page or exceeds current page and spills to the next page
            if (hdr->xlp_rem_len <= (XLOG_BLCKSZ - page_hdr_size))
            {
                len += hdr->xlp_rem_len;
                next_ptr += page_hdr_size + MAXALIGN(hdr->xlp_rem_len);
                rem_size -= hdr->xlp_rem_len;
            }
            else
            {
                len += XLOG_BLCKSZ - page_hdr_size;
                next_ptr += XLOG_BLCKSZ;
                rem_size -= XLOG_BLCKSZ - page_hdr_size;
            }
        }

        record = (XLogRecord *)xlogreader->readRecordBuf;
        xlogreader->NextRecPtr = next_ptr;
    }

    if (!rec_len)
    {
        elog(INFO, "Empty record");
        return NULL;
    }

    if (!ValidXLogRecordHeader(xlogreader, targetRecPtr, xlogreader->DecodeRecPtr, record, false))
    {
        elog(INFO, "Invalid record header ==> errorMsg: %s", xlogreader->errormsg_buf);
        return NULL;
    }

    if (!ValidXLogRecord(xlogreader, record, targetRecPtr))
    {
        elog(INFO, "Invalid record ==> errorMsg: %s", xlogreader->errormsg_buf);
        return NULL;
    }

    decoded = XLogReadRecordAlloc(xlogreader, rec_len, false);
    if (!decoded)
    {
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("out of memory"),
                 errdetail("Failed while allocating space for decoding WAL record")));
    }

    if (DecodeXLogRecord(xlogreader, decoded, record, targetRecPtr, &errormsg))
    {
        // decoded->next_lsn = xlogreader->NextRecPtr;

        // if (xlogreader->decode_queue_tail)
        //     xlogreader->decode_queue_tail->next = decoded;
        // xlogreader->decode_queue_tail = decoded;
        // if (!xlogreader->decode_queue_head)
        //     xlogreader->decode_queue_head = decoded;

        xlogreader->record = decoded;
    }

    xlogreader->DecodeRecPtr = targetRecPtr;

    // elog(INFO, "TargetPTR: %zu NextPTR: %zu RecordLen: %u", targetRecPtr, xlogreader->NextRecPtr, record->xl_tot_len);

    return record;
}

Datum pg_xlog_records(PG_FUNCTION_ARGS)
{
#define XLOG_FIELD_NUM 5
    text *xlog_file_name = PG_GETARG_TEXT_PP(0);
    TimeLineID tli;
    XLogSegNo seg_no;
    XLogReaderState *xlog_reader;
    XLogPageReadPrivate private;

    char *directory = NULL;
    char *fname = NULL;
    Datum values[XLOG_FIELD_NUM];
    bool nulls[XLOG_FIELD_NUM] = {false};
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

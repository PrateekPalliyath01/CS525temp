#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>

#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"



typedef enum { RM_FREE = 0, RM_OCCUPIED = 1 } RM_Tombstone;

static inline char rm_marker(RM_Tombstone t) { return t == RM_OCCUPIED ? (char)1 : (char)0; }
static inline bool rm_is_occupied(char b)    { return b == (char)1; }

#define RM_ATTR_NAME_MAX 15
#define RM_DEFAULT_POOL_PAGES 100

typedef struct RMContext {
    BM_PageHandle page;
    BM_BufferPool pool;
    RID cursor;            // for scans
    Expr *pred;            // scan predicate
    int tupleCount;        // number of live/known tuples
    int firstFreePage;     // heuristic page index with free slots (>=1)
    int scanned;           // scan progress counter
} RMContext;

static RMContext *g_ctx = NULL;

// Find first free slot on a page, based on tombstone byte at start of each record
static int rm_find_free_slot(char *pageData, int recSize) {
    const int slots = PAGE_SIZE / recSize;
    for (int s = 0; s < slots; s++) {
        char *slot = pageData + s * recSize;
        if (!rm_is_occupied(*slot)) return s;
    }
    return -1;
}



extern RC initRecordManager(void *mgmtData) {
    (void)mgmtData;
    initStorageManager();
    return RC_OK;
}

extern RC shutdownRecordManager() {
    if (g_ctx) {
        free(g_ctx);
        g_ctx = NULL;
    }
    return RC_OK;
}



extern RC createTable(char *name, Schema *schema) {
    if (!name || !schema) return RC_INVALID_PARAMETER;

    g_ctx = (RMContext *)malloc(sizeof(RMContext));
    if (!g_ctx) return RC_MEMORY_ALLOCATION_ERROR;

    RC rc = initBufferPool(&g_ctx->pool, name, RM_DEFAULT_POOL_PAGES, RS_LRU, NULL);
    if (rc != RC_OK) { free(g_ctx); g_ctx = NULL; return rc; }

    // Build metadata page (page 0): [tupleCount:int][firstFreePage:int][numAttr:int][keySize:int] + attr entries
    char page0[PAGE_SIZE];
    char *p = page0;
    *(int*)p = 0;                     p += sizeof(int); // tupleCount
    *(int*)p = 1;                     p += sizeof(int); // firstFreePage (data starts at page 1)
    *(int*)p = schema->numAttr;       p += sizeof(int);
    *(int*)p = schema->keySize;       p += sizeof(int);

    for (int i = 0; i < schema->numAttr; i++) {
        // attr name (fixed width)
        memset(p, 0, RM_ATTR_NAME_MAX);
        strncpy(p, schema->attrNames[i], RM_ATTR_NAME_MAX - 1);
        p += RM_ATTR_NAME_MAX;

        *(int*)p = (int)schema->dataTypes[i]; p += sizeof(int);
        *(int*)p = (int)schema->typeLength[i]; p += sizeof(int);
    }

    SM_FileHandle fh;
    rc = createPageFile(name);                 if (rc != RC_OK) { free(g_ctx); g_ctx = NULL; return rc; }
    rc = openPageFile(name, &fh);              if (rc != RC_OK) { free(g_ctx); g_ctx = NULL; return rc; }
    rc = writeBlock(0, &fh, page0);            if (rc != RC_OK) { closePageFile(&fh); free(g_ctx); g_ctx = NULL; return rc; }
    rc = closePageFile(&fh);                   if (rc != RC_OK) { free(g_ctx); g_ctx = NULL; return rc; }

    // Initialize context defaults
    g_ctx->tupleCount = 0;
    g_ctx->firstFreePage = 1;
    g_ctx->cursor.page = 1;
    g_ctx->cursor.slot = -1;
    g_ctx->pred = NULL;
    g_ctx->scanned = 0;
    g_ctx->page.data = NULL;

    return RC_OK;
}

extern RC openTable(RM_TableData *rel, char *name) {
    if (!rel || !name) return RC_INVALID_PARAMETER;
    if (!g_ctx) return RC_ERROR;

    rel->mgmtData = g_ctx;
    rel->name = name;

    RC rc = pinPage(&g_ctx->pool, &g_ctx->page, 0);
    if (rc != RC_OK) return rc;

    char *p = (char*)g_ctx->page.data;
    g_ctx->tupleCount    = *(int*)p; p += sizeof(int);
    g_ctx->firstFreePage = *(int*)p; p += sizeof(int);
    int numAttr          = *(int*)p; p += sizeof(int);
    int keySize          = *(int*)p; p += sizeof(int);

    Schema *schema = (Schema*)malloc(sizeof(Schema));
    if (!schema) { unpinPage(&g_ctx->pool, &g_ctx->page); return RC_MEMORY_ALLOCATION_ERROR; }
    schema->numAttr = numAttr;
    schema->keySize = keySize;

    schema->attrNames = (char**)malloc(sizeof(char*) * numAttr);
    schema->dataTypes = (DataType*)malloc(sizeof(DataType) * numAttr);
    schema->typeLength = (int*)malloc(sizeof(int) * numAttr);
    schema->keyAttrs = NULL; // (keys can be loaded if needed)

    if (!schema->attrNames || !schema->dataTypes || !schema->typeLength) {
        if (schema->attrNames) free(schema->attrNames);
        if (schema->dataTypes) free(schema->dataTypes);
        if (schema->typeLength) free(schema->typeLength);
        free(schema);
        unpinPage(&g_ctx->pool, &g_ctx->page);
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    for (int i = 0; i < numAttr; i++) {
        schema->attrNames[i] = (char*)malloc(RM_ATTR_NAME_MAX);
        if (!schema->attrNames[i]) {
            for (int j = 0; j < i; j++) free(schema->attrNames[j]);
            free(schema->attrNames); free(schema->dataTypes); free(schema->typeLength); free(schema);
            unpinPage(&g_ctx->pool, &g_ctx->page);
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        strncpy(schema->attrNames[i], p, RM_ATTR_NAME_MAX);
        p += RM_ATTR_NAME_MAX;

        schema->dataTypes[i] = (DataType)(*(int*)p); p += sizeof(int);
        schema->typeLength[i] = *(int*)p;            p += sizeof(int);
    }

    rel->schema = schema;

    rc = unpinPage(&g_ctx->pool, &g_ctx->page); if (rc != RC_OK) return rc;
    rc = forcePage(&g_ctx->pool, &g_ctx->page); if (rc != RC_OK) return rc;

    return RC_OK;
}

extern RC closeTable(RM_TableData *rel) {
    if (!rel || !rel->mgmtData) return RC_INVALID_PARAMETER;
    RMContext *ctx = (RMContext*)rel->mgmtData;
    shutdownBufferPool(&ctx->pool);
    return RC_OK;
}

extern RC deleteTable(char *name) {
    if (!name) return RC_INVALID_PARAMETER;
    return destroyPageFile(name);
}

extern int getNumTuples(RM_TableData *rel) {
    if (!rel || !rel->mgmtData) return -1;
    RMContext *ctx = (RMContext*)rel->mgmtData;
    return ctx->tupleCount;
}



extern RC insertRecord(RM_TableData *rel, Record *record) {
    if (!rel || !record) return RC_INVALID_PARAMETER;
    RMContext *ctx = (RMContext*)rel->mgmtData;
    const int recSize = getRecordSize(rel->schema);
    if (recSize <= 0) return RC_MEMORY_ALLOCATION_ERROR;

    RID *rid = &record->id;
    rid->page = ctx->firstFreePage;

    RC rc = pinPage(&ctx->pool, &ctx->page, rid->page);
    if (rc != RC_OK) return rc;

    char *data = ctx->page.data;
    int slot = rm_find_free_slot(data, recSize);

    // If page full, linearly probe next pages until a free slot appears
    while (slot == -1) {
        rc = unpinPage(&ctx->pool, &ctx->page); if (rc != RC_OK) return rc;
        rid->page++;
        rc = pinPage(&ctx->pool, &ctx->page, rid->page);
        if (rc != RC_OK) return rc;
        data = ctx->page.data;
        slot = rm_find_free_slot(data, recSize);
    }

    rid->slot = slot;
    char *slotPtr = data + slot * recSize;
    *slotPtr = rm_marker(RM_OCCUPIED);
    memcpy(slotPtr + 1, record->data + 1, recSize - 1);

    rc = markDirty(&ctx->pool, &ctx->page); if (rc != RC_OK) { unpinPage(&ctx->pool, &ctx->page); return rc; }
    rc = unpinPage(&ctx->pool, &ctx->page); if (rc != RC_OK) return rc;

    ctx->tupleCount++;
    // Update firstFreePage heuristic if we moved forward
    if (rid->page > ctx->firstFreePage) ctx->firstFreePage = rid->page;

    // Persist updated counts to page 0
    rc = pinPage(&ctx->pool, &ctx->page, 0); if (rc != RC_OK) return rc;
    char *p = ctx->page.data;
    *(int*)p = ctx->tupleCount;          p += sizeof(int);
    *(int*)p = ctx->firstFreePage;       /* keep other metadata intact */
    rc = markDirty(&ctx->pool, &ctx->page);
    if (rc == RC_OK) rc = unpinPage(&ctx->pool, &ctx->page);
    return rc;
}

extern RC deleteRecord(RM_TableData *rel, RID id) {
    if (!rel) return RC_INVALID_PARAMETER;
    RMContext *ctx = (RMContext*)rel->mgmtData;

    RC rc = pinPage(&ctx->pool, &ctx->page, id.page);
    if (rc != RC_OK) return rc;

    const int recSize = getRecordSize(rel->schema);
    if (recSize <= 0) { unpinPage(&ctx->pool, &ctx->page); return RC_MEMORY_ALLOCATION_ERROR; }

    char *slot = ctx->page.data + id.slot * recSize;
    *slot = rm_marker(RM_FREE);

    rc = markDirty(&ctx->pool, &ctx->page);
    if (rc == RC_OK) rc = unpinPage(&ctx->pool, &ctx->page);
    if (rc != RC_OK) return rc;

    ctx->firstFreePage = id.page; // heuristic
    if (ctx->tupleCount > 0) ctx->tupleCount--;

    // Persist counts
    rc = pinPage(&ctx->pool, &ctx->page, 0); if (rc != RC_OK) return rc;
    char *p = ctx->page.data;
    *(int*)p = ctx->tupleCount;          p += sizeof(int);
    *(int*)p = ctx->firstFreePage;
    rc = markDirty(&ctx->pool, &ctx->page);
    if (rc == RC_OK) rc = unpinPage(&ctx->pool, &ctx->page);
    return rc;
}

extern RC updateRecord(RM_TableData *rel, Record *record) {
    if (!rel || !record) return RC_INVALID_PARAMETER;
    RMContext *ctx = (RMContext*)rel->mgmtData;

    RC rc = pinPage(&ctx->pool, &ctx->page, record->id.page);
    if (rc != RC_OK) return rc;

    const int recSize = getRecordSize(rel->schema);
    if (recSize <= 0) { unpinPage(&ctx->pool, &ctx->page); return RC_MEMORY_ALLOCATION_ERROR; }

    char *slot = ctx->page.data + record->id.slot * recSize;
    *slot = rm_marker(RM_OCCUPIED);
    memcpy(slot + 1, record->data + 1, recSize - 1);

    rc = markDirty(&ctx->pool, &ctx->page);
    if (rc == RC_OK) rc = unpinPage(&ctx->pool, &ctx->page);
    return rc;
}

extern RC getRecord(RM_TableData *rel, RID id, Record *record) {
    if (!rel || !record) return RC_INVALID_PARAMETER;
    RMContext *ctx = (RMContext*)rel->mgmtData;

    RC rc = pinPage(&ctx->pool, &ctx->page, id.page);
    if (rc != RC_OK) return rc;

    const int recSize = getRecordSize(rel->schema);
    if (recSize <= 0) { unpinPage(&ctx->pool, &ctx->page); return RC_MEMORY_ALLOCATION_ERROR; }

    char *slot = ctx->page.data + id.slot * recSize;
    if (!rm_is_occupied(*slot)) {
        unpinPage(&ctx->pool, &ctx->page);
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
    }

    record->id = id;
    memcpy(record->data, slot, recSize);

    rc = unpinPage(&ctx->pool, &ctx->page);
    return rc;
}

// === Scans ===================================================================

extern RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    if (!rel || !scan) return RC_INVALID_PARAMETER;
    if (!cond) return RC_SCAN_CONDITION_NOT_FOUND;
    if (!rel->mgmtData || !rel->schema) return RC_ERROR;

    RMContext *tableCtx = (RMContext*)rel->mgmtData;
    RMContext *scanCtx = (RMContext*)malloc(sizeof(RMContext));
    if (!scanCtx) return RC_MEMORY_ALLOCATION_ERROR;

    // Share buffer pool; independent scan state
    scanCtx->pool          = tableCtx->pool;
    scanCtx->page.data     = NULL;
    scanCtx->cursor.page   = 1;     // page 0 is metadata
    scanCtx->cursor.slot   = -1;    // will advance to 0 on first next()
    scanCtx->scanned       = 0;
    scanCtx->pred          = cond;
    scanCtx->tupleCount    = tableCtx->tupleCount;
    scanCtx->firstFreePage = tableCtx->firstFreePage;

    scan->rel = rel;
    scan->mgmtData = scanCtx;
    return RC_OK;
}

extern RC next(RM_ScanHandle *scan, Record *out) {
    if (!scan || !scan->rel || !out) return RC_INVALID_PARAMETER;

    RMContext *s = (RMContext *)scan->mgmtData;
    RMContext *t = (RMContext *)scan->rel->mgmtData;
    Schema    *schema = scan->rel->schema;

    if (!s || !schema || !s->pred) return RC_SCAN_CONDITION_NOT_FOUND;

    const int recSize = getRecordSize(schema);
    if (recSize <= 0) return RC_ERROR;

    const int slotsPerPage = PAGE_SIZE / recSize;

    // Safety cap to prevent infinite loops if metadata is off
    int safetyCap = (s->firstFreePage + 2) * slotsPerPage + 2;

    while (safetyCap-- > 0) {
        // advance cursor
        if (++s->cursor.slot >= slotsPerPage) {
            s->cursor.slot = 0;
            s->cursor.page++;
        }
        if (s->cursor.page <= 0) s->cursor.page = 1;

        // Stop if we moved past known data range
        if (s->cursor.page > s->firstFreePage + 1) {
            return RC_RM_NO_MORE_TUPLES;
        }

        RC rc = pinPage(&t->pool, &s->page, s->cursor.page);
        if (rc != RC_OK) return rc;

        char *slot = s->page.data + s->cursor.slot * recSize;

        if (!rm_is_occupied(*slot)) { // skip free slots
            rc = unpinPage(&t->pool, &s->page);
            if (rc != RC_OK) return rc;
            continue;
        }

        // Build output record
        out->id = s->cursor;
        memcpy(out->data, slot, recSize);

        // Evaluate predicate (use caller-provided Value* pattern as in scaffold)
        Value *evalRes = (Value*)malloc(sizeof(Value));
        if (!evalRes) { unpinPage(&t->pool, &s->page); return RC_MEMORY_ALLOCATION_ERROR; }

        rc = evalExpr(out, schema, s->pred, &evalRes);
        if (rc != RC_OK) {
            free(evalRes);
            unpinPage(&t->pool, &s->page);
            return rc;
        }

        bool match = (evalRes->dt == DT_BOOL && evalRes->v.boolV == TRUE);
        free(evalRes);

        rc = unpinPage(&t->pool, &s->page);
        if (rc != RC_OK) return rc;

        if (match) return RC_OK;
    }

    return RC_RM_NO_MORE_TUPLES;
}

extern RC closeScan(RM_ScanHandle *scan) {
    if (!scan || !scan->mgmtData) return RC_INVALID_PARAMETER;
    RMContext *s = (RMContext *)scan->mgmtData;
    free(s);
    scan->mgmtData = NULL;
    return RC_OK;
}



extern int getRecordSize(Schema *schema) {
    if (!schema) return -1;
    int size = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_STRING: size += schema->typeLength[i]; break;
            case DT_INT:    size += (int)sizeof(int);      break;
            case DT_FLOAT:  size += (int)sizeof(float);    break;
            case DT_BOOL:   size += (int)sizeof(bool);     break;
            default: return -1;
        }
    }
    return size + 1; // +1 byte for tombstone
}

extern Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
                            int *typeLength, int keySize, int *keys) {
    if (numAttr <= 0 || !attrNames || !dataTypes) return NULL;

    Schema *s = (Schema*)malloc(sizeof(Schema));
    if (!s) return NULL;

    s->numAttr = numAttr;
    s->attrNames = attrNames;
    s->dataTypes = dataTypes;
    s->typeLength = typeLength;
    s->keySize = keySize;
    s->keyAttrs = keys;
    return s;
}

extern RC freeSchema(Schema *schema) {
    if (!schema) return RC_INVALID_PARAMETER;
    free(schema);
    return RC_OK;
}

extern RC createRecord(Record **record, Schema *schema) {
    if (!record || !schema) return RC_INVALID_PARAMETER;

    Record *r = (Record*)malloc(sizeof(Record));
    if (!r) return RC_MEMORY_ALLOCATION_ERROR;

    int rs = getRecordSize(schema);
    if (rs <= 0) { free(r); return RC_ERROR; }

    r->data = (char*)malloc(rs);
    if (!r->data) { free(r); return RC_MEMORY_ALLOCATION_ERROR; }

    r->id.page = -1;
    r->id.slot = -1;

    // Initialize: tombstone FREE + zero rest
    memset(r->data, 0, rs);
    r->data[0] = rm_marker(RM_FREE);

    *record = r;
    return RC_OK;
}

static RC getAttributeOffset(Schema *schema, int attrNum, int *result) {
    if (!schema || !result || attrNum < 0 || attrNum >= schema->numAttr) return RC_ERROR;
    int off = 1; // past tombstone
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_STRING: off += schema->typeLength[i]; break;
            case DT_INT:    off += (int)sizeof(int);      break;
            case DT_FLOAT:  off += (int)sizeof(float);    break;
            case DT_BOOL:   off += (int)sizeof(bool);     break;
            default: return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE;
        }
    }
    *result = off;
    return RC_OK;
}

extern RC freeRecord(Record *record) {
    if (!record) return RC_INVALID_PARAMETER;
    if (record->data) free(record->data);
    free(record);
    return RC_OK;
}

extern RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    if (!record || !schema || !value) return RC_INVALID_PARAMETER;
    if (attrNum < 0 || attrNum >= schema->numAttr) return RC_RM_NO_MORE_TUPLES;

    int off = 0;
    RC rc = getAttributeOffset(schema, attrNum, &off);
    if (rc != RC_OK) return rc;

    Value *val = (Value*)malloc(sizeof(Value));
    if (!val) return RC_MEMORY_ALLOCATION_ERROR;

    char *data = record->data + off;

    switch (schema->dataTypes[attrNum]) {
        case DT_STRING: {
            int len = schema->typeLength[attrNum];
            val->v.stringV = (char*)malloc(len + 1);
            if (!val->v.stringV) { free(val); return RC_MEMORY_ALLOCATION_ERROR; }
            strncpy(val->v.stringV, data, len);
            val->v.stringV[len] = '\0';
            val->dt = DT_STRING;
        } break;
        case DT_INT: {
            int x = 0; memcpy(&x, data, sizeof(int));
            val->v.intV = x; val->dt = DT_INT;
        } break;
        case DT_FLOAT: {
            float x = 0; memcpy(&x, data, sizeof(float));
            val->v.floatV = x; val->dt = DT_FLOAT;
        } break;
        case DT_BOOL: {
            bool x = false; memcpy(&x, data, sizeof(bool));
            val->v.boolV = x; val->dt = DT_BOOL;
        } break;
        default:
            free(val);
            return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE;
    }

    *value = val;
    return RC_OK;
}

extern RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    if (!record || !schema || !value) return RC_INVALID_PARAMETER;
    if (attrNum < 0 || attrNum >= schema->numAttr) return RC_RM_NO_MORE_TUPLES;

    int off = 0;
    RC rc = getAttributeOffset(schema, attrNum, &off);
    if (rc != RC_OK) return rc;

    char *data = record->data + off;

    switch (schema->dataTypes[attrNum]) {
        case DT_STRING: {
            int len = schema->typeLength[attrNum];
            strncpy(data, value->v.stringV, len);
        } break;
        case DT_INT:   *(int*)data   = value->v.intV;   break;
        case DT_FLOAT: *(float*)data = value->v.floatV; break;
        case DT_BOOL:  *(bool*)data  = value->v.boolV;  break;
        default: return RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE;
    }

    return RC_OK;
}

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "storage_mgr.h"

// ================= Frame and State Structures =================

typedef struct Frame {
    SM_PageHandle data;
    PageNumber pageNum;
    bool isDirty;
    int pinCount;
    int refCount;
    int lastTouch;
    struct Frame *next;
    struct Frame *prev;
} Frame;

typedef struct BpState {
    Frame *head;
    Frame *tail;
    Frame *fifoPtr;
    int used;
    int capacity;
    int reads;
    int writes;
    int clockIdx;
    int tick;
} BpState;

// ================= Helper Functions =================

static Frame *fr_new(SM_PageHandle data, PageNumber pn) {
    Frame *f = malloc(sizeof(Frame));
    if (!f) return NULL;
    f->data = data;
    f->pageNum = pn;
    f->isDirty = false;
    f->pinCount = 1;
    f->refCount = 1;
    f->lastTouch = 0;
    f->next = f->prev = NULL;
    return f;
}

static Frame *fr_find(BpState *st, PageNumber pn) {
    for (Frame *c = st->head; c; c = c->next)
        if (c->pageNum == pn) return c;
    return NULL;
}

static void fr_push_back(BpState *st, Frame *f) {
    if (!st->head) st->head = st->tail = f;
    else {
        st->tail->next = f;
        f->prev = st->tail;
        st->tail = f;
    }
    st->used++;
}

// ================= Disk I/O Helpers =================

static RC io_read_page(const char *file, PageNumber pn, SM_PageHandle buf, int *reads) {
    SM_FileHandle fh = {0};
    RC rc = openPageFile((char *)file, &fh);
    if (rc != RC_OK) return rc;

    rc = ensureCapacity(pn + 1, &fh);
    if (rc != RC_OK) { closePageFile(&fh); return rc; }

    rc = readBlock(pn, &fh, buf);
    if (rc != RC_OK) snprintf(buf, PAGE_SIZE, "Page-%d", pn);

    closePageFile(&fh);
    if (reads) (*reads)++;
    return RC_OK;
}

static RC io_write_page(const char *file, PageNumber pn, SM_PageHandle buf, int *writes) {
    SM_FileHandle fh = {0};
    RC rc = openPageFile((char *)file, &fh);
    if (rc != RC_OK) return rc;

    rc = ensureCapacity(pn + 1, &fh);
    if (rc == RC_OK) rc = writeBlock(pn, &fh, buf);

    closePageFile(&fh);
    if (rc == RC_OK && writes) (*writes)++;
    return rc;
}

// ================= Buffer Manager API =================

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy, void *stratData) {
    (void)stratData;
    if (!bm || !pageFileName || numPages <= 0) return RC_ERROR;

    initStorageManager();

    BpState *st = malloc(sizeof(BpState));
    if (!st) return RC_ERROR;

    st->head = st->tail = st->fifoPtr = NULL;
    st->used = 0;
    st->capacity = numPages;
    st->reads = st->writes = 0;
    st->clockIdx = st->tick = 0;

    bm->pageFile = strdup(pageFileName); // own copy to avoid dangling pointer
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = st;

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return RC_OK;

    BpState *st = (BpState *)bm->mgmtData;
    forceFlushPool(bm);

    for (Frame *c = st->head; c; c = c->next)
        if (c->pinCount > 0)
            return RC_PINNED_PAGES_IN_BUFFER;

    Frame *cur = st->head;
    while (cur) {
        Frame *nxt = cur->next;
        free(cur->data);
        free(cur);
        cur = nxt;
    }

    free(st);
    bm->mgmtData = NULL;

    if (bm->pageFile) {
        free(bm->pageFile);
        bm->pageFile = NULL;
    }

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm) {
    if (!bm || !bm->mgmtData) return RC_OK;
    BpState *st = (BpState *)bm->mgmtData;

    for (Frame *c = st->head; c; c = c->next) {
        if (c->isDirty && c->pinCount == 0) {
            RC rc = io_write_page(bm->pageFile, c->pageNum, c->data, &st->writes);
            if (rc != RC_OK) return rc;
            c->isDirty = false;
        }
    }
    return RC_OK;
}

// ================= Page Operations =================

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BpState *st = (BpState *)bm->mgmtData;
    Frame *f = fr_find(st, page->pageNum);
    if (!f) return RC_ERROR;
    f->isDirty = true;
    return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BpState *st = (BpState *)bm->mgmtData;
    Frame *f = fr_find(st, page->pageNum);
    if (!f || f->pinCount <= 0) return RC_ERROR;
    f->pinCount--;
    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BpState *st = (BpState *)bm->mgmtData;
    Frame *f = fr_find(st, page->pageNum);
    if (!f) return RC_ERROR;

    RC rc = io_write_page(bm->pageFile, f->pageNum, f->data, &st->writes);
    if (rc != RC_OK) return rc;
    f->isDirty = false;
    return RC_OK;
}

// ================= Pinning Pages =================

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
    BpState *st = (BpState *)bm->mgmtData;

    // check if already in buffer
    Frame *hit = fr_find(st, pageNum);
    if (hit) {
        hit->pinCount++;
        hit->refCount++;
        st->tick++;
        hit->lastTouch = st->tick;
        page->pageNum = pageNum;
        page->data = hit->data;
        return RC_OK;
    }

    // free slot?
    if (st->used < st->capacity) {
        SM_PageHandle buf = malloc(PAGE_SIZE);
        if (!buf) return RC_ERROR;
        memset(buf, 0, PAGE_SIZE);

        RC rc = io_read_page(bm->pageFile, pageNum, buf, &st->reads);
        if (rc != RC_OK) { free(buf); return rc; }

        Frame *f = fr_new(buf, pageNum);
        st->tick++; f->lastTouch = st->tick;
        fr_push_back(st, f);

        page->pageNum = pageNum;
        page->data = buf;
        return RC_OK;
    }

    // no free slot — replacement (FIFO for simplicity)
    Frame *victim = st->head;
    while (victim && victim->pinCount > 0) victim = victim->next;
    if (!victim) return RC_PINNED_PAGES_IN_BUFFER;

    if (victim->isDirty)
        io_write_page(bm->pageFile, victim->pageNum, victim->data, &st->writes);

    memset(victim->data, 0, PAGE_SIZE);
    RC rc = io_read_page(bm->pageFile, pageNum, victim->data, &st->reads);
    if (rc != RC_OK) return rc;

    victim->pageNum = pageNum;
    victim->pinCount = 1;
    victim->refCount = 1;
    st->tick++; victim->lastTouch = st->tick;

    page->pageNum = pageNum;
    page->data = victim->data;
    return RC_OK;
}

// ================= Stats Accessors =================

PageNumber *getFrameContents(BM_BufferPool *const bm) {
    BpState *st = (BpState *)bm->mgmtData;
    PageNumber *res = calloc(st->capacity, sizeof(PageNumber));
    Frame *c = st->head;
    for (int i = 0; c && i < st->capacity; i++, c = c->next)
        res[i] = c->pageNum;
    return res;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    BpState *st = (BpState *)bm->mgmtData;
    bool *res = calloc(st->capacity, sizeof(bool));
    Frame *c = st->head;
    for (int i = 0; c && i < st->capacity; i++, c = c->next)
        res[i] = c->isDirty;
    return res;
}

int *getFixCounts(BM_BufferPool *const bm) {
    BpState *st = (BpState *)bm->mgmtData;
    int *res = calloc(st->capacity, sizeof(int));
    Frame *c = st->head;
    for (int i = 0; c && i < st->capacity; i++, c = c->next)
        res[i] = c->pinCount;
    return res;
}

int getNumReadIO(BM_BufferPool *const bm) {
    BpState *st = (BpState *)bm->mgmtData;
    return st ? st->reads : 0;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    BpState *st = (BpState *)bm->mgmtData;
    return st ? st->writes : 0;
}

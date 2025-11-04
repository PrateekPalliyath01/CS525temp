// storage_mgr.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "storage_mgr.h"
#include "dberror.h"

#ifndef PAGE_SIZE
#define PAGE_SIZE 4096
#endif

/* ========================= Helpers ========================= */

static long file_size_bytes(const char *path) {
    struct stat st;
    return (path && stat(path, &st) == 0) ? (long)st.st_size : -1L;
}

static int bytes_to_pages(long sz) {
    if (sz <= 0) return 1;
    long pages = (sz + PAGE_SIZE - 1) / PAGE_SIZE;
    if (pages < 1) pages = 1;
    return (int)pages;
}

static RC validate_read_args(SM_FileHandle *fHandle, int pageNum, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    if (!fHandle->mgmtInfo) return RC_FILE_HANDLE_NOT_INIT;
    if (!memPage) return RC_WRITE_FAILED;
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) return RC_READ_NON_EXISTING_PAGE;
    return RC_OK;
}

/* ========================= API ========================= */

void initStorageManager(void) {
    /* no-op for this implementation */
}

/* Create a new page file with exactly one zeroed page. */
RC createPageFile(char *fileName) {
    if (!fileName) return RC_FILE_NOT_FOUND;

    FILE *fp = fopen(fileName, "wb");           // create/truncate, binary
    if (!fp) return RC_FILE_NOT_FOUND;

    void *zero = calloc(1, PAGE_SIZE);
    if (!zero) { fclose(fp); return RC_WRITE_FAILED; }

    size_t n = fwrite(zero, 1, PAGE_SIZE, fp);
    free(zero);
    if (n != PAGE_SIZE) { fclose(fp); return RC_WRITE_FAILED; }

    fflush(fp);
    fclose(fp);
    return RC_OK;
}

/* Open an existing page file and initialize the handle. */
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    if (!fileName || !fHandle) return RC_FILE_HANDLE_NOT_INIT;

    long sz = file_size_bytes(fileName);
    if (sz < 0) return RC_FILE_NOT_FOUND;

    FILE *fp = fopen(fileName, "rb+");          // read/write, binary
    if (!fp) return RC_FILE_NOT_FOUND;

    fHandle->mgmtInfo = fp;
    fHandle->fileName = strdup(fileName);       // own a heap copy; free in closePageFile
    if (!fHandle->fileName) { fclose(fp); return RC_WRITE_FAILED; }

    fHandle->totalNumPages = bytes_to_pages(sz);
    fHandle->curPagePos    = 0;

    return RC_OK;
}

/* Close the page file; free owned resources. */
RC closePageFile(SM_FileHandle *fHandle) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fp && fclose(fp) != 0) return RC_FILE_CLOSE_FAILED;

    fHandle->mgmtInfo = NULL;

    if (fHandle->fileName) {
        free(fHandle->fileName);
        fHandle->fileName = NULL;
    }
    return RC_OK;
}

/* Destroy (delete) the page file. */
RC destroyPageFile(char *fileName) {
    if (!fileName) return RC_FILE_NOT_FOUND;
    return (remove(fileName) == 0) ? RC_OK : RC_FILE_NOT_FOUND;
}

/* Read page 'pageNum' into 'memPage'. */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC rc = validate_read_args(fHandle, pageNum, memPage);
    if (rc != RC_OK) return rc;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, (long)pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return RC_READ_NON_EXISTING_PAGE;

    size_t n = fread(memPage, 1, PAGE_SIZE, fp);
    if (n != PAGE_SIZE) return RC_READ_NON_EXISTING_PAGE;

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

/* Return the current page position. */
int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle ? fHandle->curPagePos : -1;
}

/* Convenience readers. */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    int prev = fHandle->curPagePos - 1;
    return (prev >= 0) ? readBlock(prev, fHandle, memPage) : RC_READ_NON_EXISTING_PAGE;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    int next = fHandle->curPagePos + 1;
    return (next < fHandle->totalNumPages) ? readBlock(next, fHandle, memPage)
                                           : RC_READ_NON_EXISTING_PAGE;
}

RC readLastBlock  (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/* Write page 'pageNum' from 'memPage' into the file. */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    if (!fHandle->mgmtInfo) return RC_FILE_HANDLE_NOT_INIT;
    if (!memPage) return RC_WRITE_FAILED;
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) return RC_READ_NON_EXISTING_PAGE;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, (long)pageNum * PAGE_SIZE, SEEK_SET) != 0) return RC_WRITE_FAILED;

    size_t n = fwrite(memPage, 1, PAGE_SIZE, fp);
    if (n != PAGE_SIZE) return RC_WRITE_FAILED;

    fflush(fp);
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

/* Write to the current page position. */
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/* Append one zeroed page at the end of the file. */
RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    if (!fHandle->mgmtInfo) return RC_FILE_HANDLE_NOT_INIT;

    void *zero = calloc(1, PAGE_SIZE);
    if (!zero) return RC_WRITE_FAILED;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, 0, SEEK_END) != 0) { free(zero); return RC_WRITE_FAILED; }

    size_t n = fwrite(zero, 1, PAGE_SIZE, fp);
    free(zero);
    if (n != PAGE_SIZE) return RC_WRITE_FAILED;

    fflush(fp);

    fHandle->totalNumPages += 1;
    fHandle->curPagePos = fHandle->totalNumPages - 1;
    return RC_OK;
}

/* Ensure the file has at least 'numPages' pages; append zero pages if needed. */
RC ensureCapacity(int numPages, SM_FileHandle *fHandle) {
    if (!fHandle) return RC_FILE_HANDLE_NOT_INIT;
    if (!fHandle->mgmtInfo) return RC_FILE_HANDLE_NOT_INIT;
    if (numPages <= 0) return RC_OK;

    if (fHandle->totalNumPages >= numPages) return RC_OK;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, 0, SEEK_END) != 0) return RC_WRITE_FAILED;

    int need = numPages - fHandle->totalNumPages;

    /* write pages one-by-one to keep memory small and handle partial failures */
    void *zero = calloc(1, PAGE_SIZE);
    if (!zero) return RC_WRITE_FAILED;

    for (int i = 0; i < need; i++) {
        size_t n = fwrite(zero, 1, PAGE_SIZE, fp);
        if (n != PAGE_SIZE) { free(zero); return RC_WRITE_FAILED; }
    }
    free(zero);
    fflush(fp);

    fHandle->totalNumPages = numPages;
    fHandle->curPagePos    = numPages - 1;
    return RC_OK;
}




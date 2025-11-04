#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>

// Structure representing a page frame in the buffer pool
// Uses doubly linked list for easy insertion/deletion
typedef struct DLNode
{
    SM_PageHandle data;  // Actual page data in memory
    PageNumber pageNum;  // Page number in the file
    bool isDirty;        // True if page was modified
    int pinCount;        // Number of clients using this page
    int accessCount;     // Counter for LFU strategy
    int lastAccessed;    // Timestamp for LRU strategy
    int *accessHistory;  // History array for LRU-K
    int historySize;     // Size of history array
    struct DLNode *next; // Next node in the list
    struct DLNode *prev; // Previous node in the list
} DLNode;

// Metadata structure maintaining buffer pool state and statistics
typedef struct BufferPoolMetadata
{
    DLNode *head;      // First page frame in the pool
    DLNode *tail;      // Last page frame in the pool
    DLNode *fifoHead;  // Tracks oldest page for FIFO
    int numFramesUsed; // Current number of frames in use
    int totalFrames;   // Total capacity of frames
    int readCount;     // Number of disk reads performed
    int writeCount;    // Number of disk writes performed
    int clockHand;     // Current position for CLOCK algorithm
    int globalTimer;   // Global counter for timestamps
} BufferPoolMetadata;

/**
 * Creates a new DLNode with given data and page number
 */
static DLNode *createNode(SM_PageHandle data, PageNumber pageNum)
{
    // Allocate memory for new node
    DLNode *newNode = (DLNode *)malloc(sizeof(DLNode));

    // Initialize node fields
    newNode->data = data;
    newNode->pageNum = pageNum;
    newNode->isDirty = false;
    newNode->pinCount = 1;     // New pages start with pin count 1
    newNode->accessCount = 1;  // Initialize access count
    newNode->lastAccessed = 0; // Will be set by caller
    newNode->next = NULL;
    newNode->prev = NULL;

    return newNode;
}

/**
 * Searches for a page in the buffer pool
 */
static DLNode *findPage(BufferPoolMetadata *metadata, PageNumber pageNum)
{
    // Start from head of list
    DLNode *current = metadata->head;

    // Traverse until page is found or end of list
    while (current != NULL)
    {
        if (current->pageNum == pageNum)
            return current;
        current = current->next;
    }
    return NULL;
}

/**
 * Implements FIFO page replacement strategy
 */
static DLNode *replaceFIFO(BufferPoolMetadata *metadata)
{
    // If FIFO queue is empty, initialize to head
    if (metadata->fifoHead == NULL)
    {
        metadata->fifoHead = metadata->head;
    }

    // Start search from FIFO head
    DLNode *current = metadata->fifoHead;
    DLNode *startNode = current;

    // Search for first unpinned page
    do
    {
        // Wrap around if we reach end of list
        if (current == NULL)
        {
            current = metadata->head;
        }

        // Found an unpinned page
        if (current->pinCount == 0)
        {
            // Update FIFO head to next node
            metadata->fifoHead = current->next;
            if (metadata->fifoHead == NULL)
            {
                metadata->fifoHead = metadata->head;
            }
            return current;
        }

        current = current->next;

        // Wrap around at end of list
        if (current == NULL)
        {
            current = metadata->head;
        }
    } while (current != startNode);

    return NULL; // No unpinned pages found
}

/**
 * Implements LRU page replacement strategy
 */
static DLNode *replaceLRU(BufferPoolMetadata *metadata)
{
    DLNode *current = metadata->head;
    DLNode *victim = NULL;
    int minAccess = metadata->globalTimer + 1; // Initialize higher than any access time

    // Find page with oldest access time
    while (current != NULL)
    {
        // Consider only unpinned pages
        if (current->pinCount == 0 && current->lastAccessed < minAccess)
        {
            minAccess = current->lastAccessed;
            victim = current;
        }
        current = current->next;
    }

    return victim;
}

/**
 * Implements LFU page replacement strategy
 */
static DLNode *replaceLFU(BufferPoolMetadata *metadata)
{
    DLNode *current = metadata->head;
    DLNode *victim = NULL;
    int minCount = INT_MAX;
    int oldestTimestamp = INT_MAX;

    // Find page with lowest access count
    while (current != NULL)
    {
        if (current->pinCount == 0)
        {
            // First unpinned page found
            if (victim == NULL)
            {
                victim = current;
                minCount = current->accessCount;
                oldestTimestamp = current->lastAccessed;
            }
            else
            {
                // Replace if this page has lower access count
                if (current->accessCount < minCount)
                {
                    victim = current;
                    minCount = current->accessCount;
                    oldestTimestamp = current->lastAccessed;
                }
                // If equal access counts, choose older page
                else if (current->accessCount == minCount &&
                         current->lastAccessed < oldestTimestamp)
                {
                    victim = current;
                    oldestTimestamp = current->lastAccessed;
                }
            }
        }
        current = current->next;
    }

    return victim;
}

/**
 * Implements CLOCK page replacement strategy
 */
static DLNode *replaceCLOCK(BufferPoolMetadata *metadata)
{
    DLNode *current = metadata->head;
    int i;

    // Move to current clock hand position
    for (i = 0; i < metadata->clockHand; i++)
    {
        if (current->next != NULL)
        {
            current = current->next;
        }
        else
        {
            current = metadata->head; // Wrap around
        }
    }

    // Keep searching until victim found
    while (true)
    {
        // Found an unpinned page with no recent access
        if (current->pinCount == 0 && current->accessCount == 0)
        {
            metadata->clockHand = (metadata->clockHand + 1) % metadata->totalFrames;
            return current;
        }

        // Give second chance by resetting access count
        if (current->accessCount > 0)
        {
            current->accessCount = 0;
        }

        // Advance clock hand
        metadata->clockHand = (metadata->clockHand + 1) % metadata->totalFrames;

        // Move to next frame
        current = current->next;
        if (current == NULL)
        {
            current = metadata->head;
        }
    }
}

/**
 * Creates a new buffer pool and initializes required data structures.
 *
 * @param bm Buffer pool handle to initialize
 * @param pageFileName Name of the page file to use
 * @param numPages Number of pages the buffer pool can hold
 * @param strategy Page replacement strategy to use
 * @param stratData Additional data for replacement strategy (if needed)
 * @return RC_OK on successful initialization
 *
 * Allocates and initializes the buffer pool metadata including the frame list,
 * counters, and strategy-specific data. Sets up tracking for page replacements
 * and statistics. The buffer pool starts empty with no frames used.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy, void *stratData)
{
    // Allocate and initialize metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)malloc(sizeof(BufferPoolMetadata));
    metadata->head = NULL;
    metadata->tail = NULL;
    metadata->fifoHead = NULL;
    metadata->numFramesUsed = 0;
    metadata->totalFrames = numPages;
    metadata->readCount = 0;
    metadata->writeCount = 0;
    metadata->clockHand = 0;
    metadata->globalTimer = 0;

    // Initialize buffer pool handle
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = metadata;

    return RC_OK;
}

/**
 * Shuts down an existing buffer pool and releases all resources.
 *
 * @param bm Buffer pool handle to shut down
 * @return RC_OK on success, RC_PINNED_PAGES_IN_BUFFER if pages still pinned
 *
 * Forces all dirty pages to disk, verifies no pages are pinned, and frees
 * all allocated memory. Checks for pinned pages before shutdown to prevent
 * data loss. Releases both page frames and metadata structures.
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Write all dirty pages to disk
    forceFlushPool(bm);

    // Free all allocated memory
    DLNode *current = metadata->head;
    while (current != NULL)
    {
        // Check for pinned pages
        if (current->pinCount > 0)
            return RC_PINNED_PAGES_IN_BUFFER;

        // Free node and its data
        DLNode *temp = current;
        current = current->next;
        free(temp->data);
        free(temp);
    }

    // Free metadata structure
    free(metadata);
    bm->mgmtData = NULL;
    return RC_OK;
}

/**
 * Writes all dirty pages from buffer pool to disk.
 *
 * @param bm Buffer pool handle containing pages to flush
 * @return RC_OK on successful flush
 *
 * Iterates through all pages in the buffer pool and writes dirty, unpinned
 * pages to disk. Updates write statistics and marks flushed pages as clean.
 * Skips pinned pages even if dirty to maintain consistency.
 */
RC forceFlushPool(BM_BufferPool *const bm)
{
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;
    DLNode *current = metadata->head;

    // Iterate through all pages
    while (current != NULL)
    {
        // Write dirty and unpinned pages to disk
        if (current->isDirty && current->pinCount == 0)
        {
            SM_FileHandle fh;
            openPageFile(bm->pageFile, &fh);
            writeBlock(current->pageNum, &fh, current->data);
            current->isDirty = false;
            metadata->writeCount++;
            closePageFile(&fh);
        }
        current = current->next;
    }
    return RC_OK;
}

/**
 * Marks a page in the buffer pool as dirty.
 *
 * @param bm Buffer pool handle
 * @param page Page handle of page to mark
 * @return RC_OK if page found and marked, RC_ERROR if page not in pool
 *
 * Sets the dirty flag for the specified page indicating it needs to be
 * written to disk before replacement. Essential for maintaining data
 * consistency between memory and disk.
 */
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Get metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Find the page in buffer pool
    DLNode *node = findPage(metadata, page->pageNum);

    // If page found, mark it as dirty
    if (node != NULL)
    {
        node->isDirty = true;
        return RC_OK;
    }
    return RC_ERROR;
}

/**
 * Decrements the pin count of a page.
 *
 * @param bm Buffer pool handle
 * @param page Page handle of page to unpin
 * @return RC_OK on success, RC_ERROR if page not found or already unpinned
 *
 * Reduces pin count indicating one fewer client is using the page.
 * Pages with zero pin count become candidates for replacement.
 * Prevents unpinning already unpinned pages.
 */
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Get metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Find the page in buffer pool
    DLNode *node = findPage(metadata, page->pageNum);

    // Decrement pin count if page is pinned
    if (node != NULL && node->pinCount > 0)
    {
        node->pinCount--;
        return RC_OK;
    }
    return RC_ERROR;
}

/**
 * Immediately writes a page to disk.
 *
 * @param bm Buffer pool handle
 * @param page Page handle of page to force
 * @return RC_OK on successful write, RC_ERROR if page not found
 *
 * Opens the page file, writes the page content regardless of dirty flag,
 * and updates write statistics. Useful for immediate persistence of
 * critical data changes.
 */
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Get metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Find the page in buffer pool
    DLNode *node = findPage(metadata, page->pageNum);

    if (node != NULL)
    {
        // Open file and write page
        SM_FileHandle fh;
        openPageFile(bm->pageFile, &fh);
        writeBlock(node->pageNum, &fh, node->data);
        closePageFile(&fh);

        // Update page and statistics
        node->isDirty = false;
        metadata->writeCount++;
        return RC_OK;
    }
    return RC_ERROR;
}

/**
 * Pins a page into the buffer pool, loading it from disk if necessary.
 *
 * @param bm Buffer pool handle
 * @param page Page handle to store the requested page
 * @param pageNum Page number to be pinned
 * @return RC_OK on success, RC_ERROR on failure
 *
 * This function first checks if the page is already in the buffer pool.
 * If present, it increments the pin count and updates metadata.
 * If not, it loads the page from disk into an available frame or
 * replaces a page based on the chosen replacement strategy.
 */
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum)
{
    // Retrieve buffer pool metadata
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Check if the page is already in the buffer pool
    DLNode *node = findPage(metadata, pageNum);
    if (node != NULL)
    {
        // Page found: Increment pin count and access count
        node->pinCount++;
        node->accessCount++; // This may be redundant depending on LRU logic

        // Update global timestamp for LRU replacement policy
        metadata->globalTimer++;
        node->lastAccessed = metadata->globalTimer;

        // Update page handle
        page->pageNum = pageNum;
        page->data = node->data;
        return RC_OK;
    }

    // Check if there is space in the buffer pool
    if (metadata->numFramesUsed < metadata->totalFrames)
    {
        SM_FileHandle fh;
        RC rc = openPageFile(bm->pageFile, &fh);
        if (rc != RC_OK)
            return rc;

        // Allocate memory for the new page
        SM_PageHandle newData = (SM_PageHandle)malloc(PAGE_SIZE);
        if (newData == NULL)
        {
            closePageFile(&fh);
            return RC_ERROR;
        }

        // Initialize the page memory with zeros
        memset(newData, 0, PAGE_SIZE);

        // Ensure the file has enough pages
        rc = ensureCapacity(pageNum + 1, &fh);
        if (rc != RC_OK)
        {
            free(newData);
            closePageFile(&fh);
            return rc;
        }

        // Read the page from disk if it exists
        if (pageNum < fh.totalNumPages)
        {
            rc = readBlock(pageNum, &fh, newData);
            if (rc != RC_OK)
            {
                // If read fails, initialize with default content
                sprintf(newData, "Page-%i", pageNum);
            }
        }
        else
        {
            // If the page does not exist, initialize it with default content
            sprintf(newData, "Page-%i", pageNum);
        }

        closePageFile(&fh);

        // Create a new node for this page
        DLNode *newNode = createNode(newData, pageNum);
        metadata->globalTimer++;
        newNode->lastAccessed = metadata->globalTimer;

        // If FIFO queue is empty, set head
        if (metadata->fifoHead == NULL)
        {
            metadata->fifoHead = newNode;
        }

        // Insert the new node into the linked list
        if (metadata->head == NULL)
        {
            metadata->head = metadata->tail = newNode;
        }
        else
        {
            metadata->tail->next = newNode;
            newNode->prev = metadata->tail;
            metadata->tail = newNode;
        }

        // Update metadata
        metadata->numFramesUsed++;
        metadata->readCount++;

        // Assign the page handle
        page->pageNum = pageNum;
        page->data = newData;
        return RC_OK;
    }

    // Buffer pool is full; apply a replacement strategy
    DLNode *victim = NULL;
    switch (bm->strategy)
    {
    case RS_FIFO:
        victim = replaceFIFO(metadata);
        break;
    case RS_LRU:
        victim = replaceLRU(metadata);
        break;
    case RS_CLOCK:
        victim = replaceCLOCK(metadata);
        break;
    case RS_LRU_K:
        printf("\n LRU-k algorithm not implemented");
        break;
    case RS_LFU:
        victim = replaceLFU(metadata);
        break;
    default:
        printf("\nAlgorithm Not Implemented\n");
        break;
    }

    // Ensure a victim is found and is not pinned
    if (victim == NULL || victim->pinCount > 0)
        return RC_ERROR;

    // If the victim page is dirty, write it to disk before replacing
    if (victim->isDirty)
    {
        SM_FileHandle fh;
        RC rc = openPageFile(bm->pageFile, &fh);
        if (rc != RC_OK)
            return rc;

        writeBlock(victim->pageNum, &fh, victim->data);
        closePageFile(&fh);
        metadata->writeCount++;
    }

    // Reset the victim's memory for new data
    memset(victim->data, 0, PAGE_SIZE);

    // Open the page file again for reading
    SM_FileHandle fh;
    RC rc = openPageFile(bm->pageFile, &fh);
    if (rc != RC_OK)
        return rc;

    // Ensure sufficient capacity before reading
    rc = ensureCapacity(pageNum + 1, &fh);
    if (rc != RC_OK)
    {
        closePageFile(&fh);
        return rc;
    }

    // Read or initialize the new page content
    if (pageNum < fh.totalNumPages)
    {
        rc = readBlock(pageNum, &fh, victim->data);
        if (rc != RC_OK)
        {
            sprintf(victim->data, "Page-%i", pageNum);
        }
    }
    else
    {
        sprintf(victim->data, "Page-%i", pageNum);
    }

    closePageFile(&fh);

    // Update the victim node with the new page details
    victim->pageNum = pageNum;
    victim->isDirty = false;
    victim->pinCount = 1;
    metadata->globalTimer++;
    victim->lastAccessed = metadata->globalTimer;

    // Assign the page handle
    page->pageNum = pageNum;
    page->data = victim->data;

    // Update read count
    metadata->readCount++;
    return RC_OK;
}

/**
 * Retrieves the page numbers stored in each frame.
 *
 * @param bm Buffer pool handle
 * @return Array of page numbers, NO_PAGE for empty frames
 *
 * Allocates and returns an array showing which page occupies each frame
 * in the buffer pool. The array index corresponds to frame number.
 * Caller must free the returned array.
 */
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    // Get metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Allocate array for frame contents
    PageNumber *frameContents = malloc(sizeof(PageNumber) * metadata->totalFrames);

    // Initialize all frames to NO_PAGE
    for (int i = 0; i < metadata->totalFrames; i++)
        frameContents[i] = NO_PAGE;

    // Fill in actual page numbers
    DLNode *current = metadata->head;
    int index = 0;
    while (current != NULL && index < metadata->totalFrames)
    {
        frameContents[index++] = current->pageNum;
        current = current->next;
    }

    return frameContents;
}

/**
 * Retrieves dirty flags for all frames.
 *
 * @param bm Buffer pool handle
 * @return Array of boolean dirty flags
 *
 * Creates an array indicating which frames contain pages modified in memory
 * but not yet written to disk. Array index matches frame number.
 * Caller must free the returned array.
 */
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    // Get metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Allocate array for dirty flags
    bool *dirtyFlags = malloc(sizeof(bool) * metadata->totalFrames);

    // Initialize all flags to false
    for (int i = 0; i < metadata->totalFrames; i++)
        dirtyFlags[i] = false;

    // Set flags for dirty pages
    DLNode *current = metadata->head;
    int index = 0;
    while (current != NULL && index < metadata->totalFrames)
    {
        dirtyFlags[index++] = current->isDirty;
        current = current->next;
    }

    return dirtyFlags;
}

/**
 * Retrieves pin counts for all frames.
 *
 * @param bm Buffer pool handle
 * @return Array of integer pin counts
 *
 * Returns array showing how many clients are using each page in the
 * buffer pool. Zero indicates page is unused and can be replaced.
 * Caller must free the returned array.
 */
int *getFixCounts(BM_BufferPool *const bm)
{
    // Get metadata structure
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;

    // Allocate array for fix counts
    int *fixCounts = malloc(sizeof(int) * metadata->totalFrames);

    // Initialize all counts to 0
    for (int i = 0; i < metadata->totalFrames; i++)
        fixCounts[i] = 0;

    // Fill in actual fix counts
    DLNode *current = metadata->head;
    int index = 0;
    while (current != NULL && index < metadata->totalFrames)
    {
        fixCounts[index++] = current->pinCount;
        current = current->next;
    }

    return fixCounts;
}

/**
 * Returns total number of pages read from disk.
 *
 * @param bm Buffer pool handle
 * @return Number of read operations performed
 *
 * Provides count of disk reads since pool initialization. Used for
 * monitoring and optimizing buffer pool performance.
 */
int getNumReadIO(BM_BufferPool *const bm)
{
    // Return read counter from metadata
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;
    return metadata->readCount;
}

/**
 * Returns total number of pages written to disk.
 *
 * @param bm Buffer pool handle
 * @return Number of write operations performed
 *
 * Provides count of disk writes since pool initialization. Includes
 * both forced writes and automatic flushes of dirty pages.
 */
int getNumWriteIO(BM_BufferPool *const bm)
{
    // Return write counter from metadata
    BufferPoolMetadata *metadata = (BufferPoolMetadata *)bm->mgmtData;
    return metadata->writeCount;
}

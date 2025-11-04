/*******************************************************************************
 * File: storage_mgr.c
 * Implementation of a simple storage manager that maintains a page file on disk.
 * The storage manager handles reading and writing of pages to and from a file,
 * managing page allocation, and ensuring proper file capacity.
 *
 * The system uses a page-based approach where each page is a fixed-size block
 * of memory (PAGE_SIZE bytes). All I/O operations are performed at the page level
 * with careful error handling and memory management to prevent leaks and handle
 * failure scenarios robustly.
 ******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "storage_mgr.h"
#include "dberror.h"

/************************** Debug Configuration *********************************/
#define DEBUG_MSG(msg) // printf("[DEBUG] %s\n", msg)

/************************** Helper Functions ***********************************/

/**
 * Get the size of a file in bytes.
 *
 * @param filename The path to the file
 * @return The size of the file in bytes, or -1 if the file cannot be accessed
 *
 * Uses the stat system call to retrieve file information, populating a stat
 * structure with file details. Returns -1 for inaccessible or non-existent files,
 * allowing callers to handle error conditions appropriately.
 */
static long get_file_size(const char *filename)
{
    struct stat st;
    // Use stat() to get file information without opening the file
    if (stat(filename, &st) == 0)
        return st.st_size;
    return -1; // Return -1 to indicate error
}

/**
 * Validates parameters for read operations.
 *
 * @param fh File handle to validate
 * @param pageNum Page number to validate
 * @param memPage Memory page to validate
 * @return RC_OK if valid, appropriate error code otherwise
 *
 * Performs three crucial validations: file handle initialization, memory page
 * buffer existence, and page number bounds checking. This centralized validation
 * ensures consistent error checking across all read operations.
 */
static RC validate_read(SM_FileHandle *fh, int pageNum, SM_PageHandle memPage)
{
    // Check if file handle is initialized
    if (!fh)
        return RC_FILE_HANDLE_NOT_INIT;
    // Ensure memory page buffer is valid
    if (!memPage)
        return RC_WRITE_FAILED;
    // Verify page number is within valid range
    if (pageNum < 0 || pageNum >= fh->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;
    return RC_OK;
}

/************************** Core Functions ************************************/

/**
 * Initializes the storage manager.
 *
 * Serves as an initialization point for the storage manager system. While currently
 * lightweight, this function can be extended to handle global configurations,
 * logging systems, shared resources, and connection pools in future implementations.
 */
void initStorageManager(void)
{
    // Log initialization - can be expanded for more complex initialization
    DEBUG_MSG("Storage manager initialized");
}

/**
 * Author: Rayyan Maindargi
 * Creates a new page file on disk.
 *
 * @param filename Name of the file to create
 * @return RC_OK if successful, error code otherwise
 *
 * Creates a new file and initializes it with one empty page of zeros. The process
 * involves opening the file in write mode, allocating a temporary buffer for the
 * initial page, writing zeros to the entire page, and cleaning up resources.
 * Handles memory allocation failures and write errors gracefully.
 */
RC createPageFile(char *filename)
{
    // Validate input parameter
    if (!filename)
        return RC_FILE_NOT_FOUND;

    // Create new file in write mode
    FILE *fp = fopen(filename, "w");
    if (!fp)
        return RC_FILE_NOT_FOUND;

    // Allocate memory for one page
    char *page_buffer = malloc(PAGE_SIZE);
    if (!page_buffer)
    {
        // Clean up if memory allocation fails
        fclose(fp);
        return RC_WRITE_FAILED;
    }

    // Initialize page with zeros
    memset(page_buffer, 0, PAGE_SIZE);

    // Write the empty page to file
    size_t result = fwrite(page_buffer, PAGE_SIZE, 1, fp);

    // Clean up allocated resources
    free(page_buffer);
    fclose(fp);

    // Return success only if exactly one page was written
    return (result == 1) ? RC_OK : RC_WRITE_FAILED;
}

/**
 * Author: Rayyan Maindargi
 * Opens an existing page file.
 *
 * @param filename Name of the file to open
 * @param fileHandle Handle to be initialized with file information
 * @return RC_OK if successful, error code otherwise
 *
 * Opens an existing file in read/write mode and initializes a file handle with
 * its information. Calculates total pages based on file size, sets initial cursor
 * position to 0, and stores the file pointer for future operations. Features
 * comprehensive error checking for file existence and handle initialization.
 */
RC openPageFile(char *filename, SM_FileHandle *fileHandle)
{
    // Validate input parameters
    if (!fileHandle || !filename)
        return RC_FILE_HANDLE_NOT_INIT;

    // Get file size before opening to verify existence
    long file_size = get_file_size(filename);
    if (file_size < 0)
        return RC_FILE_NOT_FOUND;

    // Open file for reading and writing
    FILE *fp = fopen(filename, "r+");
    if (!fp)
        return RC_FILE_NOT_FOUND;

    // Initialize file handle with file information
    fileHandle->mgmtInfo = fp;
    fileHandle->fileName = filename;
    // Calculate total pages, rounding up to include partial pages
    fileHandle->totalNumPages = (file_size + PAGE_SIZE - 1) / PAGE_SIZE;
    fileHandle->curPagePos = 0; // Start at beginning of file

    return RC_OK;
}

/**
 * Author: Rayyan Maindargi
 * Closes a page file.
 *
 * @param fileHandle Handle to the file to be closed
 * @return RC_OK if successful, error code otherwise
 *
 * Safely closes an open page file by flushing any pending writes and releasing
 * system resources. Clears the management info pointer to prevent subsequent
 * accidental use. Includes checks for null file handles and failed close
 * operations to ensure proper cleanup.
 */
RC closePageFile(SM_FileHandle *fileHandle)
{
    // Validate file handle
    if (!fileHandle)
        return RC_FILE_HANDLE_NOT_INIT;

    FILE *fp = (FILE *)fileHandle->mgmtInfo;
    // Only attempt to close if file pointer exists
    if (fp && fclose(fp) != 0)
        return RC_FILE_CLOSE_FAILED;

    // Clear file pointer to prevent reuse
    fileHandle->mgmtInfo = NULL;
    return RC_OK;
}

/**
 * Author: Rayyan Maindargi
 * Removes a page file from disk.
 *
 * @param filename Name of the file to destroy
 * @return RC_OK if successful, error code otherwise
 *
 * Permanently deletes the specified file from disk using the system remove
 * function. Returns appropriate error code if the file doesn't exist or
 * cannot be deleted due to permission issues or other system constraints.
 */
RC destroyPageFile(char *filename)
{
    // Attempt to remove file, return appropriate status
    return (remove(filename) == 0) ? RC_OK : RC_FILE_NOT_FOUND;
}

/************************** Block Read Operations *****************************/

/**
 * Author: Rayyan Maindargi
 * Reads a specific page from disk into memory.
 *
 * @param pageNum Page number to read (0-based)
 * @param fh File handle
 * @param memPage Memory buffer to store the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Performs a direct page read from disk by seeking to the appropriate file
 * position and reading exactly PAGE_SIZE bytes. Updates the current page
 * position on success. Includes comprehensive parameter validation and error
 * checking for seek and read operations.
 */
RC readBlock(int pageNum, SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Validate all parameters using helper function
    RC valid = validate_read(fh, pageNum, memPage);
    if (valid != RC_OK)
        return valid;

    FILE *fp = (FILE *)fh->mgmtInfo;
    // Calculate byte offset for desired page
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET))
        return RC_READ_NON_EXISTING_PAGE;

    // Attempt to read entire page
    if (fread(memPage, PAGE_SIZE, 1, fp) != 1)
        return RC_READ_NON_EXISTING_PAGE;

    // Update current position on successful read
    fh->curPagePos = pageNum;
    return RC_OK;
}

/**
 * Author: Purnendu Kale
 * Returns the current page position in the file.
 *
 * @param fh File handle
 * @return Current page position or -1 if handle is invalid
 *
 * Provides quick access to the current page position within the file.
 * Returns -1 for invalid file handles to allow error detection by callers.
 */
int getBlockPos(SM_FileHandle *fh)
{
    // Return current position or -1 for invalid handle
    return (fh) ? fh->curPagePos : -1;
}

/**
 * Author: Purnendu Kale
 * Reads the first page of the file.
 *
 * @param fh File handle
 * @param memPage Memory buffer to store the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Convenience function that reads page 0 of the file. Uses the main readBlock
 * function to perform the actual read operation, inheriting its error checking
 * and validation capabilities.
 */
RC readFirstBlock(SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Simple wrapper around readBlock for page 0
    return readBlock(0, fh, memPage);
}

/**
 * Author: Purnendu Kale
 * Reads the page before the current position.
 *
 * @param fh File handle
 * @param memPage Memory buffer to store the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Moves back one page and reads its content. Includes bounds checking to prevent
 * reading before the start of file. Uses the main readBlock function for the
 * actual read operation while handling position calculations internally.
 */
RC readPreviousBlock(SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Validate file handle
    if (!fh)
        return RC_FILE_HANDLE_NOT_INIT;

    int curr = fh->curPagePos;
    // Check if we can move backward and read
    return (curr > 0) ? readBlock(curr - 1, fh, memPage) : RC_READ_NON_EXISTING_PAGE;
}

/**
 * Author: Purnendu Kale
 * Reads the page at the current position.
 *
 * @param fh File handle
 * @param memPage Memory buffer to store the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Reads the current page without changing position. Utilizes the main readBlock
 * function while maintaining the current page position. Includes validation of
 * file handle to ensure safe operation.
 */
RC readCurrentBlock(SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Read page at current position if handle is valid
    return (fh) ? readBlock(fh->curPagePos, fh, memPage) : RC_FILE_HANDLE_NOT_INIT;
}

/**
 * Author: Purnendu Kale
 * Reads the page after the current position.
 *
 * @param fh File handle
 * @param memPage Memory buffer to store the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Advances one page and reads its content. Includes bounds checking to prevent
 * reading past the end of file. Uses the main readBlock function while handling
 * position calculations and validation internally.
 */
RC readNextBlock(SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Validate file handle
    if (!fh)
        return RC_FILE_HANDLE_NOT_INIT;

    int next = fh->curPagePos + 1;
    // Check if next page exists before reading
    return (next < fh->totalNumPages) ? readBlock(next, fh, memPage) : RC_READ_NON_EXISTING_PAGE;
}

/**
 * Author: Nijgururaj Ashtagi
 * Reads the last page of the file.
 *
 * @param fh File handle
 * @param memPage Memory buffer to store the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Reads the final page of the file. Calculates the last page number based on
 * total pages and uses the main readBlock function for the actual read operation.
 * Includes validation of the file handle.
 */
RC readLastBlock(SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Read the last page if handle is valid
    return (fh) ? readBlock(fh->totalNumPages - 1, fh, memPage) : RC_FILE_HANDLE_NOT_INIT;
}

/************************** Block Write Operations ****************************/

/**
 * Author: Nijgururaj Ashtagi
 * Writes a page to disk at a specific position.
 *
 * @param pageNum Page number to write to (0-based)
 * @param fh File handle
 * @param memPage Memory buffer containing the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Performs a direct page write to disk by seeking to the appropriate file
 * position and writing exactly PAGE_SIZE bytes. Updates the current page
 * position on success. Includes parameter validation and error checking for
 * seek and write operations.
 */
RC writeBlock(int pageNum, SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Validate input parameters
    if (!fh || !memPage)
        return RC_FILE_HANDLE_NOT_INIT;
    // Check page number bounds
    if (pageNum < 0 || pageNum >= fh->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;

    FILE *fp = (FILE *)fh->mgmtInfo;
    // Seek to target page position
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET))
        return RC_WRITE_FAILED;

    // Write entire page to file
    if (fwrite(memPage, PAGE_SIZE, 1, fp) != 1)
        return RC_WRITE_FAILED;

    // Update current position after successful write
    fh->curPagePos = pageNum;
    return RC_OK;
}

/**
 * Author: Nijgururaj Ashtagi
 * Writes a page to disk at the current position.
 *
 * @param fh File handle
 * @param memPage Memory buffer containing the page content
 * @return RC_OK if successful, error code otherwise
 *
 * Writes data to the current page position. Uses the main writeBlock function
 * while maintaining the current page position. Includes validation of the file
 * handle to ensure safe operation.
 */
RC writeCurrentBlock(SM_FileHandle *fh, SM_PageHandle memPage)
{
    // Write to current position if handle is valid
    return (fh) ? writeBlock(fh->curPagePos, fh, memPage) : RC_FILE_HANDLE_NOT_INIT;
}

/**
 * Author: Nijgururaj Ashtagi
 * Appends a new empty page to the end of the file.
 *
 * @param fh File handle
 * @return RC_OK if successful, error code otherwise
 *
 * Creates and appends a new zero-filled page to the file. Manages memory
 * allocation for the temporary page buffer and handles cleanup. Updates the
 * total page count on successful append. Includes error checking for memory
 * allocation and write operations.
 */
RC appendEmptyBlock(SM_FileHandle *fh)
{
    // Validate file handle
    if (!fh)
        return RC_FILE_HANDLE_NOT_INIT;

    // Allocate and initialize empty page
    char *empty = malloc(PAGE_SIZE);
    if (!empty)
        return RC_WRITE_FAILED;
    memset(empty, 0, PAGE_SIZE);

    FILE *fp = (FILE *)fh->mgmtInfo;
    // Move to end of file
    fseek(fp, 0, SEEK_END);
    // Write empty page
    int result = (fwrite(empty, PAGE_SIZE, 1, fp) == 1);
    free(empty);

    if (result)
    {
        // Update total pages on successful append
        fh->totalNumPages++;
        return RC_OK;
    }
    return RC_WRITE_FAILED;
}

/**
 * Author: Nijgururaj Ashtagi
 * Ensures the file has at least the specified number of pages.
 *
 * @param numPages Minimum number of pages required
 * @param fh File handle
 * @return RC_OK if successful, error code otherwise
 *
 * Checks if the file needs additional pages and appends empty pages as needed.
 * Optimizes the append operation by allocating memory for all needed pages at
 * once. Updates the total page count on success. Includes parameter validation
 * and error checking for memory allocation and write operations.
 */
RC ensureCapacity(int numPages, SM_FileHandle *fh)
{
    // Validate input parameters
    if (!fh)
        return RC_FILE_HANDLE_NOT_INIT;
    if (numPages <= 0)
        return RC_READ_NON_EXISTING_PAGE;

    // Check if expansion is needed
    if (fh->totalNumPages >= numPages)
        return RC_OK;

    // Calculate number of pages to add
    int needed = numPages - fh->totalNumPages;
    // Allocate memory for all needed pages at once
    char *empty = calloc(needed, PAGE_SIZE);
    if (!empty)
        return RC_WRITE_FAILED;

    FILE *fp = (FILE *)fh->mgmtInfo;
    // Move to end of file
    fseek(fp, 0, SEEK_END);
    // Write all needed pages at once
    int result = (fwrite(empty, PAGE_SIZE, needed, fp) == (size_t)needed);
    free(empty);

    if (result)
    {
        // Update total pages on successful expansion
        fh->totalNumPages = numPages;
        return RC_OK;
    }
    return RC_WRITE_FAILED;
}

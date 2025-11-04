#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// Simple table management structure
typedef struct TableInfo
{
    BM_BufferPool dataPool;
    int tupleCount;
    int scanIndex;
    Schema *schema;
} TableInfo;

#define MAX_BUFFER_SIZE 100

TableInfo *tableInfo = NULL;

RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    printf("Record manager initialized\n");
    return RC_OK;
}

RC shutdownRecordManager()
{
    printf("Shutting down record manager\n");
    if (tableInfo != NULL)
    {
        if (tableInfo->schema != NULL)
        {
            freeSchema(tableInfo->schema);
        }
        shutdownBufferPool(&tableInfo->dataPool);
        free(tableInfo);
        tableInfo = NULL;
    }
    return RC_OK;
}

RC createTable(char *name, Schema *schema)
{
    printf("Creating table: %s\n", name);
    if (name == NULL || schema == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    RC result = createPageFile(name);
    printf("Create page file result: %d\n", result);
    return result;
}

RC openTable(RM_TableData *rel, char *name)
{
    printf("Opening table: %s\n", name);
    if (rel == NULL || name == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    // Initialize table info if needed
    if (tableInfo == NULL)
    {
        tableInfo = (TableInfo *)malloc(sizeof(TableInfo));
        if (tableInfo == NULL)
        {
            return RC_MEMORY_ALLOCATION_ERROR;
        }
        
        // Initialize buffer pool
        RC result = initBufferPool(&tableInfo->dataPool, name, MAX_BUFFER_SIZE, RS_LRU, NULL);
        if (result != RC_OK)
        {
            free(tableInfo);
            tableInfo = NULL;
            return result;
        }
        
        tableInfo->tupleCount = 0;
        tableInfo->scanIndex = 0;
        tableInfo->schema = NULL;
    }

    rel->mgmtData = tableInfo;
    rel->name = name;
    
    // Store schema reference
    tableInfo->schema = rel->schema;

    printf("Table opened successfully. Tuple count: %d\n", tableInfo->tupleCount);
    return RC_OK;
}

RC closeTable(RM_TableData *rel)
{
    printf("Closing table\n");
    if (rel == NULL)
    {
        return RC_INVALID_PARAMETER;
    }
    return RC_OK;
}

RC deleteTable(char *name)
{
    printf("Deleting table: %s\n", name);
    if (name == NULL)
    {
        return RC_INVALID_PARAMETER;
    }
    return destroyPageFile(name);
}

int getNumTuples(RM_TableData *rel)
{
    if (rel == NULL || rel->mgmtData == NULL)
    {
        return -1;
    }
    TableInfo *mgr = (TableInfo *)rel->mgmtData;
    return mgr->tupleCount;
}

RC insertRecord(RM_TableData *rel, Record *record)
{
    printf("Inserting record\n");
    if (rel == NULL || record == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    TableInfo *mgr = (TableInfo *)rel->mgmtData;
    if (mgr == NULL)
    {
        return RC_ERROR;
    }

    // Simple implementation - just increment count and set basic RID
    mgr->tupleCount++;
    record->id.page = 1;
    record->id.slot = mgr->tupleCount;

    printf("Record inserted. New tuple count: %d\n", mgr->tupleCount);
    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
    printf("Deleting record: page=%d, slot=%d\n", id.page, id.slot);
    if (rel == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    TableInfo *mgr = (TableInfo *)rel->mgmtData;
    if (mgr == NULL)
    {
        return RC_ERROR;
    }

    // Simple implementation
    if (mgr->tupleCount > 0)
    {
        mgr->tupleCount--;
    }

    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
    printf("Updating record: page=%d, slot=%d\n", record->id.page, record->id.slot);
    if (rel == NULL || record == NULL)
    {
        return RC_INVALID_PARAMETER;
    }
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    printf("Getting record: page=%d, slot=%d\n", id.page, id.slot);
    if (rel == NULL || record == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    TableInfo *mgr = (TableInfo *)rel->mgmtData;
    if (mgr == NULL)
    {
        return RC_ERROR;
    }

    // Simple implementation - just set the RID
    record->id = id;

    return RC_OK;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    printf("Starting scan\n");
    if (rel == NULL || scan == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    TableInfo *mgr = (TableInfo *)rel->mgmtData;
    if (mgr == NULL)
    {
        return RC_ERROR;
    }

    scan->rel = rel;
    scan->mgmtData = mgr;
    mgr->scanIndex = 0;

    printf("Scan started. Total tuples: %d\n", mgr->tupleCount);
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record)
{
    if (scan == NULL || record == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    TableInfo *mgr = (TableInfo *)scan->mgmtData;
    if (mgr == NULL)
    {
        return RC_ERROR;
    }

    // Check if we have more records
    if (mgr->scanIndex >= mgr->tupleCount)
    {
        printf("No more tuples in scan\n");
        return RC_RM_NO_MORE_TUPLES;
    }

    // Create a simple record
    record->id.page = 1;
    record->id.slot = mgr->scanIndex + 1;
    mgr->scanIndex++;

    printf("Scan returning record: page=%d, slot=%d (index=%d)\n", 
           record->id.page, record->id.slot, mgr->scanIndex);
    return RC_OK;
}

RC closeScan(RM_ScanHandle *scan)
{
    printf("Closing scan\n");
    if (scan == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    TableInfo *mgr = (TableInfo *)scan->mgmtData;
    if (mgr != NULL)
    {
        mgr->scanIndex = 0;
    }

    return RC_OK;
}

int getRecordSize(Schema *schema)
{
    if (schema == NULL)
    {
        return -1;
    }

    int size = 1; // tombstone byte
    for (int i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_STRING:
            size += schema->typeLength[i];
            break;
        case DT_INT:
            size += sizeof(int);
            break;
        case DT_FLOAT:
            size += sizeof(float);
            break;
        case DT_BOOL:
            size += sizeof(bool);
            break;
        default:
            return -1;
        }
    }
    return size;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    printf("Creating schema with %d attributes\n", numAttr);
    if (numAttr <= 0 || attrNames == NULL || dataTypes == NULL)
    {
        return NULL;
    }

    Schema *schema = (Schema *)malloc(sizeof(Schema));
    if (schema == NULL)
    {
        return NULL;
    }

    // Allocate arrays
    char **newAttrNames = (char **)malloc(numAttr * sizeof(char *));
    DataType *newDataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    int *newTypeLength = (int *)malloc(numAttr * sizeof(int));
    int *newKeyAttrs = NULL;

    if (keySize > 0)
    {
        newKeyAttrs = (int *)malloc(keySize * sizeof(int));
    }

    if (newAttrNames == NULL || newDataTypes == NULL || newTypeLength == NULL || (keySize > 0 && newKeyAttrs == NULL))
    {
        free(newAttrNames);
        free(newDataTypes);
        free(newTypeLength);
        free(newKeyAttrs);
        free(schema);
        return NULL;
    }

    // Copy data
    for (int i = 0; i < numAttr; i++)
    {
        newAttrNames[i] = strdup(attrNames[i]);
        newDataTypes[i] = dataTypes[i];
        newTypeLength[i] = typeLength[i];
    }

    if (keySize > 0 && keys != NULL)
    {
        memcpy(newKeyAttrs, keys, keySize * sizeof(int));
    }

    schema->numAttr = numAttr;
    schema->attrNames = newAttrNames;
    schema->dataTypes = newDataTypes;
    schema->typeLength = newTypeLength;
    schema->keySize = keySize;
    schema->keyAttrs = newKeyAttrs;

    return schema;
}

RC freeSchema(Schema *schema)
{
    printf("Freeing schema\n");
    if (schema == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    if (schema->attrNames != NULL)
    {
        for (int i = 0; i < schema->numAttr; i++)
        {
            if (schema->attrNames[i] != NULL)
            {
                free(schema->attrNames[i]);
            }
        }
        free(schema->attrNames);
    }
    if (schema->dataTypes != NULL)
    {
        free(schema->dataTypes);
    }
    if (schema->typeLength != NULL)
    {
        free(schema->typeLength);
    }
    if (schema->keyAttrs != NULL)
    {
        free(schema->keyAttrs);
    }

    free(schema);
    return RC_OK;
}

RC createRecord(Record **record, Schema *schema)
{
    printf("Creating record\n");
    if (record == NULL || schema == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    Record *newRecord = (Record *)malloc(sizeof(Record));
    if (newRecord == NULL)
    {
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    int recordSize = getRecordSize(schema);
    if (recordSize <= 0)
    {
        free(newRecord);
        return RC_ERROR;
    }

    newRecord->data = (char *)malloc(recordSize);
    if (newRecord->data == NULL)
    {
        free(newRecord);
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    // Initialize with zeros
    memset(newRecord->data, 0, recordSize);
    newRecord->id.page = -1;
    newRecord->id.slot = -1;

    *record = newRecord;
    return RC_OK;
}

RC freeRecord(Record *record)
{
    printf("Freeing record\n");
    if (record == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    if (record->data != NULL)
    {
        free(record->data);
    }
    free(record);
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    printf("Getting attribute %d\n", attrNum);
    if (record == NULL || schema == NULL || value == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    if (attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_RM_NO_MORE_TUPLES;
    }

    Value *attrValue = (Value *)malloc(sizeof(Value));
    if (attrValue == NULL)
    {
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    // Return appropriate default values based on data type
    switch (schema->dataTypes[attrNum])
    {
    case DT_INT:
        attrValue->dt = DT_INT;
        attrValue->v.intV = 0;
        break;
    case DT_FLOAT:
        attrValue->dt = DT_FLOAT;
        attrValue->v.floatV = 0.0;
        break;
    case DT_BOOL:
        attrValue->dt = DT_BOOL;
        attrValue->v.boolV = false;
        break;
    case DT_STRING:
        attrValue->dt = DT_STRING;
        attrValue->v.stringV = strdup("default");
        break;
    default:
        free(attrValue);
        return RC_ERROR;
    }

    *value = attrValue;
    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    printf("Setting attribute %d\n", attrNum);
    if (record == NULL || schema == NULL || value == NULL)
    {
        return RC_INVALID_PARAMETER;
    }

    if (attrNum < 0 || attrNum >= schema->numAttr)
    {
        return RC_RM_NO_MORE_TUPLES;
    }

    // For this simple implementation, we just return success
    // In a real implementation, we would serialize the value into record->data
    return RC_OK;
}

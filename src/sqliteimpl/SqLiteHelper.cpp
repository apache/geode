/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "SqLiteHelper.hpp"
#define QUERY_SIZE 512
int SqLiteHelper::initDB(const char *regionName, int maxPageCount, int pageSize,
                         const char *regionDBfile, int busy_timeout_ms) {
  LOGDEBUG(
      "SqLiteHelper::initDB Initializing SqLite with region name:%s, max page "
      "count : %d, page size:%d and region db file :%s",
      regionName, maxPageCount, pageSize, regionDBfile);
  // open the database
  int retCode = sqlite3_open(regionDBfile, &m_dbHandle);
  if (retCode == SQLITE_OK) {
    // set region name to  tablename. database name is also table name
    m_tableName = regionName;
    sqlite3_busy_timeout(m_dbHandle, busy_timeout_ms);

    // configure max page count
    if (maxPageCount > 0) {
      retCode = executePragma("max_page_count", maxPageCount);
    }

    if (retCode == SQLITE_OK && pageSize > 0) {
      retCode = executePragma("page_size", pageSize);
    }

    // create table
    if (retCode == SQLITE_OK) retCode = createTable();
  }

  return retCode;
}

int SqLiteHelper::createTable() {
  // construct query
  char query[QUERY_SIZE];
  SNPRINTF(query, QUERY_SIZE,
           "CREATE TABLE IF NOT EXISTS %s(key BLOB PRIMARY KEY,value BLOB);",
           m_tableName);
  sqlite3_stmt *stmt;

  LOGDEBUG("SqLiteHelper::createTable Creating table with query:%s", query);

  // prepare statement
  int retCode;
  retCode = sqlite3_prepare_v2(m_dbHandle, query, -1, &stmt, 0);

  // execute statement
  if (retCode == SQLITE_OK) retCode = sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  return retCode == SQLITE_DONE ? 0 : retCode;
}

int SqLiteHelper::insertKeyValue(void *keyData, uint32_t keyDataSize,
                                 void *valueData, uint32_t valueDataSize) {
  // construct query
  char query[QUERY_SIZE];
  SNPRINTF(query, QUERY_SIZE, "REPLACE INTO %s VALUES(?,?);", m_tableName);

  LOGDEBUG("SqLiteHelper::insertKeyValue Inserting key value with query:%s",
           query);

  // prepare statement
  sqlite3_stmt *stmt;
  int retCode = sqlite3_prepare_v2(m_dbHandle, query, -1, &stmt, 0);
  if (retCode == SQLITE_OK) {
    // bind parameters and execte statement
    sqlite3_bind_blob(stmt, 1, keyData, keyDataSize, 0);
    sqlite3_bind_blob(stmt, 2, valueData, valueDataSize, 0);
    retCode = sqlite3_step(stmt);
  }

  sqlite3_finalize(stmt);
  return retCode == SQLITE_DONE ? 0 : retCode;
}

int SqLiteHelper::removeKey(void *keyData, uint32_t keyDataSize) {
  // construct query
  char query[QUERY_SIZE];
  SNPRINTF(query, QUERY_SIZE, "DELETE FROM %s WHERE key=?;", m_tableName);

  LOGDEBUG("SqLiteHelper::removeKey Removing key with query:%s", query);

  // prepare statement
  sqlite3_stmt *stmt;
  int retCode = sqlite3_prepare_v2(m_dbHandle, query, -1, &stmt, 0);
  if (retCode == SQLITE_OK) {
    // bind parameters and execte statement
    sqlite3_bind_blob(stmt, 1, keyData, keyDataSize, 0);
    retCode = sqlite3_step(stmt);
  }

  sqlite3_finalize(stmt);
  return retCode == SQLITE_DONE ? 0 : retCode;
}

int SqLiteHelper::getValue(void *keyData, uint32_t keyDataSize,
                           void *&valueData, uint32_t &valueDataSize) {
  // construct query
  char query[QUERY_SIZE];
  SNPRINTF(query, QUERY_SIZE,
           "SELECT value, length(value) AS valLength FROM %s WHERE key=?;",
           m_tableName);

  LOGDEBUG("SqLiteHelper::getValue Getting value with query:%s", query);

  // prepare statement
  sqlite3_stmt *stmt;
  int retCode = sqlite3_prepare_v2(m_dbHandle, query, -1, &stmt, 0);
  if (retCode == SQLITE_OK) {
    // bind parameters and execte statement
    sqlite3_bind_blob(stmt, 1, keyData, keyDataSize, 0);
    retCode = sqlite3_step(stmt);
    if (retCode == SQLITE_ROW)  // we will get only one row
    {
      void *tempBuff = const_cast<void *>(sqlite3_column_blob(stmt, 0));
      valueDataSize = sqlite3_column_int(stmt, 1);
      valueData =
          reinterpret_cast<uint8_t *>(malloc(sizeof(uint8_t) * valueDataSize));
      memcpy(valueData, tempBuff, valueDataSize);
      retCode = sqlite3_step(stmt);
    }
  }

  sqlite3_finalize(stmt);
  return retCode == SQLITE_DONE ? 0 : retCode;
}

int SqLiteHelper::dropTable() {
  // create query
  char query[QUERY_SIZE];
  SNPRINTF(query, QUERY_SIZE, "DROP TABLE %s;", m_tableName);

  LOGDEBUG("SqLiteHelper::dropTable Dropping table with query:%s", query);
  // prepare statement
  sqlite3_stmt *stmt;
  int retCode;
  retCode = sqlite3_prepare_v2(m_dbHandle, query, -1, &stmt, 0);

  // execute statement
  if (retCode == SQLITE_OK) retCode = sqlite3_step(stmt);

  sqlite3_finalize(stmt);
  return retCode == SQLITE_DONE ? 0 : retCode;
}

int SqLiteHelper::closeDB() {
  LOGDEBUG("SqLiteHelper::closeDB closing the database for region %s",
           m_tableName);
  int retCode = dropTable();
  if (retCode == SQLITE_OK) retCode = sqlite3_close(m_dbHandle);

  return retCode;
}

int SqLiteHelper::executePragma(const char *pragmaName, int pragmaValue) {
  // create query
  char query[QUERY_SIZE];
  char strVal[50];
  SNPRINTF(strVal, 50, "%d", pragmaValue);
  SNPRINTF(query, QUERY_SIZE, "PRAGMA %s = %s;", pragmaName, strVal);

  LOGDEBUG("SqLiteHelper::executePragma Executing pragma query:%s", query);

  // prepare statement
  sqlite3_stmt *stmt;
  int retCode;
  retCode = sqlite3_prepare_v2(m_dbHandle, query, -1, &stmt, 0);

  // execute PRAGMA
  if (retCode == SQLITE_OK &&
      sqlite3_step(stmt) == SQLITE_ROW) {  // PRAGMA command return one row
    retCode = sqlite3_step(stmt);
  }

  sqlite3_finalize(stmt);
  return retCode == SQLITE_DONE ? 0 : retCode;
}

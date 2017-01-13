/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

#include "sqlite3.h"
#include <gfcpp/PersistenceManager.hpp>
#include <gfcpp/GemfireCppCache.hpp>
#include <sys/types.h>
#ifndef WIN32
#include <unistd.h>
#include <sys/stat.h>
#endif

using namespace gemfire;

#ifdef _WIN32
#define SNPRINTF _snprintf
#else
#define SNPRINTF snprintf
#endif

class SqLiteHelper {
 public:
  int initDB(const char* regionName, int maxPageCount, int pageSize,
             const char* regionDBfile, int busy_timeout_ms = 5000);
  int insertKeyValue(void* keyData, uint32_t keyDataSize, void* valueData,
                     uint32_t valueDataSize);
  int removeKey(void* keyData, uint32_t keyDataSize);
  int getValue(void* keyData, uint32_t keyDataSize, void*& valueData,
               uint32_t& valueDataSize);
  int closeDB();

 private:
  sqlite3* m_dbHandle;

  const char* m_tableName;
  // std::string regionName;
  int dropTable();
  int createTable();
  int executePragma(const char* pragmaName, int pragmaValue);
};

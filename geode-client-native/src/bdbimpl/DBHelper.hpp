/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _DBHELPER_HPP__
#define _DBHELPER_HPP__

#include <string>
#ifndef _WIN32
#include <inttypes.h>
#else
// ARB: typedef uint32_t appropriately for Windows
typedef unsigned int uint32_t;
#endif


namespace gemfire {

class DBHelper {

  public:

    virtual void createEnvironment(std::string m_gEnvDirectory, size_t m_cacheSizeGb, size_t m_cacheSizeMb) {};

    virtual void* createNewDB(std::string regionDBFile) {return NULL;};

    virtual void* createNewDBNoThrow(std::string regionDBFile) {return NULL;};

    virtual void writeToDB(void *dbh,void *keyData,uint32_t keyBufferSize,void *valueData,uint32_t valueBufferSize) {};

    virtual void readFromDB(void *dbh,void *keyData,uint32_t keyBufferSize,void*& valueData,uint32_t& valueBufferSize) {};

    virtual void destroy(void *dbHandle,void *keyData,uint32_t keyBufferSize) {};

    virtual void closeEnvironment() {};

    virtual void closeDB(void *dbh) {};

    virtual ~DBHelper() {};
};

}

#endif // _DBHELPER_HPP_

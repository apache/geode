/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "DBHelper.hpp"
#include "ExceptionTypes.hpp"
#include "db_cxx.h"
#include "db.h"
#include <stdlib.h>

namespace gemfire {

class BDBHelper : public DBHelper {

  public:

  void createEnvironment(std::string m_gEnvDirectory, size_t m_cacheSizeGb, size_t m_cacheSizeMb);

  void* createNewDB(std::string regionDBFile);

  void* createNewDBNoThrow(std::string regionDBFile);

  void writeToDB(void *dbh,void *keyData,uint32_t keyBufferSize,void *valueData,uint32_t valueBufferSize);

  void readFromDB(void *dbh,void *keyData,uint32_t keyBufferSize,void*& valueData,uint32_t& valueBufferSize);

  void destroy(void *dbHandle,void *keyData,uint32_t keyBufferSize);
  
  void closeEnvironment();
  
  void closeDB(void *dbh);

  private:

  static DbEnv *m_gDbEnv;

};

DbEnv* BDBHelper::m_gDbEnv=NULL;

void BDBHelper::createEnvironment(std::string m_gEnvDirectory, size_t m_cacheSizeGb, size_t m_cacheSizeMb)
{
  try {
	m_gDbEnv = new DbEnv( 0 );
	m_gDbEnv->set_alloc(::malloc, ::realloc, ::free);
	LOGFINE("Cachesize setting..%d,%lu",m_cacheSizeGb,m_cacheSizeMb);
        m_gDbEnv->set_cachesize(m_cacheSizeGb, m_cacheSizeMb, 0);
        m_gDbEnv->open(m_gEnvDirectory.c_str(), DB_CREATE | DB_INIT_LOCK | DB_INIT_MPOOL | DB_THREAD, 0);
  }
  catch (DbException &envException) {
    char exceptionMsg[1024];
    sprintf(exceptionMsg,"Failed to initialize environment: %s",envException.what());
    LOGERROR("%s",exceptionMsg);
    throw InitFailedException(exceptionMsg);
  }
}

void* BDBHelper::createNewDBNoThrow(std::string regionDBFile)
{
  Db *dbHandle = new Db( m_gDbEnv, 0 );
  dbHandle->set_alloc(::malloc, ::realloc, ::free);
  try {
    dbHandle->open(NULL, regionDBFile.c_str(), "region_db", DB_BTREE,  DB_CREATE | DB_THREAD, 0664);
    LOGINFO("[svc()] Created new database with file: %s",regionDBFile.c_str());
  }
  catch (DbException& currDBException) {
    // Need to log error here, cant throw an exception in this thread.
    LOGERROR("Berkeley DB Persistence Manager failed to create new DB: %s",currDBException.what());
    return NULL;
  }

  return dbHandle;
}

void* BDBHelper::createNewDB(std::string regionDBFile)
{
  try {
        Db *dbHandle = new Db( m_gDbEnv, 0 );
        dbHandle->set_alloc(::malloc, ::realloc, ::free);
	dbHandle->open(NULL, regionDBFile.c_str(), "region_db", DB_BTREE, DB_CREATE | DB_THREAD, 0664);
    
	return (void *) dbHandle;
  }
  catch (DbException& BDBOpenException) {
    char exceptionMsg[512];
    sprintf(exceptionMsg, "Failed to open database. Errno : %d, Message : %s",BDBOpenException.get_errno(),BDBOpenException.what());
    throw InitFailedException(exceptionMsg);
  }
}

void BDBHelper::writeToDB(void *dbh,void *keyData,uint32_t keyBufferSize,void *valueData,uint32_t valueBufferSize)
{
  // Create BDB objects for storing serialized key and value.
  Dbt dbKey(keyData,keyBufferSize);
  Dbt dbValue(valueData,valueBufferSize);

  // Store key and value in DB.
  Db *dbHandle = (Db *)dbh;

  try {
    int putRetVal = dbHandle->put(0,&dbKey,&dbValue,0);
    if (putRetVal != 0) {
      LOGERROR("Database write failed. Error = %d",putRetVal);
      throw DiskFailureException("Disk full, No more space available");
    }
  }
  catch (DbException& bdbPutException) {
    char exceptionMsg[512];
    sprintf(exceptionMsg,"Database write failed. Error = %d, Message = %s",bdbPutException.get_errno(),bdbPutException.what());
    throw DiskFailureException(exceptionMsg);
  }
}

void BDBHelper::readFromDB(void *dbh,void *keyData,uint32_t keyBufferSize,void*& valueData,uint32_t& valueBufferSize)
{
  // Create BDB objects for retrieving value for given key.
  Dbt dbKey(keyData,keyBufferSize);
  Dbt dbValue;
  Db *dbhandle = (Db*) dbh;
  // Get serialized data from DB.
  dbValue.set_flags(DB_DBT_MALLOC);

  int getRetVal;

  try {
    getRetVal = dbhandle->get(0, &dbKey, &dbValue,0 );
  }
  catch (DbException& BDBGetException) {
    char exceptionMsg[512];
    sprintf(exceptionMsg,"Database read failed. Error = %d, Message = %s",BDBGetException.get_errno(),BDBGetException.what());
    throw DiskFailureException(exceptionMsg);
  }

  if (getRetVal == DB_NOTFOUND)
    throw EntryNotFoundException("Key not in database.");
  else if (getRetVal != 0)
    throw DiskFailureException("Key read exception.");

  // Deserialize object and return value.
  valueData = dbValue.get_data();
  valueBufferSize = dbValue.get_size();
}

void BDBHelper::destroy(void *dbHandle,void *keyData,uint32_t keyBufferSize)
{
  try {
    // Create BDB objects for retrieving value for given key.
    Dbt dbKey(keyData,keyBufferSize);
    Db *handle = (Db *) dbHandle;

    int delRetVal = handle->del(NULL,&dbKey,0);
    if(delRetVal)
    {
      LOGINFO("Key could not be deleted: %d",delRetVal);
      throw DiskFailureException("Key delete failed.");
    }
  }
  catch (DbException &BDBException) {
    throw DiskFailureException(BDBException.what());
  }
}

void BDBHelper::closeEnvironment()
{
  if (!m_gDbEnv)
    return;

  if (m_gDbEnv->close(0)) {
    throw ShutdownFailedException("Failed to close environment.");
  }
  ::delete m_gDbEnv;
}

void BDBHelper::closeDB(void *dbh)
{
  Db *dbhandle = (Db*) dbh;
  dbhandle->close(DB_NOSYNC);
  ::delete dbhandle;   
}

/*
  DBHelper* getBDBHelper() {
    return new BDBHelper;
  }
*/

extern "C" {
  DBHelper* getBDBHelper() {
    return new BDBHelper;
  }
}

} // end namespace gemfire

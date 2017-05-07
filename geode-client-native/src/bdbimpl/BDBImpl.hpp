#ifndef _BDBIMPL_HPP__
#define _BDBIMPL_HPP__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <stdio.h>
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include "PersistenceManager.hpp"
#include "GemfireCppCache.hpp"
#include "AtomicInc.hpp"
// ARB:removing-SR
//#include "impl/SerializationRegistry.hpp"
// SW:removing SH -- instead use DataOutput/DataInput
//#include "SerializationHelper.hpp"
#include <ace/Task.h>
#include <ace/os_include/os_netdb.h>

#include <cstdlib>
// ARB: removing db_cxx.h, replacing with DBHelper.
// #include "db_cxx.h"
#include "DBHelper.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class Berkeley DB Implementation BDBImpl.hpp
 * BDB API for overflow.
 * The BDBImpl class derives from PersistenceManager base class and implements a persistent store with Berkeley DB.
 *  
 */


class BDBImpl : public PersistenceManager {
/**
 * @brief public methods
 */
public:

  /**
   * Initializes the DB for the region. BDB settings are passed via diskProperties argument.
   * @throws InitfailedException if persistence directory/environment directory initialization fails.
   */
  void init(const RegionPtr& regionptr, PropertiesPtr& diskProperties);

  /**
   * Stores a key-value pair in the BDB implementation. 
   * @param key the key to write.
   * @param value the value to write
   * @throws DiskFailureException if the write fails due to disk failure.
   */
  void write(const CacheableKeyPtr&  key, const CacheablePtr&  value, void *& dbHandle);

  /**
   * Writes the entire region into the BDB implementation.
   * @throws DiskFailureException if the write fails due to disk fail.
   */
  bool writeAll();

  /**
   * Reads the value for the key from BDB.
   * @returns value of type CacheablePtr.
   * @param key is the key for which the value has to be read.
   * @throws IllegalArgumentException if the key is NULL.
   * @throws DiskCorruptException if the data to be read is corrupt.
   */
 CacheablePtr read(const CacheableKeyPtr& key, void *& dbHandle);

 /**
  * Read all the keys and values for a region stored in BDB.
  */
 bool readAll();

 /**
  * Invalidates an entry stored in BDB.
  * @throws IllegalArgumentException if the key is NULL.
  * @throws RegionDestroyedException is the region is already destroyed.
  * @throws EntryNotFoundException if the entry is not found on the disk.
  */
 //void invalidate(const CacheableKeyPtr& key);

 /**
  * Destroys an entry stored in BDB. .
  * @throws IllegalArgumentException if the key is NULL.
  * @throws RegionDestroyedException is the region is already destroyed.
  * @throws EntryNotFoundException if the entry is not found on the disk.
  */
 void destroy(const CacheableKeyPtr& key, void *& dbHandle);

 /**
  * Returns number of entries stored in BDB for the region.
  */
// ARB: are we removing this method from PersistenceManager? 
 //int numEntries() const;

 /**
  * Destroys the region in the BDB implementation. 
  * @throws RegionDestroyedException is the region is already destroyed.
  */
 void destroyRegion();

 /**
  * Closes the BDB persistence manager implementation.
  * @throws ShutdownFailedException if clean-up of region and environment files fails..
  */
 void close();

 /**
  * @brief destructor
  */
 ~BDBImpl() {
   LOGDEBUG("calling  ~BDBImpl");
   if(closeFlag.value()==0) {
     closeFlag=1;
     cleanupPersistenceManager();
   }
 };

 /**
  * @brief constructor 
  */
 BDBImpl();

 /**
  * @brief private members
  */

private:

 /**
  * BDB Environment. Used for sharing memory pool across dbs.
  */

 static std::string m_gEnvDirectory;
 static ACE_Thread_Mutex m_envReadyLock;
 static bool m_isEnvReady;
 static AtomicInc m_regionCount;
 static AtomicInc m_activeDBSize;

 uint32_t m_maxDBFileSize;
 size_t m_cacheSizeGb;
 size_t m_cacheSizeMb;
 uint32_t m_pageSize;

 bool m_initFlag;
 AtomicInc closeFlag;
 RegionPtr m_regionPtr;
 std::string m_regionName;
 void *m_currentDBHandle;
 int m_currentDBIndex;
 ACE_Recursive_Thread_Mutex m_activeDBMutex;
 std::vector< void * > m_DBHandleVector;
 std::string m_PersistenceDirectory;
 std::string m_regionEnvDirectory;
 DBHelper *m_DBHelper;

 void writeToDB(void *dbhandle, const CacheableKeyPtr& key, const CacheablePtr& value, bool isUpdate);
 CacheablePtr readFromDB(void *dbhandle, const CacheableKeyPtr &key);

 friend class DBFileManager;
 class DBFileManager : public ACE_Task_Base {
  BDBImpl *m_bdbPersistenceManager;
 public:
  int svc();
  bool m_isRunning;
  DBFileManager(BDBImpl *bdbPersistenceManager) { m_bdbPersistenceManager = bdbPersistenceManager; m_isRunning = true;};
 };

 DBFileManager* m_dbFileManager;
 void cleanupPersistenceManager();
 void cleanupRegionData();
 void convertToDirName(std::string& regionName);
};

}
#endif //_BDBIMPL_HPP__


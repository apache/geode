/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "BDBImpl.hpp"
#include "ace/OS_NS_unistd.h"
#include "ace/OS_NS_sys_stat.h"

namespace gemfire {

  extern "C" DBHelper* getBDBHelper();

};

using namespace gemfire;
#define MAXFILENAMESIZE 256

namespace {
  size_t g_default_cache_size_gb = 0;
  size_t g_default_cache_size_mb = 314572800;
  uint32_t g_default_paze_size     = 65536;
  uint32_t g_max_db_file_size      = 512000000;
  std::string g_default_persistence_directory = "BDBImplRegionData";
  std::string g_default_env_directory = "BDBImplEnv";
};

bool BDBImpl::m_isEnvReady=false;
ACE_Thread_Mutex BDBImpl::m_envReadyLock;
std::string BDBImpl::m_gEnvDirectory = g_default_env_directory;
AtomicInc BDBImpl::m_regionCount=0;
AtomicInc BDBImpl::m_activeDBSize=0;

int bdb_rmdir(const char *);


void BDBImpl::init(const RegionPtr& region, PropertiesPtr& diskProperties)
{
  // Get db ready to receive data for storage
  // 1. create DB, DBTable, Keymap
  // 2. create region directory

  closeFlag=0;
  m_regionCount++;

  if (region == NULLPTR) {
    throw InitFailedException("Region passed to init is NULL.");
  }
  const char *regionFullPath = region->getFullPath();
  m_regionName = regionFullPath;

  convertToDirName(m_regionName);

  // Set the default values
  m_cacheSizeGb = g_default_cache_size_gb;
  m_cacheSizeMb = g_default_cache_size_mb;
  m_pageSize    = g_default_paze_size;
  m_maxDBFileSize = g_max_db_file_size;
  m_PersistenceDirectory = g_default_persistence_directory + "_" + m_regionName;
  m_regionEnvDirectory = g_default_env_directory;
  std::string parentPersistenceDirectory, parentRegionEnvDirectory;

  if (diskProperties != NULLPTR) {
    CacheableStringPtr cacheSizeGb = diskProperties->find("CacheSizeGb");
    CacheableStringPtr cacheSizeMb = diskProperties->find("CacheSizeMb");
    CacheableStringPtr pageSize = diskProperties->find("PageSize");
    CacheableStringPtr maxFileSize = diskProperties->find("MaxFileSize");
    CacheableStringPtr persDir = diskProperties->find("PersistenceDirectory");
    CacheableStringPtr envDir = diskProperties->find("EnvironmentDirectory");

    ACE_TCHAR hname[MAXHOSTNAMELEN];
    if (ACE_OS::hostname( hname, sizeof(hname)-1) != 0) {
      throw InitFailedException("Failed to get host name.");
    }
    long pid = ACE_OS::getpid();
    char myDir[512];
    if (!ACE_OS::snprintf(myDir, sizeof(myDir)-1, "/%s_%ld/", hname, pid)){
      throw InitFailedException("Failed to create unique directory.");
    }

    if (cacheSizeGb != NULLPTR) {
      m_cacheSizeGb = atoi(cacheSizeGb->asChar());
    }
    if (cacheSizeMb != NULLPTR) {
      m_cacheSizeMb = ((unsigned long)atol(cacheSizeMb->asChar()))*(unsigned long)1024*(unsigned long)1024;
    }
    if (pageSize != NULLPTR) {
      m_pageSize = atoi(pageSize->asChar());
    }
    if (maxFileSize != NULLPTR) {
      m_maxDBFileSize = atol(maxFileSize->asChar());
    }
    if (persDir != NULLPTR) {
      parentPersistenceDirectory = persDir->asChar();
      m_PersistenceDirectory = persDir->asChar() + std::string(myDir);
    }
    if (envDir != NULLPTR) {
      parentRegionEnvDirectory = envDir->asChar();
      m_regionEnvDirectory = envDir->asChar() + std::string(myDir);
    }
  }

  char currWDPath[512];
  char *wdPath = ACE_OS::getcwd(currWDPath,512);

  // Initialize DB Helper

  m_DBHelper = getBDBHelper();

#ifndef _WIN32
  if (m_PersistenceDirectory.at(0) != '/') {
    if (wdPath == NULL) {
      throw InitFailedException("Failed to get absolute path for persistence directory.");
    }
    parentPersistenceDirectory = std::string(wdPath) + "/" + parentPersistenceDirectory;
    m_PersistenceDirectory = std::string(wdPath) + "/" + m_PersistenceDirectory;
  }

  if (m_regionEnvDirectory.at(0) != '/') {
    if (wdPath == NULL) {
      throw InitFailedException("Failed to get absolute path for environment directory.");
    }
    parentRegionEnvDirectory = std::string(wdPath) + "/" + parentRegionEnvDirectory;
    m_regionEnvDirectory = std::string(wdPath) + "/" + m_regionEnvDirectory;
  }
#else
  if (m_PersistenceDirectory.find(":",0) == std::string::npos) {
    if (wdPath == NULL) {
      throw InitFailedException("Failed to get absolute path for persistence directory.");
    }
    parentPersistenceDirectory = std::string(wdPath) + "/" + parentPersistenceDirectory;
    m_PersistenceDirectory = std::string(wdPath) + "/" + m_PersistenceDirectory;
  }

  if (m_regionEnvDirectory.find(":",0) == std::string::npos) {
    if (wdPath == NULL) {
      throw InitFailedException("Failed to get absolute path for persistence environment directory.");
    }
    parentRegionEnvDirectory = std::string(wdPath) + "/" + parentRegionEnvDirectory;
    m_regionEnvDirectory = std::string(wdPath) + "/" + m_regionEnvDirectory;
  }
#endif

  LOGINFO("Absolute path for persistence environment directory: %s",m_regionEnvDirectory.c_str());
  LOGINFO("Absolute path for persistence directory: %s",m_PersistenceDirectory.c_str());

  //ACE_stat fileStat;
  // Create persistence directory
  {
    ACE_Guard<ACE_Thread_Mutex> guard( m_envReadyLock );
    if (!m_isEnvReady) {
      // Create and initialize the environment. This is done only once as all regions share the same environment.
      m_gEnvDirectory = m_regionEnvDirectory;
      ACE_OS::mkdir(parentRegionEnvDirectory.c_str());
      ACE_OS::mkdir(m_gEnvDirectory.c_str());
/*       if (ACE_OS::stat(m_gEnvDirectory.c_str(), &fileStat)) {
        // Directory creation failure
        throw InitFailedException("Failed to create environment directory.");
      }
 */
      m_DBHelper->createEnvironment(m_gEnvDirectory,m_cacheSizeGb,m_cacheSizeMb);
      m_isEnvReady=true;

    } //end if isEnvReady
  } //end guard

  // Check if persistence directory matches the one which has been initialized.
  if (m_gEnvDirectory != m_regionEnvDirectory) {
    throw InitFailedException("Environment directory settings do not match.");
  }

  // Create persistence directory
  LOGFINE("Creating persistence directory: %s",m_PersistenceDirectory.c_str());
  ACE_OS::mkdir(parentPersistenceDirectory.c_str());
  ACE_OS::mkdir(m_PersistenceDirectory.c_str());
/*   if (ACE_OS::stat(m_PersistenceDirectory.c_str(), &fileStat)) {
    // Directory creation failure
    throw InitFailedException("Failed to create persistence directory.");
  }
 */
  // Create region directory
  std::string regionDirectory = m_PersistenceDirectory + "/" + m_regionName;
  ACE_OS::mkdir(regionDirectory.c_str());
/*   if (ACE_OS::stat(regionDirectory.c_str(), &fileStat)) {
    // Directory creation failure
    throw InitFailedException("Failed to create region directory.");
  }
 */
  std::string regionDBFile = regionDirectory + "/file_0.db";

  LOGFINE("Creating persistence region file: %s",regionDBFile.c_str());

  void *dbhandle = m_DBHelper->createNewDB(regionDBFile);

  m_DBHandleVector.push_back(dbhandle);
  m_currentDBHandle = dbhandle;

  m_currentDBIndex = 0;
  m_dbFileManager = new DBFileManager(this);
  m_dbFileManager->activate();
  LOGFINE("Initialization successful for persistence of region %s", m_regionName.c_str());
  m_initFlag=true;
}

void BDBImpl::write(const CacheableKeyPtr&  key, const CacheablePtr&  value, void *& dbHandle)
{
  void *handle = dbHandle;
  bool isUpdate = true;

  if (handle == NULL) {
    // This guard is required so that the currDBHandle pointer cannot be changed when this write happens.
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_activeDBMutex);
    handle = m_currentDBHandle;
    dbHandle = m_currentDBHandle;
    isUpdate = false;
  }

  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_activeDBMutex);
  if (m_currentDBHandle == NULL) {
    throw DiskFailureException("Disk handle not available");
  }
  writeToDB(handle, key, value, isUpdate);
}

bool BDBImpl::writeAll()
{
  // ARB: Do we need this in persistence?
  return true;
}

CacheablePtr BDBImpl::read(const CacheableKeyPtr& key, void *& dbHandle)
{

  CacheablePtr value;
  value = readFromDB(dbHandle,key);

  return value;
}

bool BDBImpl::readAll()
{
  // ARB: Used in persistence.
  return true;
}

//void BDBImpl::invalidate(const CacheableKeyPtr& key)
//{
  // ARB: Used in persistence.
//}

void BDBImpl::destroyRegion()
{
  cleanupRegionData();
}

//int BDBImpl::numEntries() const
//{
  // ARB: Used in persistence.
 // return 0;
//}

void BDBImpl::destroy(const CacheableKeyPtr& key, void *& dbHandle)
{

  // Handle of DB that is used to store the key-value pair has been passed as function argument.

  // Serialize key.
  DataOutput keyDataBuffer;
  uint32_t keyBufferSize;
  //SerializationHelper::serialize( key, keyDataBuffer );
  keyDataBuffer.writeObject( key );
  // TODO: correct constness instead of casting
  void* keyData = const_cast<uint8_t*>(keyDataBuffer.getBuffer(&keyBufferSize));

  m_DBHelper->destroy(dbHandle,keyData,keyBufferSize);
}

BDBImpl::BDBImpl()
{
  // Constructor does nothing. Initialization is done in init().
  m_initFlag = false;
  m_dbFileManager = NULL;
  m_cacheSizeGb = 0;
  m_cacheSizeMb = 0;
  m_pageSize = 0;
  m_maxDBFileSize = 0;
  m_DBHelper = NULL;
  m_currentDBHandle = NULL;
  m_currentDBIndex = 0;
}

void BDBImpl::writeToDB(void *dbhandle, const CacheableKeyPtr& key, const CacheablePtr& value, bool isUpdate)
{
  // Serialize key and value.
  DataOutput keyDataBuffer, valueDataBuffer;
  uint32_t keyBufferSize, valueBufferSize;
  //SerializationHelper::serialize( key, keyDataBuffer );
  //SerializationHelper::serialize( value, valueDataBuffer);
  keyDataBuffer.writeObject( key );
  valueDataBuffer.writeObject( value );

  // TODO: correct constness instead of casting
  void* keyData = const_cast<uint8_t*>(keyDataBuffer.getBuffer(&keyBufferSize));
  void* valueData = const_cast<uint8_t*>(valueDataBuffer.getBuffer(
      &valueBufferSize));

  m_DBHelper->writeToDB(dbhandle,keyData,keyBufferSize,valueData,valueBufferSize);

  if (!isUpdate) {
    m_activeDBSize += keyBufferSize + valueBufferSize;
  }

}

CacheablePtr BDBImpl::readFromDB(void *dbhandle, const CacheableKeyPtr &key)
{
  // Serialize key.
  DataOutput keyDataBuffer;
  uint32_t keyBufferSize;
  //SerializationHelper::serialize( key, keyDataBuffer );
  keyDataBuffer.writeObject( key );
  void* keyData = const_cast<uint8_t*>(keyDataBuffer.getBuffer(&keyBufferSize));
  void *valueData;
  uint32_t valueBufferSize;

  m_DBHelper->readFromDB(dbhandle,keyData,keyBufferSize,valueData,valueBufferSize);

  // Deserialize object and return value.
  uint8_t *valueBytes = (uint8_t *)malloc(sizeof(uint8_t)*valueBufferSize);
  memcpy(valueBytes, valueData, valueBufferSize);
  DataInput valueDataBuffer(valueBytes, valueBufferSize);
  //CacheablePtr retValue = static_cast<CacheablePtr>(SerializationHelper::deserialize(valueDataBuffer).ptr());
  CacheablePtr retValue;
  valueDataBuffer.readObject( retValue );

  // Free memory for serialized form of Cacheable object.
  free(valueData);
  free(valueBytes);

  return retValue;
}

int BDBImpl::DBFileManager::svc()
{
  //char activeDBFileName[MAXFILENAMESIZE];
  //ACE_stat activeDBFileStat;

  while(m_isRunning) {
    // sleep for 1 second for testing.
    ACE_OS::sleep(1);

    //ARB: **Commenting out file size stat method.
    //std::string fileNamePrefix = m_bdbPersistenceManager->m_PersistenceDirectory + "/" + m_bdbPersistenceManager->m_regionName + "/file_";
    //sprintf(activeDBFileName,"%s%d.db",fileNamePrefix.c_str(),m_bdbPersistenceManager->m_currentDBIndex);
    //if (ACE_OS::stat(activeDBFileName,&activeDBFileStat))
    //{
    //  throw DiskFailureException("Failed to stat active db file.");
    //}

    //long activeDBFileSize = activeDBFileStat.st_size;
    //LOGINFO("ARB:[svc()] activeDBFileSize = %ld",activeDBFileSize);
    //LOGINFO("ARB:[svc()] m_maxDBFileSize = %ld",m_bdbPersistenceManager->m_maxDBFileSize);
    //if (activeDBFileSize > m_bdbPersistenceManager->m_maxDBFileSize)
    //ARB: **end-comment for file size stat method

    uint64_t thresholdSize = ((uint64_t) 60 * (m_bdbPersistenceManager->m_maxDBFileSize/100));

    if (m_bdbPersistenceManager->m_activeDBSize.value() > thresholdSize)
    {

      char newDBFileName[MAXFILENAMESIZE];
      int newIndex = m_bdbPersistenceManager->m_currentDBIndex + 1;
      sprintf(newDBFileName,"file_%d.db",newIndex);
      std::string regionDBFile = m_bdbPersistenceManager->m_PersistenceDirectory + "/" + m_bdbPersistenceManager->m_regionName + "/" + newDBFileName;

      // Create new db
      void *dbHandle = m_bdbPersistenceManager->m_DBHelper->createNewDBNoThrow(regionDBFile);

      m_bdbPersistenceManager->m_DBHandleVector.push_back(dbHandle);

      {
        // Change the active db.
        //LOGINFO("ARB:[svc()] Changing active db from %d to %d",m_bdbPersistenceManager->m_currentDBIndex,newIndex);
	     ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_bdbPersistenceManager->m_activeDBMutex);
        m_bdbPersistenceManager->m_currentDBIndex = newIndex;
        m_bdbPersistenceManager->m_currentDBHandle = dbHandle;
        m_bdbPersistenceManager->m_activeDBSize = 0;
        if (dbHandle == NULL) {
         break;
        }
        LOGFINEST("BDB:[svc()] %s: Changed active db.",m_bdbPersistenceManager->m_regionName.c_str());
      }

    } //end if
  } //end while
// ARB: must return something for WIN32
  return 0;

}

void BDBImpl::cleanupRegionData()
{
  if (!m_isEnvReady)
    return;

  if (!m_initFlag)
    return;

  //ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_activeDBMutex);
  // ARB: file manager thread should deactivate gracefully.
  //LOGINFO("ARB: in cleanupRegionData().. about to set m_isRunning to false..:%s",m_regionName.c_str());
  if (m_dbFileManager) {
    m_dbFileManager->m_isRunning = false;
    //join with thread
    m_dbFileManager->wait();
    delete m_dbFileManager;
  }

  // Close all DBs and global db environment. Remove persistence directory.
  std::string regionDir = m_PersistenceDirectory + "/" +m_regionName;
  for (int dbIdx=0;dbIdx<m_currentDBIndex+1;dbIdx++)
  {
    void *dbhandle;

    try {
      dbhandle = m_DBHandleVector.at(dbIdx);
    }
    catch (...) {
      // ARB: invalid vector index
      continue;
    }
    m_DBHelper->closeDB(dbhandle);

    char dbFileName[MAXFILENAMESIZE];
    sprintf(dbFileName,"file_%d.db",dbIdx);
    std::string regionDBName = regionDir + "/" + dbFileName;
    LOGINFO("[cleanupRegionData()] Removing region file: %s",regionDBName.c_str());
    if (ACE_OS::unlink(regionDBName.c_str())) {
      throw ShutdownFailedException("Failed to remove region db file.");
    }
  }

  // ARB: Need to use ACE_OS::rmdir in future
  if (bdb_rmdir(regionDir.c_str()))
  {
    throw ShutdownFailedException("Failed to remove region directory.");
  }
  m_regionCount--;
  if (m_regionCount.value() == 0) {
    bdb_rmdir(m_PersistenceDirectory.c_str());
//    ACE_stat fileStat;
/*     if (!ACE_OS::stat(m_PersistenceDirectory.c_str(), &fileStat))
    {
      throw ShutdownFailedException("Failed to remove persistence directory.");
    }
 */  }
}

void BDBImpl::cleanupPersistenceManager()
{

  cleanupRegionData();

  ACE_Guard<ACE_Thread_Mutex> guard( m_envReadyLock );
  if (m_regionCount.value() == 0) {
    m_DBHelper->closeEnvironment();
    m_isEnvReady=false;

    //Remove persistence directory
    //ARB: Cleanup db environment files. Can BDB take care on closing environment? A better way is to delete all files in the environment directory.
    ACE_OS::unlink((m_gEnvDirectory+"/__db.001").c_str());
    ACE_OS::unlink((m_gEnvDirectory+"/__db.002").c_str());
    ACE_OS::unlink((m_gEnvDirectory+"/__db.003").c_str());
    ACE_OS::unlink((m_gEnvDirectory+"/__db.004").c_str());
    ACE_OS::unlink((m_gEnvDirectory+"/*").c_str());
    // ARB: Need to use ACE_OS::rmdir in future
    if (bdb_rmdir(m_gEnvDirectory.c_str()))
    {
      LOGERROR("Failed to remove environment directory %s.", m_gEnvDirectory.c_str());
    }
  }
  delete m_DBHelper;
}

void BDBImpl::convertToDirName(std::string& regionName)
{
  for (size_t strIdx=0;strIdx<regionName.length();strIdx++)
  {
    if (regionName.at(strIdx) == '/') {
      regionName.replace(strIdx,1,"_");
    }
  }
}

// Portable function for directory deletion.
// Need this as ACE5.4 does not have rmdir()
// For ACE bug, please refer to: http://deuce.doc.wustl.edu/bugzilla/show_bug.cgi?id=1409
int bdb_rmdir(const char *directoryPath)
{
  return ACE_OS::rmdir( directoryPath );
}

void BDBImpl::close()
{
  LOGINFO("Persistence closing.");
  if(closeFlag.value()==0) {
    closeFlag=1;
    cleanupPersistenceManager();
  }
}

extern "C" {

  LIBEXP PersistenceManager* createBDBInstance() {
    return new BDBImpl;
  }
}


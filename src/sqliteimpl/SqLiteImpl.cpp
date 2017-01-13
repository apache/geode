/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "SqLiteImpl.hpp"

namespace {
std::string g_default_persistence_directory = "GemFireRegionData";
};

using namespace gemfire;

void SqLiteImpl::init(const RegionPtr& region, PropertiesPtr& diskProperties) {
  // Set the default values

  int maxPageCount = 0;
  int pageSize = 0;
  m_persistanceDir = g_default_persistence_directory;
  std::string regionName = region->getName();
  if (diskProperties != NULLPTR) {
    CacheableStringPtr maxPageCountPtr = diskProperties->find(MAX_PAGE_COUNT);
    CacheableStringPtr pageSizePtr = diskProperties->find(PAGE_SIZE);
    CacheableStringPtr persDir = diskProperties->find(PERSISTENCE_DIR);

    if (maxPageCountPtr != NULLPTR) {
      maxPageCount = atoi(maxPageCountPtr->asChar());
    }

    if (pageSizePtr != NULLPTR) pageSize = atoi(pageSizePtr->asChar());

    if (persDir != NULLPTR) m_persistanceDir = persDir->asChar();
  }

#ifndef _WIN32
  char currWDPath[512];
  ::getcwd(currWDPath, 512);

  if (m_persistanceDir.at(0) != '/') {
    if (0 == ::strlen(currWDPath)) {
      throw InitFailedException(
          "Failed to get absolute path for persistence directory.");
    }
    m_persistanceDir = std::string(currWDPath) + "/" + m_persistanceDir;
  }

  // Create persistence directory
  LOGFINE("SqLiteImpl::init creating persistence directory: %s",
          m_persistanceDir.c_str());
  ::mkdir(m_persistanceDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

  // Create region directory
  std::string regionDirectory = m_persistanceDir + "/" + regionName;
  ::mkdir(regionDirectory.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  m_regionDBFile = regionDirectory + "/" + regionName + ".db";
  LOGFINE("SqLiteImpl::init creating persistence region file: %s",
          m_regionDBFile.c_str());

#else
  char currWDPath[512];
  GetCurrentDirectory(512, currWDPath);

  if (m_persistanceDir.find(":", 0) == std::string::npos) {
    if (currWDPath == NULL)
      throw InitFailedException(
          "Failed to get absolute path for persistence directory.");
    m_persistanceDir = std::string(currWDPath) + "/" + m_persistanceDir;
  }

  LOGINFO("SqLiteImpl::init absolute path for persistence directory: %s",
          m_persistanceDir.c_str());

  // Create persistence directory
  LOGFINE("SqLiteImpl::init creating persistence directory: %s",
          m_persistanceDir.c_str());
  CreateDirectory(m_persistanceDir.c_str(), NULL);

  // Create region directory
  std::string regionDirectory = m_persistanceDir + "/" + regionName;
  CreateDirectory(regionDirectory.c_str(), NULL);
  m_regionDBFile = regionDirectory + "/" + regionName + ".db";

  LOGFINE("SqLiteImpl::init creating persistence region file: %s",
          m_regionDBFile.c_str());
#endif

  if (m_sqliteHelper->initDB(region->getName(), maxPageCount, pageSize,
                             m_regionDBFile.c_str()) != 0) {
    throw IllegalStateException("Failed to initialize database in SQLITE.");
  }
}

void SqLiteImpl::write(const CacheableKeyPtr& key, const CacheablePtr& value,
                       void*& dbHandle) {
  // Serialize key and value.
  DataOutput keyDataBuffer, valueDataBuffer;
  uint32_t keyBufferSize, valueBufferSize;

  keyDataBuffer.writeObject(key);
  valueDataBuffer.writeObject(value);
  void* keyData = const_cast<uint8_t*>(keyDataBuffer.getBuffer(&keyBufferSize));
  void* valueData =
      const_cast<uint8_t*>(valueDataBuffer.getBuffer(&valueBufferSize));

  if (m_sqliteHelper->insertKeyValue(keyData, keyBufferSize, valueData,
                                     valueBufferSize) != 0) {
    throw IllegalStateException("Failed to write key value in SQLITE.");
  }
}

bool SqLiteImpl::writeAll() { return true; }

CacheablePtr SqLiteImpl::read(const CacheableKeyPtr& key, void*& dbHandle) {
  // Serialize key.
  DataOutput keyDataBuffer;
  uint32_t keyBufferSize;
  keyDataBuffer.writeObject(key);
  void* keyData = const_cast<uint8_t*>(keyDataBuffer.getBuffer(&keyBufferSize));
  void* valueData;
  uint32_t valueBufferSize;

  if (m_sqliteHelper->getValue(keyData, keyBufferSize, valueData,
                               valueBufferSize) != 0) {
    throw IllegalStateException("Failed to read the value from SQLITE.");
  }

  // Deserialize object and return value.
  DataInput valueDataBuffer(reinterpret_cast<uint8_t*>(valueData),
                            valueBufferSize);
  CacheablePtr retValue;
  valueDataBuffer.readObject(retValue);

  // Free memory for serialized form of Cacheable object.
  free(valueData);
  return retValue;
}

bool SqLiteImpl::readAll() { return true; }

void SqLiteImpl::destroyRegion() {
  if (m_sqliteHelper->closeDB() != 0) {
    throw IllegalStateException("Failed to destroy region from SQLITE.");
  }

#ifndef _WIN32
  ::unlink(m_regionDBFile.c_str());
  ::rmdir(m_regionDir.c_str());
  ::rmdir(m_persistanceDir.c_str());
#else
  DeleteFile(m_regionDBFile.c_str());
  RemoveDirectory(m_regionDir.c_str());
  RemoveDirectory(m_persistanceDir.c_str());
#endif
}

void SqLiteImpl::destroy(const CacheableKeyPtr& key, void*& dbHandle) {
  // Serialize key and value.
  DataOutput keyDataBuffer;
  uint32_t keyBufferSize;
  keyDataBuffer.writeObject(key);
  void* keyData = const_cast<uint8_t*>(keyDataBuffer.getBuffer(&keyBufferSize));
  if (m_sqliteHelper->removeKey(keyData, keyBufferSize) != 0) {
    throw IllegalStateException("Failed to destroy the key from SQLITE.");
  }
}

SqLiteImpl::SqLiteImpl() { m_sqliteHelper = new SqLiteHelper(); }

void SqLiteImpl::close() {
  int lastError ATTR_UNUSED = 0;
  m_sqliteHelper->closeDB();

#ifndef _WIN32
  ::unlink(m_regionDBFile.c_str());
  ::rmdir(m_regionDir.c_str());
  ::rmdir(m_persistanceDir.c_str());
#else
  DeleteFile(m_regionDBFile.c_str());
  RemoveDirectory(m_regionDir.c_str());
  RemoveDirectory(m_persistanceDir.c_str());
#endif
}

extern "C" {

LIBEXP PersistenceManager* createSqLiteInstance() { return new SqLiteImpl; }
}

#ifndef _SqLiteIMPL_HPP__
#define _SqLiteIMPL_HPP__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "SqLiteHelper.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class SqLite(3.7) Implementation SqLiteImpl.hpp
 * SqLite API for overflow.
 * The SqLiteImpl class derives from PersistenceManager base class and
 * implements a persistent store with SqLite DB.
 *
 */

class SqLiteImpl : public PersistenceManager {
  /**
   * @brief public methods
   */
 public:
  /**
   * Initializes the DB for the region. SqLite settings are passed via
   * diskProperties argument.
   * @throws InitfailedException if persistence directory/environment directory
   * initialization fails.
   */
  void init(const RegionPtr& regionptr, PropertiesPtr& diskProperties);

  /**
   * Stores a key-value pair in the SqLite implementation.
   * @param key the key to write.
   * @param value the value to write
   * @throws DiskFailureException if the write fails due to disk failure.
   */
  void write(const CacheableKeyPtr& key, const CacheablePtr& value,
             void*& dbHandle);

  /**
   * Writes the entire region into the SqLite implementation.
   * @throws DiskFailureException if the write fails due to disk fail.
   */
  bool writeAll();

  /**
   * Reads the value for the key from SqLite.
   * @returns value of type CacheablePtr.
   * @param key is the key for which the value has to be read.
   * @throws IllegalArgumentException if the key is NULL.
   * @throws DiskCorruptException if the data to be read is corrupt.
   */
  CacheablePtr read(const CacheableKeyPtr& key, void*& dbHandle);

  /**
   * Read all the keys and values for a region stored in SqLite.
   */
  bool readAll();

  /**
   * Invalidates an entry stored in SqLite.
   * @throws IllegalArgumentException if the key is NULL.
   * @throws RegionDestroyedException is the region is already destroyed.
   * @throws EntryNotFoundException if the entry is not found on the disk.
   */
  // void invalidate(const CacheableKeyPtr& key);

  /**
   * Destroys an entry stored in SqLite. .
   * @throws IllegalArgumentException if the key is NULL.
   * @throws RegionDestroyedException is the region is already destroyed.
   * @throws EntryNotFoundException if the entry is not found on the disk.
   */
  void destroy(const CacheableKeyPtr& key, void*& dbHandle);

  /**
   * Returns number of entries stored in SqLite for the region.
   */
  //  are we removing this method from PersistenceManager?
  // int numEntries() const;

  /**
   * Destroys the region in the SqLite implementation.
   * @throws RegionDestroyedException is the region is already destroyed.
   */
  void destroyRegion();

  /**
   * Closes the SqLite persistence manager implementation.
   * @throws ShutdownFailedException if clean-up of region and environment files
   * fails..
   */
  void close();

  /**
   * @brief destructor
   */
  ~SqLiteImpl() { LOGDEBUG("SqLiteImpl::~SqLiteImpl calling  ~SqLiteImpl"); }

  /**
   * @brief constructor
   */
  SqLiteImpl();

  /**
   * @brief private members
   */

 private:
  SqLiteHelper* m_sqliteHelper;

  RegionPtr m_regionPtr;
  std::string m_regionDBFile;
  std::string m_regionDir;
  std::string m_persistanceDir;
};
}
#endif  //_SqLiteIMPL_HPP__

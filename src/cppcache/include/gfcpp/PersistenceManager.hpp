#pragma once

#ifndef GEODE_GFCPP_PERSISTENCEMANAGER_H_
#define GEODE_GFCPP_PERSISTENCEMANAGER_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "DistributedSystem.hpp"
#include "ExceptionTypes.hpp"
#include "CacheableKey.hpp"
#include "Cacheable.hpp"

/**
 * @file
 */

#define MAX_PAGE_COUNT "MaxPageCount"
#define PAGE_SIZE "PageSize"
#define PERSISTENCE_DIR "PersistenceDirectory"

namespace apache {
namespace geode {
namespace client {

typedef PersistenceManagerPtr (*getPersistenceManagerInstance)(
    const RegionPtr&);

/**
 * @class PersistenceManager PersistenceManager.hpp
 * PersistenceManager API for persistence and overflow.
 * This class abstracts the disk-related operations in case of persistence or
 * overflow to disk.
 * A specific disk storage implementation will implement all the methods
 * described here.
 */
class CPPCACHE_EXPORT PersistenceManager : public SharedBase {
  /**
   * @brief public methods
   */
 public:
  /**
  * Returns the current persistence manager.
  * @return persistence manager
  */
  static PersistenceManagerPtr getPersistenceManager();

  /**
  * Writes a key, value pair of region to the disk. The actual file or database
  * related write operations should be implemented
  * in this method by the class implementing this method.
  * @param key the key to write.
  * @param value the value to write
  * @param PersistenceInfo related persistence information.
  * @throws RegionDestroyedException is the region is already destroyed.
  * @throws OutofMemoryException if the disk is full
  * @throws DiskFailureException if the write fails due to disk fail.
  */
  virtual void write(const CacheableKeyPtr& key, const CacheablePtr& value,
                     void*& PersistenceInfo) = 0;

  /**
   * Writes all the entries for a region. Refer persistance requirement doc for
   * the use case.
   * @throws DiskFailureException if the write fails due to disk fail.
   */
  virtual bool writeAll() = 0;

  /**
   * This method gets called after an implementation object is created.
   * Initializes all the implementation
   * specific environments needed.
   * @throws InitfailedException if the persistence manager cannot be
   * initialized.
   */
  virtual void init(const RegionPtr& region, PropertiesPtr& diskProperties) = 0;

  /**
  * Reads the value for the key from the disk.
  * @param key is the key for which the value has to be read.
  * @param PersistenceInfo related persistence information.
  * @returns value of type CacheablePtr.
  * @throws DiskCorruptException if the data to be read is corrupt.
  */
  virtual CacheablePtr read(const CacheableKeyPtr& key,
                            void*& PersistenceInfo) = 0;

  /**
  * Reads all the values from the region.
  * @return true
  */
  virtual bool readAll() = 0;

  /**
  * destroys the entry specified by the key in the argument.
  * @param key is the key of the entry which is being destroyed.
  * @param PersistenceInfo related persistence information.
  * @throws RegionDestroyedException is the region is already destroyed.
  * @throws EntryNotFoundException if the entry is not found on the disk.
  */
  virtual void destroy(const CacheableKeyPtr& key, void*& PersistenceInfo) = 0;

  /**
   * Closes the persistence manager instance.
   * @throws ShutdownFailedException if close is not successful.
   */
  virtual void close() = 0;

  PersistenceManager(const RegionPtr& regionPtr);
  PersistenceManager();
  /**
    * @brief destructor
    */
  virtual ~PersistenceManager() = 0;

 protected:
  /** Region for this persistence manager.
   */
  const RegionPtr m_regionPtr;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_PERSISTENCEMANAGER_H_

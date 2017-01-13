/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_GEMFIRECACHE_H__
#define __GEMFIRE_GEMFIRECACHE_H__

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "RegionService.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * GemFireCache represents the singleton cache that must be created
 * in order to connect to Gemfire server.
 * Users must create a {@link Cache}.
 * Instances of this interface are created using one of the following methods:
 * <ul>
 * <li> {@link ClientCacheFactory#create()} creates a client instance of {@link
 * Cache}.
 * </ul>
 *
 */

class CPPCACHE_EXPORT GemFireCache : public RegionService {
  /**
   * @brief public methods
   */
 public:
  /** Returns the name of this cache.
   * @return the string name of this cache
   */
  virtual const char* getName() const = 0;

  /**
   * Initializes the cache from an xml file
   *
   * @param cacheXml
   *        Valid cache.xml file
   */
  virtual void initializeDeclarativeCache(const char* cacheXml) = 0;

  /**
  * Returns the distributed system that this cache was
  * {@link CacheFactory::createCacheFactory created} with.
  */
  virtual DistributedSystemPtr getDistributedSystem() const = 0;

  /**
   * Returns whether Cache saves unread fields for Pdx types.
   */
  virtual bool getPdxIgnoreUnreadFields() = 0;

  /**
  * Returns whether { @link PdxInstance} is preferred for PDX types instead of
  * C++ object.
  */
  virtual bool getPdxReadSerialized() = 0;
};

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_GEMFIRECACHE_H__

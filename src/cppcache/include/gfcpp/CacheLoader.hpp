#ifndef __GEMFIRE_CACHELOADER_H__
#define __GEMFIRE_CACHELOADER_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableKey.hpp"
#include "Cacheable.hpp"
#include "UserData.hpp"

namespace gemfire {

/**
 * @class CacheLoader CacheLoader.hpp
 * An application plug-in that can be installed on a region. Loaders
 * facilitate loading of data into the cache. When an application does a
 * lookup for a key in a region and it does not exist, the system checks to
 * see if any loaders are available for the region in the system and
 * invokes them to get the value for the key into the cache.
 * Allows data to be loaded from a 3rd party data source and placed
 * into the region
 * When {@link Region::get} is called for a region
 * entry that has a <code>NULLPTR</code> value, the
 * {@link CacheLoader::load} method of the
 * region's cache loader is invoked.  The <code>load</code> method
 * creates the value for the desired key by performing an operation such
 * as a database query.
 *
 * @see AttributesFactory::setCacheLoader
 * @see RegionAttributes::getCacheLoader
 */
class CPPCACHE_EXPORT CacheLoader : public SharedBase {
 public:
  /**Loads a value. Application writers should implement this
   * method to customize the loading of a value. This method is called
   * by the caching service when the requested value is not in the cache.
   * Any exception thrown by this method is propagated back to and thrown
   * by the invocation of {@link Region::get} that triggered this load.
   * @param rp a Region Pointer for which this is called.
   * @param key the key for the cacheable
   * @param helper any related user data, or NULLPTR
   * @return the value supplied for this key, or NULLPTR if no value can be
   * supplied.
   *
   *@see Region::get .
   */
  virtual CacheablePtr load(const RegionPtr& rp, const CacheableKeyPtr& key,
                            const UserDataPtr& aCallbackArgument) = 0;

  /** Called when the region containing this callback is destroyed, when
   * the cache is closed.
   *
   * <p>Implementations should clean up any external
   * resources, such as database connections. Any runtime exceptions this method
   * throws will be logged.
   *
   * <p>It is possible for this method to be called multiple times on a single
   * callback instance, so implementations must be tolerant of this.
   *
   * @param rp the region pointer
   *
   * @see Cache::close
   * @see Region::destroyRegion
   */
  virtual void close(const RegionPtr& rp);

  virtual ~CacheLoader();

 protected:
  CacheLoader();

 protected:
  // never implemented.
  CacheLoader(const CacheLoader& other);
  void operator=(const CacheLoader& other);
};

}  // namespace
#endif  // ifndef __GEMFIRE_CACHELOADER_H__

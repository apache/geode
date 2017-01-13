#ifndef _GEMFIRE_CACHEXMLCREATION_HPP_
#define _GEMFIRE_CACHEXMLCREATION_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "RegionXmlCreation.hpp"
#include "PoolXmlCreation.hpp"
#include <vector>

namespace gemfire {

class Cache;
/**
 * Represents a {@link Cache} that is created declaratively.
 *
 * @since 1.0
 */

class CPPCACHE_EXPORT CacheXmlCreation {
 public:
  /**
   * Creates a new <code>CacheXmlCreation</code> with no root region
   */
  CacheXmlCreation();

  /**
   * Adds a root region to the cache
   */
  void addRootRegion(RegionXmlCreation* root);

  /** Adds a pool to the cache */
  void addPool(PoolXmlCreation* pool);

  /**
   * Fills in the contents of a {@link Cache} based on this creation
   * object's state.
   *
   * @param  cache
   *         The cache which is to be populated
   * @throws OutOfMemoryException if the memory allocation failed
   * @throws NotConnectedException if the cache is not connected
   * @throws InvalidArgumentException if the attributePtr is NULL.
   *         or if RegionAttributes is null or if regionName is null,
   *         the empty   string, or contains a '/'
   * @throws RegionExistsException
   * @throws CacheClosedException if the cache is closed
   *         when the region is created.
   * @throws UnknownException otherwise
   *
   */
  void create(Cache* cache);

  void setPdxIgnoreUnreadField(bool ignore);

  void setPdxReadSerialized(bool val);

  bool getPdxIgnoreUnreadField() { return m_pdxIgnoreUnreadFields; }

  bool getPdxReadSerialized(bool val) { return m_readPdxSerialized; }

  ~CacheXmlCreation();

 private:
  /** This cache's roots */
  std::vector<RegionXmlCreation*> rootRegions;

  /** This cache's pools */
  std::vector<PoolXmlCreation*> pools;

  Cache* m_cache;
  bool m_pdxIgnoreUnreadFields;
  bool m_readPdxSerialized;
};
};

#endif  // #ifndef  _GEMFIRE_CACHEXMLCREATION_HPP_

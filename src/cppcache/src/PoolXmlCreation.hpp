#ifndef _GEMFIRE_POOLXMLCREATION_HPP_
#define _GEMFIRE_POOLXMLCREATION_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <string>
#include <vector>
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/Pool.hpp>
#include "PoolXmlCreation.hpp"
#include <gfcpp/PoolFactory.hpp>

namespace gemfire {
class Cache;

/**
 * Represents a {@link Pool} that is created declaratively.
 *
 * @since 3.0
 */
class CPPCACHE_EXPORT PoolXmlCreation {
 private:
  /** An <code>AttributesFactory</code> for creating default
    * <code>PoolAttribute</code>s */
  PoolFactoryPtr poolFactory;

  /** The name of this pool */
  std::string poolName;

  /*
  std::vector<std::string> locatorhosts;
  std::vector<std::string> locatorports;
  std::vector<std::string> serverhosts;
  std::vector<std::string> serverports;
  */

 public:
  ~PoolXmlCreation();
  /**
   * Creates a new <code>PoolXmlCreation</code> with the given pool name.
   */
  PoolXmlCreation(const char* name, PoolFactoryPtr factory);

  /** Add a locator */
  // void addLocator(const char * host, const char * port);

  /** Add a server */
  // void addServer(const char * host, const char * port);

  /**
   * Creates a {@link Pool} using the
   * description provided by this <code>PoolXmlCreation</code>.
   *
   * @throws OutOfMemoryException if the memory allocation failed
   * @throws NotConnectedException if the cache is not connected
   * @throws InvalidArgumentException if the attributePtr is NULL.
   * or if PoolAttributes is null or if poolName is null or
   * the empty string
   * @throws PoolExistsException
   * @throws CacheClosedException if the cache is closed
   *         at the time of region creation
   * @throws UnknownException otherwise
   *
   */
  PoolPtr create();
};
};
#endif  // #ifndef  _GEMFIRE_POOLXMLCREATION_HPP_

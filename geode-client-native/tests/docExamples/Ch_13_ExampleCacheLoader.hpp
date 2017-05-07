/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

/**
* @file ExampleCacheLoader.hpp
* @since   1.0
* @version 1.0
* @see
*/

#ifndef __TEST_CACHE_LOADER_HPP__
#define __TEST_CACHE_LOADER_HPP__

#include "gfcpp/GemfireCppCache.hpp"
#include <gfcpp/CacheLoader.hpp>

using namespace gemfire;
using namespace docExample;

class CacheLoaderCheck
{
public:
  CacheLoaderCheck();
  ~CacheLoaderCheck();
  void startServer();
  void stopServer();
  void initRegion();

public:
  CacheFactoryPtr cacheFactoryPtr; // for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
};

#endif // __TEST_CACHE_LOADER_HPP__

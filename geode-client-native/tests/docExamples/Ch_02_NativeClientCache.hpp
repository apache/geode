/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CH_02_NATIVECLIENTCACHE_HPP_
#define CH_02_NATIVECLIENTCACHE_HPP_

/*
* This is the example to verify the code snippets given in the native client guide chapter 2.
* This is the example with The Native Client Cache.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class NativeClientCache
{
public:
  NativeClientCache();
  ~NativeClientCache();
  void startServer();
  void stopServer();
  void initRegion();
  void initCache(CacheFactoryPtr& cacheFactoryPtr,CachePtr& cachePtr, const char* xmlfile = NULL);
  void cleanUp(CachePtr cachePtr);
  void createValues();
  void cleanUp();
  void example_2_3();
  void example_2_4();
  void example_2_5();
  void example_2_6();
  void example_2_7();
  void example_2_8();
  void example_2_9();
  void setListener(RegionPtr& region);
  void setListenerUsingFactory(RegionPtr& region);
  void removeListener(RegionPtr& region);
  void setWriter(RegionPtr& region);
  void setWriterUsingFactory(RegionPtr& region);
  void removeWriter(RegionPtr& region);
  void setLoader(RegionPtr& region);
  void setLoaderUsingFactory(RegionPtr& region);
  void removeLoader(RegionPtr& region);

public:
  CacheFactoryPtr cacheFactoryPtr; //for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regPtr0; //for examples.
  RegionPtr regPtr1; //for examples.
  VectorOfCacheableKey keys0; //for examples.
  VectorOfCacheableKey keys1; //for examples.
  CacheableStringPtr keyPtr1; //for examples.
  CacheableStringPtr keyPtr3; //for examples.
};

#endif /* CH_02_NATIVECLIENTCACHE_HPP_ */

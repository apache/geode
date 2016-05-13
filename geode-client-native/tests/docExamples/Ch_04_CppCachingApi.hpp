/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* This is the example to verify the code snippet given in the native client guide chapter 4.
* This the example with c++ caching API
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"


using namespace gemfire;

class CppCachingApi
{
public:
  CppCachingApi();
  ~CppCachingApi();
  void startServer();
  void stopServer();
  void example_4_1();
  void example_4_2();
  void example_4_3();
  void example_4_4();
  void example_4_5();
  void example_4_6();
  void example_4_12();

public:
  CacheFactoryPtr cacheFactoryPtr; // for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
};

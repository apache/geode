/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CONNECTIONPOOLS_HPP_
#define CONNECTIONPOOLS_HPP_
/*
* This is the example to verify the code snippets given in the native client guide chapter 11.
* This is the example with Using Connection Pools.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class ConnectionPools
{
public:
  ConnectionPools();
  ~ConnectionPools();
  void startLocator();
  void stopLocator();
  void startServer();
  void stopServer();
  void example_11_2();
public:
  CacheFactoryPtr systemPtr; //for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
};

#endif /* CONNECTIONPOOLS_HPP_ */

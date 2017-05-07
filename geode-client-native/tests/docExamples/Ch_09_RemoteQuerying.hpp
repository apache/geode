/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef REMOTEQUERYING_HPP_
#define REMOTEQUERYING_HPP_

/*
* This is the example to verify the code snippets given in the native client guide chapter 9.
* This is the example with Remote Querying.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class RemoteQuerying
{
public:
  RemoteQuerying();
  ~RemoteQuerying();
  void startServer();
  void stopServer();
  void initRegion();
  void example_9_2();
  void example_9_3();
  void example_9_4();
  void example_9_14();
  void example_9_15();
  void example_9_16();
  void example_9_17();
  void example_9_18();
  void example_9_19();

public:
  CacheFactoryPtr cacheFactoryPtr; //for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
  RegionPtr posRegion; //for examples.
};
#endif /* REMOTEQUERYING_HPP_ */

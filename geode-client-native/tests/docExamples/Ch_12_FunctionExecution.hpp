/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef FUNCTIONEXECUTION_HPP_
#define FUNCTIONEXECUTION_HPP_

/*
* This is the example to verify the code snippets given in the native client guide chapter 12.
* This is the example with Function Execution.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class FunctionExecution
{
public:
  FunctionExecution();
  ~FunctionExecution();
  void startServer();
  void stopServer();
  void initPool();
  RegionPtr initRegion();
  void registerAll();
  void createPoolRegion();
  PoolPtr createPool(const char* poolName, const char* serverGroup,
    bool locator = true, bool server = false, int redundancy = 0,
    bool clientNotification = false, int subscriptionAckInterval = -1);
  void example_12_1();
  void example_12_3();

public:
  CacheFactoryPtr cacheFactoryPtr; //for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regPtr0; //for examples.
  PoolPtr pptr; //for examples.
};

#endif /* FUNCTIONEXECUTION_HPP_ */

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef EXAMPLETESTXML_HPP_
#define EXAMPLETESTXML_HPP_

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"


using namespace gemfire;

class ExampleTestXml
{
public:
  ExampleTestXml();
  ~ExampleTestXml();
  void startServer();
  void startLocator();
  void stopLocator();
  void stopServer();
  void connectToDS();
  void initCache();
  void initCache_7_3();
  void initCache_3_1();
  void initCache_2_1();
  void cleanUp();
  void example_3_1();
  void example_7_3();
  void example_9_13();
  void example_13_1();
  void example_2_1();

public:
  CacheFactoryPtr cacheFactoryPtr; // for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
};

#endif /* EXAMPLETESTXML_HPP_ */

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef PRESERVINGDATA_HPP_
#define PRESERVINGDATA_HPP_

/*
* This is the example to verify the code snippets given in the native client guide chapter 7.
* This is the example with Preserving Data.
*/

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"

using namespace gemfire;

class PreservingData
{
public:
  PreservingData();
  ~PreservingData();
  void startServer();
  void stopServer();
  void initRegion();
  void example_7_2();
  void example_7_5();
  void example_7_6();
  void example_7_7();
  void example_7_8();

public:
  CacheFactoryPtr systemPtr; // for examples.
  CacheFactoryPtr cacheFactoryPtr; //for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr; //for examples.
};

#endif /* PRESERVINGDATA_HPP_ */

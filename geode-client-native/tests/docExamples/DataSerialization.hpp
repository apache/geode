/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DATASERIALISATION_HPP__
#define __DATASERIALISATION_HPP__

#include "CacheHelper.hpp"
#include "Ch_13_User.hpp"
#include "Ch_13_ExampleObject.hpp"

using namespace gemfire;
using namespace docExample;

class DataSerialization
{
public:
  DataSerialization();
  ~DataSerialization();
  void startServer();
  void stopServer();
  void initRegion();
public:
  CacheFactoryPtr cacheFactoryPtr; // for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr1; //for examples.
  RegionPtr regionPtr2; //for examples.
};

#endif

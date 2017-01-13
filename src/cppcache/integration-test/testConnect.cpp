/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testConnect"

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

using namespace gemfire;

const char* host_name = "Suds";
DUNIT_TASK(s1p1, CreateRegionOne)
  {
    try {
      DistributedSystem::disconnect();
      FAIL("Expected an exception.");
    } catch (const NotConnectedException& ex) {
      LOG("Got expected exception.");
      LOG(ex.getMessage());
    }
    try {
      DistributedSystemPtr dsys = DistributedSystem::connect(host_name);
      if (!dsys->isConnected()) FAIL("Distributed system is not connected");
    } catch (const Exception& ex) {
      LOG(ex.getMessage());
      ASSERT(false, "connect failed.");
    }
  }
ENDTASK

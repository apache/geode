/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef THINCLIENTDISTOPS2_HPP_
#define THINCLIENTDISTOPS2_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>

#include <string>
#include <vector>

#define ROOT_NAME "ThinClientDistOps2"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;
const char* poolName = "__TEST_POOL1__";

const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

#include "LocatorHelper.hpp"

#define verifyEntry(a, b, c, d) _verifyEntry(a, b, c, d, __LINE__)

void _verifyEntry(const char* name, const char* key, const char* val,
                  bool checkLocal, int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyEntry() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, val, false, checkLocal);
  LOG("Entry verified.");
}

const char* _keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* _vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* _nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                        "New Value-4"};

const char* _regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2And3)
  {
    if (isLocalServer) CacheHelper::initServer(2);
    if (isLocalServer) CacheHelper::initServer(3);
    LOG("SERVER23 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2And3_Locator)
  {
    if (isLocalServer) CacheHelper::initServer(2, NULL, locatorsG);
    if (isLocalServer) CacheHelper::initServer(3, NULL, locatorsG);
    LOG("SERVER23 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1Regions_Pooled_Locator)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    createPooledRegion(_regionNames[0], USE_ACK, locatorsG, poolName);
    createPooledRegion(_regionNames[1], NO_ACK, locatorsG, poolName);
    LOG("CreateClient1Regions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2Regions_Pooled_Locator)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    createPooledRegion(_regionNames[0], USE_ACK, locatorsG, poolName);
    createPooledRegion(_regionNames[1], NO_ACK, locatorsG, poolName);
    LOG("CreateClient1Regions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1Entries)
  {
    createEntry(_regionNames[0], _keys[0], _vals[0]);
    createEntry(_regionNames[1], _keys[2], _vals[2]);
    LOG("CreateClient1Entries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2Entries)
  {
    doNetsearch(_regionNames[0], _keys[0], _vals[0]);
    doNetsearch(_regionNames[1], _keys[2], _vals[2]);
    createEntry(_regionNames[0], _keys[1], _vals[1]);
    createEntry(_regionNames[1], _keys[3], _vals[3]);
    LOG("CreateClient2Entries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UpdateClient1Entry)
  {
    doNetsearch(_regionNames[0], _keys[1], _vals[1]);
    doNetsearch(_regionNames[1], _keys[3], _vals[3]);
    updateEntry(_regionNames[0], _keys[0], _nvals[0]);
    updateEntry(_regionNames[1], _keys[2], _nvals[2]);
    LOG("UpdateClient1Entry complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, UpdateClient2Entry)
  {
    doNetsearch(_regionNames[0], _keys[0], _vals[0], false);
    doNetsearch(_regionNames[1], _keys[2], _vals[2], false);
    updateEntry(_regionNames[0], _keys[1], _nvals[1]);
    updateEntry(_regionNames[1], _keys[3], _nvals[3]);
    LOG("UpdateClient2Entry complete.");
  }
END_TASK_DEFINITION

// Test for getAll
DUNIT_TASK_DEFINITION(CLIENT1, Client1GetAll)
  {
    RegionPtr reg0 = getHelper()->getRegion(_regionNames[0]);

    VectorOfCacheableKey keys0;
    CacheableKeyPtr key0 = CacheableString::create(_keys[0]);
    CacheableKeyPtr key1 = CacheableString::create(_keys[1]);

    // test invalid combination with caching disabled for getAll
    reg0->localDestroyRegion();
    reg0 = NULLPTR;
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, 0,
                                    "__TEST_POOL1__", false, false);
    reg0 = getHelper()->getRegion(_regionNames[0]);
    keys0.push_back(key0);
    keys0.push_back(key1);
    try {
      reg0->getAll(keys0, NULLPTR, NULLPTR, true);
      FAIL("Expected IllegalArgumentException");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }
    // re-create region with caching enabled
    reg0->localDestroyRegion();
    reg0 = NULLPTR;
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, 0,
                                    "__TEST_POOL1__", true, true);
    reg0 = getHelper()->getRegion(_regionNames[0]);
    // check for IllegalArgumentException for empty key list
    HashMapOfCacheablePtr values(new HashMapOfCacheable());
    HashMapOfExceptionPtr exceptions(new HashMapOfException());
    keys0.clear();
    try {
      reg0->getAll(keys0, values, exceptions);
      FAIL("Expected IllegalArgumentException");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }
    keys0.push_back(key0);
    keys0.push_back(key1);
    try {
      reg0->getAll(keys0, NULLPTR, NULLPTR, false);
      FAIL("Expected IllegalArgumentException");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }

    reg0->getAll(keys0, values, exceptions);
    ASSERT(values->size() == 2, "Expected 2 values");
    ASSERT(exceptions->size() == 0, "Expected no exceptions");
    CacheableStringPtr val0 =
        dynCast<CacheableStringPtr>(values->operator[](key0));
    CacheableStringPtr val1 =
        dynCast<CacheableStringPtr>(values->operator[](key1));
    ASSERT(strcmp(_nvals[0], val0->asChar()) == 0, "Got unexpected value");
    ASSERT(strcmp(_nvals[1], val1->asChar()) == 0, "Got unexpected value");

    // for second region invalidate only one key to have a partial get
    // from java server
    RegionPtr reg1 = getHelper()->getRegion(_regionNames[1]);
    CacheableKeyPtr key2 = CacheableString::create(_keys[2]);
    CacheableKeyPtr key3 = CacheableString::create(_keys[3]);
    reg1->localInvalidate(key2);
    VectorOfCacheableKey keys1;
    keys1.push_back(key2);
    keys1.push_back(key3);

    values->clear();
    exceptions->clear();
    reg1->getAll(keys1, values, exceptions, true);
    ASSERT(values->size() == 2, "Expected 2 values");
    ASSERT(exceptions->size() == 0, "Expected no exceptions");
    CacheableStringPtr val2 =
        dynCast<CacheableStringPtr>(values->operator[](key2));
    CacheableStringPtr val3 =
        dynCast<CacheableStringPtr>(values->operator[](key3));
    ASSERT(strcmp(_nvals[2], val2->asChar()) == 0, "Got unexpected value");
    ASSERT(strcmp(_vals[3], val3->asChar()) == 0, "Got unexpected value");

    // also check that the region is properly populated
    ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
    VectorOfRegionEntry regEntries;
    reg1->entries(regEntries, false);
    ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
    verifyEntry(_regionNames[1], _keys[2], _nvals[2], true);
    verifyEntry(_regionNames[1], _keys[3], _vals[3], true);

    // also check with NULL values that region is properly populated
    reg1->localInvalidate(key3);
    values = NULLPTR;
    exceptions->clear();
    reg1->getAll(keys1, values, exceptions, true);
    // now check that the region is properly populated
    ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
    regEntries.clear();
    reg1->entries(regEntries, false);
    ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
    verifyEntry(_regionNames[1], _keys[2], _nvals[2], true);
    verifyEntry(_regionNames[1], _keys[3], _nvals[3], true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1GetAll_Pool)
  {
    RegionPtr reg0 = getHelper()->getRegion(_regionNames[0]);

    VectorOfCacheableKey keys0;
    CacheableKeyPtr key0 = CacheableString::create(_keys[0]);
    CacheableKeyPtr key1 = CacheableString::create(_keys[1]);

    // test invalid combination with caching disabled for getAll
    reg0->localDestroyRegion();
    reg0 = NULLPTR;
    getHelper()->createRegionAndAttachPool(_regionNames[0], USE_ACK, poolName,
                                           false);
    reg0 = getHelper()->getRegion(_regionNames[0]);
    keys0.push_back(key0);
    keys0.push_back(key1);
    try {
      reg0->getAll(keys0, NULLPTR, NULLPTR, true);
      FAIL("Expected IllegalArgumentException");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }
    // re-create region with caching enabled
    reg0->localDestroyRegion();
    reg0 = NULLPTR;
    getHelper()->createRegionAndAttachPool(_regionNames[0], USE_ACK, poolName);
    reg0 = getHelper()->getRegion(_regionNames[0]);
    // check for IllegalArgumentException for empty key list
    HashMapOfCacheablePtr values(new HashMapOfCacheable());
    HashMapOfExceptionPtr exceptions(new HashMapOfException());
    keys0.clear();
    try {
      reg0->getAll(keys0, values, exceptions);
      FAIL("Expected IllegalArgumentException");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }
    keys0.push_back(key0);
    keys0.push_back(key1);
    try {
      reg0->getAll(keys0, NULLPTR, NULLPTR, false);
      FAIL("Expected IllegalArgumentException");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }

    reg0->getAll(keys0, values, exceptions);
    ASSERT(values->size() == 2, "Expected 2 values");
    ASSERT(exceptions->size() == 0, "Expected no exceptions");
    CacheableStringPtr val0 =
        dynCast<CacheableStringPtr>(values->operator[](key0));
    CacheableStringPtr val1 =
        dynCast<CacheableStringPtr>(values->operator[](key1));
    ASSERT(strcmp(_nvals[0], val0->asChar()) == 0, "Got unexpected value");
    ASSERT(strcmp(_nvals[1], val1->asChar()) == 0, "Got unexpected value");

    // for second region invalidate only one key to have a partial get
    // from java server
    RegionPtr reg1 = getHelper()->getRegion(_regionNames[1]);
    CacheableKeyPtr key2 = CacheableString::create(_keys[2]);
    CacheableKeyPtr key3 = CacheableString::create(_keys[3]);
    reg1->localInvalidate(key2);
    VectorOfCacheableKey keys1;
    keys1.push_back(key2);
    keys1.push_back(key3);

    values->clear();
    exceptions->clear();
    reg1->getAll(keys1, values, exceptions, true);
    ASSERT(values->size() == 2, "Expected 2 values");
    ASSERT(exceptions->size() == 0, "Expected no exceptions");
    CacheableStringPtr val2 =
        dynCast<CacheableStringPtr>(values->operator[](key2));
    CacheableStringPtr val3 =
        dynCast<CacheableStringPtr>(values->operator[](key3));
    ASSERT(strcmp(_nvals[2], val2->asChar()) == 0, "Got unexpected value");
    ASSERT(strcmp(_vals[3], val3->asChar()) == 0, "Got unexpected value");

    // also check that the region is properly populated
    ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
    VectorOfRegionEntry regEntries;
    reg1->entries(regEntries, false);
    ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
    verifyEntry(_regionNames[1], _keys[2], _nvals[2], true);
    verifyEntry(_regionNames[1], _keys[3], _vals[3], true);

    // also check with NULL values that region is properly populated
    reg1->localInvalidate(key3);
    values = NULLPTR;
    exceptions->clear();
    reg1->getAll(keys1, values, exceptions, true);
    // now check that the region is properly populated
    ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
    regEntries.clear();
    reg1->entries(regEntries, false);
    ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
    verifyEntry(_regionNames[1], _keys[2], _nvals[2], true);
    verifyEntry(_regionNames[1], _keys[3], _nvals[3], true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
    if (isLocalServer) {
      CacheHelper::closeServer(3);
      LOG("SERVER3 stopped");
    }
  }
END_TASK_DEFINITION

#endif /* THINCLIENTDISTOPS2_HPP_ */

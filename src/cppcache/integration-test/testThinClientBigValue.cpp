/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gf_base.hpp>

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

#define MEGABYTE (1024 * 1024)
#define GROWTH 5 * MEGABYTE
#define MAX_PAYLOAD 20 * MEGABYTE
#define MAX_PUTS 100000

void grow(int* iptr) { *iptr = *iptr + GROWTH; }

void putSize(RegionPtr& rptr, const char* buf, int size) {
  char msg[1024];
  CacheableKeyPtr keyPtr = CacheableKey::create(buf);
  uint8_t base = 0;

  uint8_t* valbuf = new uint8_t[size + 1];
  for (int i = 0; i <= size; i++) {
    valbuf[i] = base;
    if (base == 255) {
      base = 0;
    } else {
      base++;
    }
  }
  CacheableBytesPtr valPtr = CacheableBytes::create(valbuf, size);
  // CacheableStringPtr valPtr = CacheableString::create( valbuf);
  sprintf(msg, "about to put key: %s, with value size: %d", buf, size);
  LOG(msg);
  rptr->put(keyPtr, valPtr);
  delete[] valbuf;
  sprintf(msg, "put key: %s, with value size: %d", buf, size);
  LOG(msg);
}

void verify(CacheableBytesPtr& valuePtr, int size) {
  char msg[200];
  sprintf(msg, "verifying value of size %d", size);
  ASSERT(size == 0 || valuePtr != NULLPTR, msg);
  sprintf(msg, "value size is not %d", size);
  int tryCnt = 0;
  bool notIt = true;
  int valSize = (valuePtr != NULLPTR ? valuePtr->length() : 0);
  while ((notIt == true) && (tryCnt++ < 10)) {
    notIt = (valSize != size);
    SLEEP(100);
  }

  ASSERT(valSize == size, msg);

  uint8_t base = 0;
  printf("check size[%d]\n", size);
  for (int i = 0; i < size; i++) {
    if (valuePtr->value()[i] != base) {
      sprintf(msg, "verifying buf[%d] == %d for size %d, found %d instead", i,
              base, size, valuePtr->value()[i]);
    }
    ASSERT(valuePtr->value()[i] == base, msg);
    if (base == 255) {
      base = 0;
    } else {
      base++;
    }
  }
}

static int numberOfLocators = 1;
bool isLocalServer = true;
bool isLocator = true;
const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

DUNIT_TASK(SERVER1, StartServer)
  {
    if (isLocalServer) {
      CacheHelper::initLocator(1);
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locHostPort);
    }
    LOG("SERVER started");
  }
END_TASK(StartServer)

DUNIT_TASK(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locHostPort,
                                    "__TEST_POOL1__", true, true);
  }
END_TASK(SetupClient1)

DUNIT_TASK(CLIENT2, SetupClient2)
  {
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locHostPort,
                                    "__TEST_POOL1__", true, true);
  }
END_TASK(SetupClient2)

DUNIT_TASK(CLIENT1, puts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    char buf[1024];
    LOG("Beginning puts.");
    int expectEntries = 0;
    for (int i = 0; i <= MAX_PAYLOAD; grow(&i)) {
      sprintf(buf, "put size=%d\n", i);
      LOG(buf);
      char keybuf[100];
      sprintf(keybuf, "key%010d", i);
      putSize(regPtr, keybuf, i);
      expectEntries++;
    }
    dunit::globals()->rebind("entriesToExpect", expectEntries);
    LOG("Finished putting entries.");
  }
END_TASK(puts)

DUNIT_TASK(CLIENT2, VerifyPuts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    // region should already have n entries...
    int entriesExpected ATTR_UNUSED =
        dunit::globals()->getIntValue("entriesToExpect");

    for (int i = 0; i <= MAX_PAYLOAD; grow(&i)) {
      char keybuf[100];
      sprintf(keybuf, "key%010d", i);
      CacheableKeyPtr keyPtr = CacheableKey::create(keybuf);
      CacheableBytesPtr valPtr =
          dynCast<CacheableBytesPtr>(regPtr->get(keyPtr));
      int ntry = 20;
      while ((ntry-- > 0) && (valPtr == NULLPTR)) {
        SLEEP(200);
        valPtr = dynCast<CacheableBytesPtr>(regPtr->get(keyPtr));
      }
      LOG("from VerifyPuts");
      verify(valPtr, i);
    }
    LOG("On client Found all entries with correct size via get.");
  }
END_TASK(VerifyPuts)

DUNIT_TASK(CLIENT1, ManyPuts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    LOG("Beginning many puts.");
    int expectEntries = 0;
    char keybuf[100];
    char valbuf[200];
    for (int index = 0; index < MAX_PUTS; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      sprintf(valbuf, "values1%0100d", index);
      regPtr->put(keybuf, valbuf);
      expectEntries++;
    }
    dunit::globals()->rebind("entriesToExpect", expectEntries);
    LOG("Finished putting entries.");
  }
END_TASK(ManyPuts)

DUNIT_TASK(CLIENT2, VerifyManyPuts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    // region should already have n entries...
    int entriesExpected = dunit::globals()->getIntValue("entriesToExpect");

    char keybuf[100];
    for (int index = 0; index < entriesExpected; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(regPtr->get(keybuf));
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 107, "unexpected size of value in verify");
    }
    LOG("On client Found all entries with correct size via get.");
  }
END_TASK(VerifyManyPuts)

DUNIT_TASK(CLIENT1, UpdateManyPuts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    LOG("Beginning updated many puts.");
    int expectEntries = 0;
    char keybuf[100];
    char valbuf[1100];
    for (int index = 0; index < MAX_PUTS; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      sprintf(valbuf, "values2%01000d", index);
      regPtr->put(keybuf, valbuf);
      expectEntries++;
    }
    dunit::globals()->rebind("entriesToExpect", expectEntries);
    LOG("Finished putting entries.");
  }
END_TASK(UpdateManyPuts)

DUNIT_TASK(CLIENT2, VerifyOldManyPuts)
  {
    // region should given old entries from cache
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    int entriesExpected = dunit::globals()->getIntValue("entriesToExpect");

    char keybuf[100];
    for (int index = 0; index < entriesExpected; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(regPtr->get(keybuf));
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 107, "unexpected size of value in verify");
    }
    LOG("On client Found all entries with correct size via "
        "get.");
  }
END_TASK(VerifyOldManyPuts)

DUNIT_TASK(CLIENT2, VerifyUpdatedManyPuts)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    // region should already have n entries...
    int entriesExpected = dunit::globals()->getIntValue("entriesToExpect");
    // invalidate the region to force getting new values
    regPtr->localInvalidateRegion();

    char keybuf[100];
    for (int index = 0; index < entriesExpected; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(regPtr->get(keybuf));
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 1007, "unexpected size of value in verify");
    }
    LOG("On client Found all entries with correct size via "
        "get.");
  }
END_TASK(VerifyUpdatedManyPuts)

DUNIT_TASK(CLIENT2, VerifyUpdatedManyPutsGetAll)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    // region should already have n entries...
    int entriesExpected = dunit::globals()->getIntValue("entriesToExpect");
    // invalidate the region to force getting new values
    regPtr->localInvalidateRegion();

    VectorOfCacheableKey vec;
    char keybuf[100];
    for (int index = 0; index < entriesExpected; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      vec.push_back(CacheableKey::create(keybuf));
    }
    try {
      regPtr->getAll(vec, NULLPTR, NULLPTR, false);
      FAIL(
          "Expected IllegalArgumentException when nothing "
          "is being fetched");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }
    regPtr->getAll(vec, NULLPTR, NULLPTR, true);
    LOG("On client getAll for entries completed.");
    for (int index = 0; index < entriesExpected; ++index) {
      sprintf(keybuf, "keys1%010d", index);
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(regPtr->get(keybuf));
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 1007, "unexpected size of value in verify");
    }
    LOG("On client Found all entries with correct size via "
        "getAll.");
  }
END_TASK(VerifyUpdatedManyPutsGetAll)

DUNIT_TASK(CLIENT1, UpdateManyPutsInt64)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    LOG("Beginning updated many puts for int64.");
    int expectEntries = 0;
    char valbuf[1100];
    for (int64_t index = 0; index < MAX_PUTS; ++index) {
      int64_t key = index * index * index;
      sprintf(valbuf, "values3%0200" PRId64, index);
      // regPtr->put(key, valbuf);
      regPtr->put(CacheableInt64::create(key), valbuf);
      expectEntries++;
    }
    dunit::globals()->rebind("entriesToExpect", expectEntries);
    LOG("Finished putting int64 entries.");
  }
END_TASK(UpdateManyPutsInt64)

DUNIT_TASK(CLIENT2, VerifyManyPutsInt64)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    int entriesExpected = dunit::globals()->getIntValue("entriesToExpect");

    for (int64_t index = 0; index < entriesExpected; ++index) {
      int64_t key = index * index * index;
      // CacheableStringPtr valPtr =
      // dynCast<CacheableStringPtr>(regPtr->get(key));
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(regPtr->get(CacheableInt64::create(key)));
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 207, "unexpected size of value in verify");
    }
    LOG("On client Found all int64 entries with "
        "correct size via get.");
    for (int64_t index = 0; index < entriesExpected; ++index) {
      // CacheableKeyPtr key =
      // CacheableKey::create(CacheableInt64::create(index
      // * index * index));
      CacheableKeyPtr key = CacheableInt64::create(index * index * index);
      RegionEntryPtr entry = regPtr->getEntry(key);
      ASSERT(entry != NULLPTR, "expected non-null entry");
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(entry->getValue());
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 207, "unexpected size of value in verify");
    }
    LOG("On client Found all int64 entries with "
        "correct size via getEntry.");
  }
END_TASK(VerifyManyPutsInt64)

DUNIT_TASK(CLIENT2, VerifyUpdatedManyPutsInt64GetAll)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    // region should already have n entries...
    int entriesExpected = dunit::globals()->getIntValue("entriesToExpect");
    // invalidate the region to force getting new
    // values
    regPtr->localInvalidateRegion();

    VectorOfCacheableKey vec;
    for (int64_t index = 0; index < entriesExpected; ++index) {
      int64_t key = index * index * index;
      // vec.push_back(CacheableKey::create(key));
      vec.push_back(CacheableInt64::create(key));
    }
    try {
      regPtr->getAll(vec, NULLPTR, NULLPTR, false);
      FAIL(
          "Expected IllegalArgumentException when "
          "nothing is being fetched");
    } catch (const IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException");
    }
    HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
    regPtr->getAll(vec, valuesMap, NULLPTR, true);
    LOG("On client getAll for int64 entries "
        "completed.");
    for (int32_t index = 0; index < entriesExpected; ++index) {
      CacheableKeyPtr key = vec[index];
      RegionEntryPtr entry = regPtr->getEntry(key);
      ASSERT(entry != NULLPTR, "expected non-null entry");
      CacheableStringPtr valPtr =
          dynCast<CacheableStringPtr>(entry->getValue());
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 207, "unexpected size of value in verify");
      HashMapOfCacheable::Iterator iter = valuesMap->find(key);
      ASSERT(iter != valuesMap->end(), "expected to find key in map");
      valPtr = dynCast<CacheableStringPtr>(iter.second());
      ASSERT(valPtr != NULLPTR, "expected non-null value");
      ASSERT(valPtr->length() == 207, "unexpected size of value in verify");
    }
    LOG("On client Found all int64 entries with "
        "correct size via getAll.");
  }
END_TASK(VerifyUpdatedManyPutsInt64GetAll)

DUNIT_TASK(SERVER1, StopServer)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      CacheHelper::closeLocator(1);
    }
    LOG("SERVER stopped");
  }
END_TASK(StopServer)

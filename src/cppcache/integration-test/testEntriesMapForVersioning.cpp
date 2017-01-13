/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gf_base.hpp>

#include "fw_dunit.hpp"

#ifdef WIN32

#define CLIENT1 s1p1
DUNIT_TASK_DEFINITION(CLIENT1, CREATECLIENT)
  { LOG("This test doesn't run on windows. Too many classes to export."); }
END_TASK_DEFINITION
void runTask() { CALL_TASK(CREATECLIENT); }
DUNIT_MAIN
  { runTask(); }
END_MAIN

#else

//#define BUILD_CPPCACHE 1
#include <gfcpp/GemfireCppCache.hpp>
#include <LRUEntriesMap.hpp>
#include <LRUMapEntry.hpp>
#include <LRUExpMapEntry.hpp>
#include <VersionTag.hpp>
#include <stdlib.h>
#include <ClientProxyMembershipID.hpp>
#include <ace/OS.h>
#include <string>
#include <vector>
#include <LocalRegion.hpp>
#include <gfcpp/DataInput.hpp>
#include "DeltaEx.hpp"
#include "CacheableToken.hpp"
#include "DiskStoreId.hpp"
#include "DiskVersionTag.hpp"
#include "DSMemberForVersionStamp.hpp"
#include "CachePerfStats.hpp"
#define ROOT_SCOPE LOCAL

#include "CacheHelper.hpp"

#define CLIENT1 s1p1

using namespace gemfire;
using namespace test;

CacheHelper* cacheHelper = NULL;
RegionPtr regPtr;

const char* endpoints = NULL;

void initClient() {
  if (cacheHelper == NULL) {
    PropertiesPtr configPtr = Properties::create();
    configPtr->insert("tombstone-timeout", 5000);
    cacheHelper = new CacheHelper(true, configPtr);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}
void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false, bool caching = true) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  regPtr =
      getHelper()->createRegion(name, ackMode, caching, NULLPTR,
                                clientNotificationEnabled, true, true, 5000);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

typedef std::vector<MapEntryImplPtr> VectorOfMapEntry;

CacheableStringPtr createCacheable(const char* value) {
  CacheableStringPtr result = CacheableString::create(value);
  ASSERT(result != NULLPTR, "expected result non-NULL");
  return result;
}

uint8_t addr1[6] = {0xff, 0xff, 0xff, 0xaa, 0xff, 0xff};
uint8_t addr2[6] = {0xff, 0xff, 0xff, 0xaa, 0xff, 0xbb};
uint8_t addr3[6] = {0xff, 0xff, 0xaa, 0xaa, 0xff, 0xff};
uint8_t addr4[6] = {0xff, 0xff, 0xff, 0xff, 0xaa, 0xff};

ClientProxyMembershipIDPtr member_host1(
    new ClientProxyMembershipID(addr1, 6, 80, "", "myuniquetag", 0));
ClientProxyMembershipIDPtr member_host12(
    new ClientProxyMembershipID(addr1, 6, 80, "", "myuniquetah", 0));
ClientProxyMembershipIDPtr member_host13(
    new ClientProxyMembershipID(addr1, 6, 81, "", "myuniquetag", 0));
ClientProxyMembershipIDPtr member_host14(
    new ClientProxyMembershipID(addr1, 6, 88, "", "myuniquetag", 0));
ClientProxyMembershipIDPtr member_host15(
    new ClientProxyMembershipID(addr2, 6, 88, "", "myuniquetag", 0));
ClientProxyMembershipIDPtr member_host16(
    new ClientProxyMembershipID(addr3, 6, 88, "", "myuniquetag", 0));
ClientProxyMembershipIDPtr member_host17(
    new ClientProxyMembershipID(addr4, 6, 88, "", "myuniquetag", 0));
DiskStoreId* diskStore17 = new DiskStoreId(1, 7);
DiskStoreId* diskStore18 = new DiskStoreId(1, 8);
DiskStoreId* diskStore27 = new DiskStoreId(2, 7);
ClientProxyMembershipIDPtr member_host_vmview5(
    new ClientProxyMembershipID(addr4, 6, 88, "", "", 5));
ClientProxyMembershipIDPtr member_host_vmview6(
    new ClientProxyMembershipID(addr4, 6, 88, "", "", 6));
ClientProxyMembershipIDPtr member_host_vmview7(
    new ClientProxyMembershipID(addr4, 6, 88, "", "", 7));

uint16_t host1;
uint16_t host12;
uint16_t host13;
uint16_t host14;
uint16_t host15;
uint16_t host16;
uint16_t host17;
uint16_t disk17;
uint16_t disk18;
uint16_t disk27;
uint16_t hostVmview5;
uint16_t hostVmview6;
uint16_t hostVmview7;

int DeltaEx::toDeltaCount = 0;
int DeltaEx::toDataCount = 0;
int DeltaEx::fromDeltaCount = 0;
int DeltaEx::fromDataCount = 0;
int DeltaEx::cloneCount = 0;

DUNIT_TASK_DEFINITION(CLIENT1, CREATECLIENT)
  {
    initClient();
    createRegion("myregion", true, false);
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_AddHosts)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    host1 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host1);
    host12 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host12);
    host13 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host13);
    host14 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host14);
    host15 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host15);
    host16 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host16);
    host17 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host17);
    disk17 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        DSMemberForVersionStampPtr(diskStore17));
    disk18 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        DSMemberForVersionStampPtr(diskStore18));
    disk27 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        DSMemberForVersionStampPtr(diskStore27));
    hostVmview5 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host_vmview5);
    hostVmview6 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host_vmview6);
    hostVmview7 = lregPtr->getCacheImpl()->getMemberListForVersionStamp()->add(
        member_host_vmview7);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo_TestPut)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, true, lregPtr);
    entries->open();
    CacheableKeyPtr keyPtr = CacheableKey::create((char*)"key1");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    MapEntryImplPtr me;
    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    VersionTagPtr versionTag13(new VersionTag(5, 6, 7, host13, 0));
    VersionTagPtr versionTag14(new VersionTag(5, 6, 7, host14, 0));
    VersionTagPtr versionTag15(new VersionTag(5, 6, 7, host15, 0));
    VersionTagPtr versionTag16(new VersionTag(5, 6, 7, host16, 0));
    VersionTagPtr versionTag17(new VersionTag(5, 6, 7, host17, 0));

    CacheablePtr oldValue;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    char log[256];

    GfErrType err =
        entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag12);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag13);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    uint32_t conflatedEvents =
        lregPtr->getCacheImpl()->m_cacheStats->getConflatedEvents();
    ASSERT(conflatedEvents == 2, "conflated events should be 2");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag14);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag15);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag16);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag17);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    MapEntryImplPtr result;
    CacheablePtr value;
    entries->getEntry(keyPtr, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 7, "an exception");

    VersionTagPtr versionTag18(new VersionTag(0xffffaa, 6, 7, host1, 0));

    // version rollover, this will not be applied
    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag18);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 7, "an exception");

    CacheableKeyPtr keyPtr2 = CacheableKey::create((char*)"Key2");
    err = entries->put(keyPtr2, ct, me, oldValue, -1, 0, versionTag18);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 100, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 1, "an exception");

    // version rollover, this will be applied
    err = entries->put(keyPtr2, ct1, me, oldValue, -1, 0, versionTag12);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 2, "an exception");

    // Null version tag, this will be applied
    VersionTagPtr versionTag19;
    err = entries->put(keyPtr2, ct, me, oldValue, -1, 0, versionTag19);
    ASSERT(err != GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 100, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    // inserts a null tag
    CacheableKeyPtr keyPtr3 = CacheableKey::create((char*)"Key3");
    err = entries->put(keyPtr3, ct1, me, oldValue, -1, 0, versionTag19);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr3, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 0, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 0, "an exception");

    // inserts an entry with version stamp, the previous entry is without
    // version
    // stamp,
    // should be allowed.
    err = entries->put(keyPtr3, ct1, me, oldValue, -1, 0, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr3, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    try {
      Serializable::registerType(DeltaEx::create);
    } catch (IllegalStateException&) {
      //  ignore exception caused by type reregistration.
    }
    DeltaEx::toDeltaCount = 0;
    DeltaEx::toDataCount = 0;
    sprintf(log, "Some delta tests...");
    LOG(log);

    CacheableKeyPtr keyPtr4 = CacheableKey::create((char*)"Key4");
    DeltaEx* ptr = new DeltaEx();
    CacheablePtr valPtr(ptr);
    err = entries->put(keyPtr4, valPtr, me, oldValue, -1, 0, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr4, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");
    ASSERT(DeltaEx::fromDeltaCount == 0, " Delta count should have been 0 ");

    DeltaEx* ptr1 = new DeltaEx();
    ptr1->setDelta(true);
    CacheablePtr valPtr1(ptr1);
    DataOutput doutput;
    doutput.writeInt(1);
    const uint8_t* buffer = doutput.getBuffer();

    DataInput datainput(buffer, doutput.getBufferLength());

    bool isUpdate;
    VersionTagPtr versionTag12plus(new VersionTag(6, 6, 7, host13, host12));
    err = entries->put(keyPtr4, valPtr1, me, oldValue, -1, 0, versionTag12plus,
                       isUpdate, &datainput);

    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr4, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 3, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 6, "an exception");
    ASSERT(DeltaEx::fromDeltaCount == 1, " Delta count should have been 1 ");

    // Delta update, Not allowed as same tag and stamp versions
    err = entries->put(keyPtr4, valPtr1, me, oldValue, -1, 0, versionTag12plus,
                       isUpdate, &datainput);
    ASSERT(err == GF_INVALID_DELTA, "an exception");
    entries->getEntry(keyPtr4, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 3, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 6, "an exception");
    ASSERT(DeltaEx::fromDeltaCount == 1, " Delta count should have been 1 ");

    // Delta update, Not allowed as delta based on a different host version
    // different
    VersionTagPtr versionTag12pp(new VersionTag(7, 6, 7, host13, host12));
    err = entries->put(keyPtr4, valPtr1, me, oldValue, -1, 0, versionTag12plus,
                       isUpdate, &datainput);
    ASSERT(err == GF_INVALID_DELTA, "an exception");
    entries->getEntry(keyPtr4, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 3, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 6, "an exception");
    ASSERT(DeltaEx::fromDeltaCount == 1, " Delta count should have been 1 ");

    DeltaEx* ptr2 = new DeltaEx();
    ptr2->setDelta(true);
    CacheablePtr valPtr2(ptr2);
    DataOutput doutput1;
    doutput1.writeInt(1);
    const uint8_t* buffer1 = doutput1.getBuffer();
    DataInput datainput1(buffer1, doutput1.getBufferLength());
    DeltaEx::fromDeltaCount = 0;
    // Delta update,  allowed as delta based on correct host version different
    VersionTagPtr versionTag12pp1(new VersionTag(7, 6, 7, host14, host13));
    err = entries->put(keyPtr4, valPtr2, me, oldValue, -1, 0, versionTag12pp1,
                       isUpdate, &datainput1);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr4, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 4, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 7, "an exception");
    ASSERT(DeltaEx::fromDeltaCount == 1, " Delta count should have been 1 ");

    /******* Test disk version tags*****************/
    VersionTagPtr versiondiskTag17(new DiskVersionTag(5, 6, 7, disk17, 0));
    VersionTagPtr versiondiskTag18(new DiskVersionTag(5, 6, 7, disk18, 0));
    VersionTagPtr versiondiskTag27(new DiskVersionTag(5, 6, 7, disk27, 0));
    CacheableKeyPtr keydiskPtr = CacheableKey::create((char*)"keydisk1");
    err = entries->put(keydiskPtr, ct, me, oldValue, -1, 0, versiondiskTag17);
    err = entries->put(keydiskPtr, ct1, me, oldValue, -1, 0, versiondiskTag27);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keydiskPtr, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == disk27, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    err = entries->put(keydiskPtr, ct, me, oldValue, -1, 0, versiondiskTag18);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keydiskPtr, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == disk27, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    /********* Test with vm view ids ****************/
    VersionTagPtr versionvmviewTag5(new VersionTag(5, 6, 7, hostVmview5, 0));
    VersionTagPtr versionvmviewTag6(new VersionTag(5, 6, 7, hostVmview6, 0));
    VersionTagPtr versionvmviewTag7(new VersionTag(5, 6, 7, hostVmview7, 0));
    CacheableKeyPtr keyvmviewPtr = CacheableKey::create((char*)"keyvm1");
    err =
        entries->put(keyvmviewPtr, ct, me, oldValue, -1, 0, versionvmviewTag5);

    err =
        entries->put(keyvmviewPtr, ct1, me, oldValue, -1, 0, versionvmviewTag7);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyvmviewPtr, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == hostVmview7,
           "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    err =
        entries->put(keyvmviewPtr, ct, me, oldValue, -1, 0, versionvmviewTag6);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyvmviewPtr, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == hostVmview7,
           "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    sprintf(log, "Put test complete. %d", err);
    LOG(log);

    delete entries;
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepThree_TestCreate)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, true, lregPtr);
    entries->open();
    CacheableKeyPtr keyPtr4 = CacheableKey::create((char*)"key4");
    ASSERT(keyPtr4 != NULLPTR, "expected keyPtr non-NULL");
    MapEntryImplPtr me;
    MapEntryImplPtr result;
    CacheablePtr value;

    /*new VersionTag(int32_t entryVersion,
        int16_t regionVersionHighBytes, int32_t regionVersionLowBytes,
        uint16_t internalMemId, uint16_t previousMemId) */
    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    VersionTagPtr versionTag13(new VersionTag(5, 6, 7, host13, 0));
    VersionTagPtr versionTag14(new VersionTag(5, 6, 7, host14, 0));
    VersionTagPtr versionTag15(new VersionTag(5, 6, 7, host15, 0));
    VersionTagPtr versionTag16(new VersionTag(5, 6, 7, host16, 0));
    VersionTagPtr versionTag17(new VersionTag(5, 6, 7, host17, 0));

    CacheablePtr oldValue;
    entries->create(keyPtr4, ct, me, oldValue, -1, 0, versionTag1);
    entries->getEntry(keyPtr4, result, value);
    ASSERT(me->getVersionStamp().getEntryVersion() == 5, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 1, "an exception");

    char log[256];

    CacheableKeyPtr keyPtr = CacheableKey::create((char*)"Key");
    GfErrType err =
        entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag13);
    ASSERT(err == GF_NOERR, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag14);
    ASSERT(err == GF_NOERR, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag15);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->create(keyPtr, NULLPTR, me, oldValue, -1, 0, versionTag16);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->create(keyPtr, ct1, me, oldValue, -1, 0, versionTag17);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 7, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    VersionTagPtr versionTag18(new VersionTag(0xffffaa, 6, 7, host1, 0));

    CacheableKeyPtr keyPtr2 = CacheableKey::create((char*)"Key2");
    err = entries->create(keyPtr2, NULLPTR, me, oldValue, -1, 0, versionTag18);
    ASSERT(err == GF_NOERR, "an exception");

    // version rollover, this will be applied
    err = entries->create(keyPtr2, NULLPTR, me, oldValue, -1, 0, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");

    // Null version tag, this will be applied
    VersionTagPtr versionTag19;
    err = entries->create(keyPtr2, ct, me, oldValue, -1, 0, versionTag19);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    // inserts a null tag
    CacheableKeyPtr keyPtr3 = CacheableKey::create((char*)"Key3");
    err = entries->create(keyPtr3, NULLPTR, me, oldValue, -1, 0, versionTag19);
    ASSERT(err == GF_NOERR, "an exception");

    // inserts an entry with version stamp, the previous entry is without
    // version
    // stamp,
    // should be allowed.
    err = entries->create(keyPtr3, ct1, me, oldValue, -1, 0, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr3, result, value);
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    sprintf(log, "Create test complete. %d", err);
    LOG(log);

    delete entries;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight_TestLRUEntries)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = LRUExpEntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new LRUEntriesMap(entryFactory, lregPtr,
                                            LRUAction::DESTROY, 50000, true);
    entries->open();
    CacheableKeyPtr keyPtr4 = CacheableKey::create((char*)"key4");
    CacheableKeyPtr keyPtr5 = CacheableKey::create((char*)"key5");
    CacheableKeyPtr keyPtr6 = CacheableKey::create((char*)"key6");
    MapEntryImplPtr me;
    MapEntryImplPtr result;
    CacheablePtr value;
    char log[256];

    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    entries->put(keyPtr4, ct, me, value, -1, 0, versionTag1);

    GfErrType err =
        entries->remove(keyPtr4, value, me, -1, versionTag12, false);
    ASSERT(err == GF_NOERR, "an exception");
    bool isTombstone;
    err = entries->isTombstone(keyPtr4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    entries->put(keyPtr5, ct, me, value, -1, 0, versionTag1);

    err = entries->invalidate(keyPtr5, me, value, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr5, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");

    entries->put(keyPtr6, ct, me, value, -1, 0, versionTag1);

    ASSERT(entries->get(keyPtr4, value, result) == false, "an exception");
    ASSERT(entries->get(keyPtr6, value, result) == true, "an exception");

    sprintf(log, "LRUentriesMap test complete. %d", err);
    LOG(log);

    delete entries;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive_TestTombstoneExpiry)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, true, lregPtr);
    entries->open();
    CacheableKeyPtr keyPtr4 = CacheableKey::create((char*)"key4");
    CacheableKeyPtr keyPtr5 = CacheableKey::create((char*)"key5");
    CacheableKeyPtr keyPtr6 = CacheableKey::create((char*)"key6");
    MapEntryImplPtr me;
    MapEntryImplPtr result;
    CacheablePtr value;
    char log[256];

    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    entries->put(keyPtr4, ct, me, value, -1, 0, versionTag1);

    GfErrType err =
        entries->remove(keyPtr4, value, me, -1, versionTag12, false);
    ASSERT(err == GF_NOERR, "an exception");
    bool isTombstone;
    err = entries->isTombstone(keyPtr4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    entries->put(keyPtr5, ct, me, value, -1, 0, versionTag1);

    err = entries->remove(keyPtr5, value, me, -1, versionTag12, false);
    ASSERT(err == GF_NOERR, "an exception");
    err = entries->isTombstone(keyPtr5, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    entries->put(keyPtr6, ct, me, value, -1, 0, versionTag1);
    uint32_t tombstone_count =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneCount();
    uint64_t tombstone_size =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneSize();

    sprintf(log,
            "Before expiry, Tombstone size: %" PRId64 " Tombstone count: %d",
            tombstone_size, tombstone_count);
    LOG(log);
    ASSERT(tombstone_count > 0, "Tombstone count should be equal to 2");
    ASSERT(tombstone_size > 160,
           "Tombstone size should be greater than 160 bytes. 160 is a approx "
           "figure for tombstone overhead");

    ACE_OS::sleep(8);

    err = entries->isTombstone(keyPtr5, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtr5, value, result) == false, "an exception");
    err = entries->isTombstone(keyPtr4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtr4, value, result) == false, "an exception");

    uint32_t tombstone_count_after =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneCount();
    uint64_t tombstone_size_after =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneSize();

    sprintf(log,
            "After expiry, Tombstone size: %" PRId64 " Tombstone count: %d",
            tombstone_size_after, tombstone_count_after);
    LOG(log);

    ASSERT((tombstone_count - 2) == tombstone_count_after,
           "Tombstone count does not match");
    ASSERT((tombstone_size - 160) > tombstone_size_after,
           "Tombstone size does not match");

    sprintf(log, "Tombstone expiry test complete. %d", err);
    LOG(log);

    delete entries;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSix_TestInvalidate)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, true, lregPtr);
    entries->open();
    CacheableKeyPtr keyPtr = CacheableKey::create((char*)"key1");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    MapEntryImplPtr me;
    MapEntryImplPtr result;
    CacheablePtr value;

    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    VersionTagPtr versionTag13(new VersionTag(5, 6, 7, host13, 0));
    VersionTagPtr versionTag14(new VersionTag(5, 6, 7, host14, 0));
    VersionTagPtr versionTag15(new VersionTag(5, 6, 7, host15, 0));
    VersionTagPtr versionTag16(new VersionTag(5, 6, 7, host16, 0));
    VersionTagPtr versionTag17(new VersionTag(5, 6, 7, host17, 0));
    VersionTagPtr versionTag22(new VersionTag(9, 10, 10, host12, 0));

    CacheablePtr oldValue;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    char log[256];

    GfErrType err = entries->invalidate(keyPtr, me, oldValue, versionTag12);

    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag13);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) != true, "an exception");

    err = entries->invalidate(keyPtr, me, oldValue, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) != true, "an exception");

    err = entries->invalidate(keyPtr, me, oldValue, versionTag14);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");

    err = entries->invalidate(keyPtr, me, oldValue, versionTag17);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 7, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");

    err = entries->invalidate(keyPtr, me, oldValue, versionTag22);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");

    VersionTagPtr versionTag18(new VersionTag(0xffffaa, 6, 7, host1, 0));

    // version rollover, this will not be applied
    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag18);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");

    CacheableKeyPtr keyPtr2 = CacheableKey::create((char*)"Key2");
    err = entries->put(keyPtr2, ct, me, oldValue, -1, 0, versionTag18);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 100, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 1, "an exception");

    // version rollover, this will be applied
    err = entries->invalidate(keyPtr2, me, oldValue, versionTag22);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");

    // Null version tag, this will be applied
    VersionTagPtr versionTag19;
    err = entries->put(keyPtr2, ct, me, oldValue, -1, 0, versionTag19);
    entries->getEntry(keyPtr2, result, value);
    ASSERT(CacheableToken::isInvalid(value) != true, "an exception");
    ASSERT(atoi(value->toString()->asChar()) == 100, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");

    // inserts a null tag
    CacheableKeyPtr keyPtr3 = CacheableKey::create((char*)"Key3");
    err = entries->put(keyPtr3, ct1, me, oldValue, -1, 0, versionTag19);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr3, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 0, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 0, "an exception");

    // removes an entry with version tag, the previous entry is without version
    // stamp,
    // should be allowed.
    err = entries->invalidate(keyPtr3, me, oldValue, versionTag12);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr3, result, value);
    ASSERT(CacheableToken::isInvalid(value) == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");
    delete entries;
    sprintf(log, "Invalidate test complete. %d", err);
    LOG(log);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven_TestGetsAfterRemove)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, true, lregPtr);
    entries->open();
    CacheableKeyPtr keyPtr4 = CacheableKey::create((char*)"key4");
    CacheableKeyPtr keyPtr5 = CacheableKey::create((char*)"key5");
    CacheableKeyPtr keyPtr6 = CacheableKey::create((char*)"key6");
    MapEntryImplPtr me;
    MapEntryImplPtr result;
    CacheablePtr value;
    char log[256];

    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    VersionTagPtr versionTag22(new VersionTag(6, 6, 7, host12, 0));

    entries->put(keyPtr4, ct, me, value, -1, 0, versionTag1);

    GfErrType err =
        entries->remove(keyPtr4, value, me, -1, versionTag12, false);
    ASSERT(err == GF_NOERR, "an exception");
    bool isTombstone;
    err = entries->isTombstone(keyPtr4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    entries->put(keyPtr5, ct, me, value, -1, 0, versionTag1);

    err = entries->remove(keyPtr5, value, me, -1, versionTag12, false);
    ASSERT(err == GF_NOERR, "an exception");
    err = entries->isTombstone(keyPtr5, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    entries->put(keyPtr6, ct, me, value, -1, 0, versionTag1);

    ASSERT(entries->containsKey(keyPtr6) == true, "an exception");
    ASSERT(entries->containsKey(keyPtr5) == false, "an exception");
    ASSERT(entries->containsKey(keyPtr4) == false, "an exception");
    ASSERT(entries->get(keyPtr4, value, result) == false, "an exception");
    ASSERT(entries->get(keyPtr5, value, result) == false, "an exception");
    ASSERT(entries->get(keyPtr6, value, result) == true, "an exception");

    VectorOfCacheable values;
    entries->values(values);
    ASSERT(values.length() == 1, "an exception");

    VectorOfCacheableKey keys;
    entries->keys(keys);
    ASSERT(keys.length() == 1, "an exception");

    VectorOfRegionEntry regionEntries;
    entries->entries(regionEntries);
    ASSERT(regionEntries.length() == 1, "an exception");

    entries->put(keyPtr5, ct, me, value, -1, 0, versionTag22);

    ASSERT(entries->containsKey(keyPtr6) == true, "an exception");
    ASSERT(entries->containsKey(keyPtr5) == true, "an exception");
    ASSERT(entries->containsKey(keyPtr4) == false, "an exception");
    ASSERT(entries->get(keyPtr4, value, result) == false, "an exception");
    ASSERT(entries->get(keyPtr5, value, result) == true, "an exception");
    ASSERT(entries->get(keyPtr6, value, result) == true, "an exception");

    entries->values(values);
    ASSERT(values.length() == 2, "an exception");

    entries->keys(keys);
    ASSERT(keys.length() == 2, "an exception");

    entries->entries(regionEntries);
    ASSERT(regionEntries.length() == 2, "an exception");

    sprintf(log, "TestGetsAfterRemove test complete. %d", err);
    LOG(log);

    delete entries;
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepFour_TestRemove)
  {
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr.ptr()));
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    CacheableStringPtr ccstr1 = createCacheable("500");
    CacheablePtr ct1 = ccstr1;
    EntryFactory* entryFactory = EntryFactory::singleton;
    entryFactory->setConcurrencyChecksEnabled(true);
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, true, lregPtr);
    entries->open();
    CacheableKeyPtr keyPtr = CacheableKey::create((char*)"key1");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    MapEntryImplPtr me;
    MapEntryImplPtr result;
    CacheablePtr value;

    VersionTagPtr versionTag1(new VersionTag(5, 6, 7, host1, 0));

    VersionTagPtr versionTag12(new VersionTag(5, 6, 7, host12, 0));

    VersionTagPtr versionTag13(new VersionTag(5, 6, 7, host13, 0));
    VersionTagPtr versionTag14(new VersionTag(5, 6, 7, host14, 0));
    VersionTagPtr versionTag15(new VersionTag(5, 6, 7, host15, 0));
    VersionTagPtr versionTag16(new VersionTag(5, 6, 7, host16, 0));
    VersionTagPtr versionTag17(new VersionTag(5, 6, 7, host17, 0));
    VersionTagPtr versionTag22(new VersionTag(9, 10, 10, host12, 0));

    CacheablePtr oldValue;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    char log[256];

    GfErrType err =
        entries->remove(keyPtr, oldValue, me, -1, versionTag12, false);

    ASSERT(err == GF_NOERR, "an exception");
    bool isTombstone;
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr, result, value);
    ASSERT(isTombstone == true, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    uint32_t tombstone_count =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneCount();
    uint64_t tombstone_size =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneSize();

    sprintf(log, "After tombstone creation, Tombstone size: %" PRId64
                 " Tombstone count: %d",
            tombstone_size, tombstone_count);
    LOG(log);
    ASSERT(tombstone_count == 1, "Tombstone count should be equal to 1");
    ASSERT(tombstone_size > 70,
           "Tombstone size should be greater than 70 bytes. 70 is an approx "
           "figure for tombstone overhead");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag13);
    ASSERT(err == GF_NOERR, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");

    tombstone_count =
        lregPtr->getCacheImpl()->m_cacheStats->getTombstoneCount();
    tombstone_size = lregPtr->getCacheImpl()->m_cacheStats->getTombstoneSize();

    sprintf(log,
            "After converting tombstone into an entry, Tombstone size: %" PRId64
            " Tombstone count: %d",
            tombstone_size, tombstone_count);
    LOG(log);
    ASSERT(tombstone_count == 0, "Tombstone count should be equal to 0");
    ASSERT(tombstone_size == 0, "Tombstone size should be 0");

    err = entries->remove(keyPtr, oldValue, me, -1, versionTag1, false);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtr, value, result) == true, "an exception");

    err = entries->remove(keyPtr, oldValue, me, -1, versionTag14, false);
    ASSERT(err == GF_NOERR, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 4, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");
    ASSERT(entries->get(keyPtr, value, result) == false, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct1, me, oldValue, -1, 0, versionTag15);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag1);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->remove(keyPtr, oldValue, me, -1, versionTag16, false);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");

    err = entries->remove(keyPtr, oldValue, me, -1, versionTag17, false);
    ASSERT(err == GF_CACHE_ENTRY_NOT_FOUND, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 7, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");
    ASSERT(entries->get(keyPtr, value, result) == false, "an exception");

    err = entries->remove(keyPtr, oldValue, me, -1, versionTag22, false);
    ASSERT(err == GF_CACHE_ENTRY_NOT_FOUND, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");
    ASSERT(entries->get(keyPtr, value, result) == false, "an exception");

    VersionTagPtr versionTag18(new VersionTag(0xffffaa, 6, 7, host1, 0));

    // version rollover, this will not be applied
    err = entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag18);
    ASSERT(err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION, "an exception");
    err = entries->isTombstone(keyPtr, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");
    ASSERT(entries->get(keyPtr, value, result) == false, "an exception");

    CacheableKeyPtr keyPtr2 = CacheableKey::create((char*)"Key2");
    err = entries->put(keyPtr2, ct, me, oldValue, -1, 0, versionTag18);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 100, "an exception");
    ASSERT(me->getVersionStamp().getMemberId() == 1, "an exception");

    // version rollover, this will be applied
    err = entries->remove(keyPtr2, oldValue, me, -1, versionTag22, false);
    ASSERT(err == GF_NOERR, "an exception");
    err = entries->isTombstone(keyPtr2, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");
    ASSERT(entries->get(keyPtr2, value, result) == false, "an exception");

    // Null version tag, this will be applied
    VersionTagPtr versionTag19;
    err = entries->put(keyPtr2, ct, me, oldValue, -1, 0, versionTag19);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr2, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 100, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 9, "an exception");

    // inserts a null tag
    CacheableKeyPtr keyPtr3 = CacheableKey::create((char*)"Key3");
    err = entries->put(keyPtr3, ct1, me, oldValue, -1, 0, versionTag19);
    ASSERT(err == GF_NOERR, "an exception");
    entries->getEntry(keyPtr3, result, value);
    ASSERT(atoi(value->toString()->asChar()) == 500, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 0, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 0, "an exception");

    // removes an entry with version tag, the previous entry is without version
    // stamp,
    // should be allowed.
    err = entries->remove(keyPtr3, oldValue, me, 0, versionTag12, false);
    ASSERT(err == GF_NOERR, "an exception");
    err = entries->isTombstone(keyPtr3, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(result->getVersionStamp().getMemberId() == 2, "an exception");
    ASSERT(result->getVersionStamp().getEntryVersion() == 5, "an exception");
    ASSERT(entries->get(keyPtr3, value, result) == false, "an exception");

    CacheableKeyPtr keyPtrR2 = CacheableKey::create((char*)"keyPtrR2");
    CacheableKeyPtr keyPtrR3 = CacheableKey::create((char*)"keyPtrR3");
    CacheableKeyPtr keyPtrR4 = CacheableKey::create((char*)"keyPtrR4");
    CacheableKeyPtr keyPtrR5 = CacheableKey::create((char*)"keyPtrR5");
    CacheableKeyPtr keyPtrR6 = CacheableKey::create((char*)"keyPtrR6");

    CacheableKeyPtr keyPtrR21 = CacheableKey::create((char*)"keyPtrR21");
    CacheableKeyPtr keyPtrR31 = CacheableKey::create((char*)"keyPtrR31");
    CacheableKeyPtr keyPtrR41 = CacheableKey::create((char*)"keyPtrR41");
    CacheableKeyPtr keyPtrR51 = CacheableKey::create((char*)"keyPtrR51");
    CacheableKeyPtr keyPtrR61 = CacheableKey::create((char*)"keyPtrR61");

    VersionTagPtr versionTag23(new VersionTag(9, 10, 10, host13, 0));
    VersionTagPtr versionTag24(new VersionTag(9, 10, 10, host14, 0));
    VersionTagPtr versionTag25(new VersionTag(9, 10, 10, host15, 0));
    VersionTagPtr versionTag26(new VersionTag(9, 10, 10, host16, 0));

    sprintf(log, "Test reaping of tombstones");
    LOG(log);

    // add few entries with null version tags
    err = entries->put(keyPtrR2, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR3, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR4, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR5, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR21, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR31, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR41, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR51, ct1, me, oldValue, -1, 0, versionTag19);
    err = entries->put(keyPtrR61, ct1, me, oldValue, -1, 0, versionTag19);

    // remove those entries using non null version tags
    err = entries->remove(keyPtrR2, oldValue, me, -1, versionTag12, false);
    err = entries->remove(keyPtrR3, oldValue, me, -1, versionTag13, false);
    err = entries->remove(keyPtrR4, oldValue, me, -1, versionTag14, false);
    err = entries->remove(keyPtrR5, oldValue, me, -1, versionTag15, false);
    err = entries->remove(keyPtrR21, oldValue, me, -1, versionTag22, false);
    err = entries->remove(keyPtrR31, oldValue, me, -1, versionTag23, false);
    err = entries->remove(keyPtrR41, oldValue, me, -1, versionTag24, false);
    err = entries->remove(keyPtrR51, oldValue, me, -1, versionTag25, false);
    err = entries->remove(keyPtrR61, oldValue, me, -1, versionTag26, false);

    // test if all of them have been converted into tombstones.
    err = entries->isTombstone(keyPtrR2, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR3, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR5, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR21, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR31, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR41, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR51, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");

    // generate dummy gc versions
    std::map<uint16_t, int64_t> gcVersions;
    int64_t temp = 3;
    temp = temp << 32;
    gcVersions[1] = temp;
    gcVersions[2] = temp;
    temp = 11;
    temp = temp << 32;
    gcVersions[3] = temp;
    temp = 9;
    temp = temp << 32;
    gcVersions[4] = temp;
    temp = 10;
    temp = temp << 32;
    gcVersions[5] = temp;

    // reap entries based on gc versions
    entries->reapTombstones(gcVersions);

    // make sure entries are reaped
    err = entries->isTombstone(keyPtrR2, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(entries->get(keyPtrR2, value, result) == false, "an exception");
    err = entries->isTombstone(keyPtrR3, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtrR3, value, result) == false, "an exception");
    err = entries->isTombstone(keyPtrR4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtrR4, value, result) == false, "an exception");

    err = entries->isTombstone(keyPtrR5, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtrR5, value, result) == false, "an exception");

    err = entries->isTombstone(keyPtrR21, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(entries->get(keyPtrR21, value, result) == false, "an exception");

    err = entries->isTombstone(keyPtrR31, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    ASSERT(entries->get(keyPtrR31, value, result) == false, "an exception");

    err = entries->isTombstone(keyPtrR41, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(entries->get(keyPtrR41, value, result) == false, "an exception");

    err = entries->isTombstone(keyPtrR51, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    ASSERT(entries->get(keyPtrR51, value, result) == false, "an exception");

    sprintf(log, "Remove test complete. %d", err);
    LOG(log);
    // reap using removedKeys API
    CacheableKeyPtr keyPtrR71 = CacheableKey::create((char*)"keyPtrR71");
    CacheableHashSetPtr removedKeys = CacheableHashSet::create();
    removedKeys->insert(keyPtrR3);
    removedKeys->insert(keyPtrR71);
    removedKeys->insert(keyPtrR2);
    removedKeys->insert(keyPtrR41);

    // reap entries based keys
    entries->reapTombstones(removedKeys);

    // make sure entries are reaped
    err = entries->isTombstone(keyPtrR2, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    err = entries->isTombstone(keyPtrR3, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    err = entries->isTombstone(keyPtrR4, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    err = entries->isTombstone(keyPtrR5, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    err = entries->isTombstone(keyPtrR21, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    err = entries->isTombstone(keyPtrR31, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    err = entries->isTombstone(keyPtrR41, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == false, "an exception");
    err = entries->isTombstone(keyPtrR51, result, isTombstone);
    ASSERT(err == GF_NOERR, "an exception");
    ASSERT(isTombstone == true, "an exception");
    delete entries;
  }
END_TASK_DEFINITION
void runTask() {
  CALL_TASK(CREATECLIENT);
  CALL_TASK(StepOne_AddHosts);
  CALL_TASK(StepTwo_TestPut);
  CALL_TASK(StepThree_TestCreate);
  CALL_TASK(StepFour_TestRemove);
  CALL_TASK(StepFive_TestTombstoneExpiry);
  CALL_TASK(StepSix_TestInvalidate);
  CALL_TASK(StepSeven_TestGetsAfterRemove);
  CALL_TASK(StepEight_TestLRUEntries);
  CALL_TASK(CloseCache1);
}

DUNIT_MAIN
  { runTask(); }
END_MAIN

#endif

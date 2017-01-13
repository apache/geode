/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testEntriesMap"

#include "fw_helper.hpp"

#ifdef WIN32

BEGIN_TEST(NotOnWindows)
  { LOG("Too many non-external symbols used too fix right now on windows."); }
END_TEST(NotOnWindows)

#else

//#define BUILD_CPPCACHE 1
#include <gfcpp/GemfireCppCache.hpp>
#include <LRUEntriesMap.hpp>
#include <LRUMapEntry.hpp>
#include <VersionTag.hpp>
#include <stdlib.h>

using namespace gemfire;
using namespace std;

typedef std::vector<MapEntryImplPtr> VectorOfMapEntry;

CacheableStringPtr createCacheable(const char* value) {
  CacheableStringPtr result = CacheableString::create(value);
  ASSERT(result != NULLPTR, "expected result non-NULL");
  return result;
}

BEGIN_TEST(PutAndGet)
  {
    CacheableStringPtr ccstr = createCacheable("100");
    CacheablePtr ct = ccstr;
    EntryFactory* entryFactory = EntryFactory::singleton;
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, false);
    entries->open();
    CacheableKeyPtr keyPtr = CacheableKey::create((char*)"foobar");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    MapEntryImplPtr me;
    VersionTagPtr versionTag;
    CacheablePtr oldValue;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    CacheablePtr myValuePtr;
    entries->get(keyPtr, myValuePtr, me);
    ASSERT(myValuePtr != NULLPTR, "expected non-NULL");
    CacheableStringPtr strValue = dynCast<CacheableStringPtr>(myValuePtr);
    ASSERT(ccstr->operator==(*strValue), "expected 100");
    delete entries;
  }
END_TEST(PutAndGet)

BEGIN_TEST(CheckMapEntryImplPtr)
  {
    char error[1000] ATTR_UNUSED;
    MapEntryImplPtr mePtr;
    ASSERT(mePtr == NULLPTR, "expected mePtr to be NULL");
    CacheableKeyPtr keyPtr = CacheableKey::create(fwtest_Name);
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    EntryFactory::singleton->newMapEntry(keyPtr, mePtr);
    ASSERT(mePtr != NULLPTR, "expected to not be null.");
  }
END_TEST(CheckMapEntryImplPtr)

BEGIN_TEST(RemoveTest)
  {
    CacheableStringPtr cst = createCacheable("200");
    CacheablePtr ct = cst;
    EntryFactory* entryFactory = EntryFactory::singleton;
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, false);
    entries->open();
    CacheableKeyPtr keyPtr = CacheableKey::create(fwtest_Name);
    MapEntryImplPtr me;
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    CacheablePtr oldValue;
    VersionTagPtr versionTag;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    CacheablePtr myValuePtr;
    (void)entries->remove(keyPtr, myValuePtr, me, -1, versionTag, false);
    CacheableStringPtr resPtr = dynCast<CacheableStringPtr>(myValuePtr);
    ASSERT(myValuePtr != NULLPTR, "expected to not be null.");
    ASSERT(resPtr->operator==(*createCacheable("200")),
           "CustomerType with m_foobar 200.");
    (void)entries->remove(keyPtr, myValuePtr, me, -1, versionTag, false);
    ASSERT(myValuePtr == NULLPTR,
           "expected already removed, and null result should clear ptr.");
  }
END_TEST(RemoveTest)

BEGIN_TEST(GetEntryTest)
  {
    CacheableStringPtr cst = createCacheable("200");
    CacheablePtr ct = cst;
    EntryFactory* entryFactory = EntryFactory::singleton;
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, false);
    entries->open();
    CacheableKeyPtr keyPtr;
    MapEntryImplPtr me;
    keyPtr = CacheableKey::create(fwtest_Name);
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    CacheablePtr oldValue;
    VersionTagPtr versionTag;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    MapEntryImplPtr mePtr;
    CacheablePtr ctPtr;
    entries->getEntry(keyPtr, mePtr, ctPtr);
    ASSERT(mePtr != NULLPTR, "should not be null.");
    CacheableStringPtr valPtr = dynCast<CacheableStringPtr>(ctPtr);
    ASSERT(valPtr->operator==(*cst),
           "Entry should have a CustomerType Value of 200");
    CacheableKeyPtr keyPtr1;
    mePtr->getKey(keyPtr1);
    ASSERT(keyPtr1->operator==(*keyPtr), "should have same key.");
  }
END_TEST(GetEntryTest)

BEGIN_TEST(MapEntryImplPtrRCTest)
  {
    // Test Reference Counting and destruction for MapEntry.
    CacheableKeyPtr keyPtr = CacheableKey::create("foobar");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    MapEntryImplPtr mePtr;
    EntryFactory ef;
    ef.newMapEntry(keyPtr, mePtr);
    CacheablePtr ct = createCacheable("someval");
    mePtr->setValue(ct);
  }
END_TEST(MapEntryImplPtrRCTest)

BEGIN_TEST(VectorOfMapEntryTestA)
  {
    VectorOfMapEntry* meVec = new VectorOfMapEntry();
    delete meVec;
  }
END_TEST(VectorOfMapEntryTestA)

BEGIN_TEST(VectorOfMapEntryTestB)
  {
    VectorOfMapEntry* meVec = new VectorOfMapEntry();
    meVec->resize(100);
    meVec->clear();
    meVec->resize(10);
    MapEntryImplPtr mePtr;
    for (int i = 0; i < 10; i++) {
      meVec->push_back(mePtr);
    }
    for (int j = 0; j < 10; j++) {
      meVec->pop_back();
    }
    delete meVec;
  }
END_TEST(VectorOfMapEntryTestB)

BEGIN_TEST(EntriesTest)
  {
    EntryFactory* entryFactory = EntryFactory::singleton;
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, false);
    entries->open();
    char keyBuf[100];
    char valBuf[100];
    VersionTagPtr versionTag;
    MapEntryImplPtr me;
    for (int i = 0; i < 10; i++) {
      sprintf(keyBuf, "key_%d", i);
      sprintf(valBuf, "%d", i);
      CacheableKeyPtr keyPtr = CacheableKey::create(keyBuf);
      ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
      CacheablePtr v = createCacheable(valBuf);
      CacheablePtr oldValue;
      entries->put(keyPtr, v, me, oldValue, -1, 0, versionTag);
    }
    VectorOfRegionEntry* entriesVec = new VectorOfRegionEntry();
    entriesVec->resize(1);
    entries->entries(*entriesVec);
    // should be 10, but they are hashed so, we don't know what order they will
    // come in...
    int total = 0;
    int expectedTotal = 0;
    for (int k = 0; k < 10; k++) {
      expectedTotal += k;
      RegionEntryPtr rePtr = entriesVec->back();
      CacheableStringPtr ctPtr;
      CacheablePtr ccPtr;
      ccPtr = rePtr->getValue();
      ctPtr = dynCast<CacheableStringPtr>(ccPtr);
      test::cout << "value is " << ctPtr->asChar() << test::endl;
      int val = atoi(ctPtr->asChar());
      test::cout << "atoi returned " << val << test::endl;
      total += val;
      entriesVec->pop_back();
    }
    entriesVec->clear();
    entriesVec->resize(0);
    delete entriesVec;
    sprintf(keyBuf, "total = %d, expected = %d", total, expectedTotal);
    ASSERT(total == expectedTotal, keyBuf);
    delete entries;
  }
END_TEST(EntriesTest)

BEGIN_TEST(ValuesTest)
  {
    EntryFactory* entryFactory = EntryFactory::singleton;
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, false);
    entries->open();
    char keyBuf[100];
    char valBuf[100];
    VersionTagPtr versionTag;
    MapEntryImplPtr me;
    for (int i = 0; i < 10; i++) {
      sprintf(keyBuf, "key_%d", i);
      sprintf(valBuf, "%d", i);
      CacheableKeyPtr keyPtr = CacheableKey::create(keyBuf);
      ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
      CacheablePtr v = createCacheable(valBuf);
      CacheablePtr oldValue;
      entries->put(keyPtr, v, me, oldValue, -1, 0, versionTag);
    }
    VectorOfCacheable* valuesVec = new VectorOfCacheable();
    valuesVec->resize(1);
    entries->values(*valuesVec);
    // should be 10, but they are hashed so, we don't know what order they will
    // come in...
    int total = 0;
    int expectedTotal = 0;
    for (int k = 0; k < 10; k++) {
      expectedTotal += k;
      CacheableStringPtr valuePtr =
          dynCast<CacheableStringPtr>(valuesVec->back());
      total += atoi(valuePtr->asChar());
      valuesVec->pop_back();
    }
    delete valuesVec;
    sprintf(keyBuf, "total = %d, expected = %d", total, expectedTotal);
    ASSERT(total == expectedTotal, keyBuf);
    delete entries;
  }
END_TEST(ValuesTest)

BEGIN_TEST(KeysTest)
  {
    EntryFactory* entryFactory = EntryFactory::singleton;
    EntriesMap* entries = new ConcurrentEntriesMap(entryFactory, false);
    entries->open();
    char keyBuf[100];
    char valBuf[100];
    VersionTagPtr versionTag;
    MapEntryImplPtr me;
    for (int i = 0; i < 10; i++) {
      sprintf(keyBuf, "key_%d", i);
      sprintf(valBuf, "%d", i);
      CacheableKeyPtr keyPtr = CacheableKey::create(keyBuf);
      ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
      CacheablePtr v = createCacheable(valBuf);
      CacheablePtr oldValue;
      entries->put(keyPtr, v, me, oldValue, -1, 0, versionTag);
    }
    VectorOfCacheableKey keysVec;
    // keysVec.resize( 1 );
    entries->keys(keysVec);
    // should be 10, but they are hashed so, we don't know what order they will
    // come in...
    int total = 0;
    int expectedTotal = 0;
    for (int k = 0; k < 10; k++) {
      expectedTotal += k;
      CacheableKeyPtr keyPtr = keysVec.back();
      CacheablePtr cvPtr;
      entries->get(keyPtr, cvPtr, me);
      CacheableStringPtr valuePtr = dynCast<CacheableStringPtr>(cvPtr);
      total += atoi(valuePtr->asChar());
      keysVec.pop_back();
    }
    sprintf(keyBuf, "total = %d, expected = %d", total, expectedTotal);
    ASSERT(total == expectedTotal, keyBuf);
    delete entries;
  }
END_TEST(KeysTest)

BEGIN_TEST(TestRehash)
  {
    EntryFactory* entryFactory = EntryFactory::singleton;
    ConcurrentEntriesMap* entries =
        new ConcurrentEntriesMap(entryFactory, false, NULL, 1);
    entries->open(10);
    ASSERT(entries->totalSegmentRehashes() == 0,
           "should not have rehashed yet.");
    char keyBuf[100];
    char valBuf[100];
    MapEntryImplPtr me;

    for (uint32_t i = 0; i < 10000; i++) {
      sprintf(keyBuf, "key_%d", i);
      sprintf(valBuf, "%d", i);
      CacheableKeyPtr keyPtr = CacheableKey::create(keyBuf);
      ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
      CacheablePtr v = createCacheable(valBuf);
      CacheablePtr oldValue;
      VersionTagPtr versionTag;
      entries->put(keyPtr, v, me, oldValue, -1, 0, versionTag);
    }
    // check rehash count...
    ASSERT(entries->totalSegmentRehashes() > 0,
           "should have rehashed several times.");
    // VectorOfMapEntry result ;
    // entries->entries( result );
    //  printf("entries->size()=%d\n", entries->size());
    ASSERT(entries->size() == 10000, "should be 10k items");
    for (uint32_t j = 0; j < 10000; j++) {
      sprintf(keyBuf, "key_%d", j);
      CacheableStringPtr valuePtr;
      CacheableKeyPtr keyPtr = CacheableKey::create(keyBuf);
      ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
      CacheablePtr cvPtr;
      entries->get(keyPtr, cvPtr, me);
      valuePtr = dynCast<CacheableStringPtr>(cvPtr);
      if (valuePtr == NULLPTR) {
        test::cout << "error finding key: " << keyBuf << test::endl;
        FAIL("should have found value for all keys after rehash.");
      }
    }
  }
END_TEST(TestRehash)

//---- LRU variants

BEGIN_TEST(LRUPutAndGet)
  {
    CacheableStringPtr cst = createCacheable("100");
    CacheablePtr ct = cst;
    MapEntryImplPtr me;
    EntryFactory* entryFactory = LRUEntryFactory::singleton;
    EntriesMap* entries = new LRUEntriesMap(
        entryFactory, NULL, LRUAction::LOCAL_DESTROY, 20, false);
    entries->open();
    ASSERT(entries->size() == 0, "expected size 0.");
    CacheableKeyPtr keyPtr = CacheableKey::create("foobar");
    CacheablePtr oldValue;
    VersionTagPtr versionTag;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 1, "expected size 1.");
    CacheableStringPtr myValuePtr;
    CacheablePtr cvPtr;
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    entries->get(keyPtr, cvPtr, me);
    myValuePtr = dynCast<CacheableStringPtr>(cvPtr);
    ASSERT(myValuePtr != NULLPTR, "expected non-NULL");
    ASSERT(cst->operator==(*myValuePtr), "expected 100");
    delete entries;
  }
END_TEST(LRUPutAndGet)

BEGIN_TEST(CheckLRUMapEntryImplPtr)
  {
    char error[1000] ATTR_UNUSED;
    MapEntryImplPtr mePtr;
    ASSERT(mePtr == NULLPTR, "expected mePtr to be NULL");
    CacheableKeyPtr keyPtr = CacheableKey::create(fwtest_Name);
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    LRUEntryFactory::singleton->newMapEntry(keyPtr, mePtr);
    ASSERT(mePtr != NULLPTR, "expected to not be null.");
    LRUMapEntryPtr lmePtr = dynCast<LRUMapEntryPtr>(mePtr);
    ASSERT(lmePtr != NULLPTR, "expected to cast successfully to LRUMapEntry.");
  }
END_TEST(LRUCheckMapEntryImplPtr)

BEGIN_TEST(LRURemoveTest)
  {
    CacheableStringPtr cst = createCacheable("200");
    CacheablePtr ct = cst;
    EntryFactory* entryFactory = LRUEntryFactory::singleton;
    EntriesMap* entries = new LRUEntriesMap(
        entryFactory, NULL, LRUAction::LOCAL_DESTROY, 20, false);
    entries->open();
    ASSERT(entries->size() == 0, "expected size 0.");
    CacheableKeyPtr keyPtr;
    MapEntryImplPtr me;
    keyPtr = CacheableKey::create(fwtest_Name);
    CacheablePtr oldValue;
    VersionTagPtr versionTag;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 1, "expected size 1.");
    CacheableStringPtr myValuePtr;
    CacheablePtr cvPtr;
    (void)entries->remove(keyPtr, cvPtr, me, -1, versionTag, false);
    myValuePtr = dynCast<CacheableStringPtr>(cvPtr);
    ASSERT(entries->size() == 0, "expected size 0.");
    ASSERT(cvPtr != NULLPTR, "expected to not be null.");
    ASSERT(myValuePtr->operator==(*createCacheable("200")),
           "CustomerType with m_foobar 200.");

    (void)entries->remove(keyPtr, cvPtr, me, -1, versionTag, false);
    ASSERT(cvPtr == NULLPTR,
           "expected already removed, and null result should clear ptr.");
  }
END_TEST(LRURemoveTest)

BEGIN_TEST(LRUGetEntryTest)
  {
    CacheableStringPtr cst = createCacheable("200");
    CacheablePtr ct = cst;
    EntryFactory* entryFactory = LRUEntryFactory::singleton;
    EntriesMap* entries = new LRUEntriesMap(
        entryFactory, NULL, LRUAction::LOCAL_DESTROY, 20, false);
    entries->open();
    CacheableKeyPtr keyPtr;
    MapEntryImplPtr me;
    keyPtr = CacheableKey::create(fwtest_Name);
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    CacheablePtr oldValue;
    VersionTagPtr versionTag;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 1, "expected size 1.");
    MapEntryImplPtr mePtr;
    CacheablePtr cvPtr;
    entries->getEntry(keyPtr, mePtr, cvPtr);
    ASSERT(mePtr != NULLPTR, "should not be null.");
    CacheableStringPtr ctPtr;
    ctPtr = dynCast<CacheableStringPtr>(cvPtr);
    ASSERT(ctPtr->operator==(*cst),
           "Entry should have a CustomerType Value of 200");
    CacheableKeyPtr keyPtr1;
    mePtr->getKey(keyPtr1);
    ASSERT(keyPtr1->operator==(*keyPtr), "should have same key.");
  }
END_TEST(LRUGetEntryTest)

BEGIN_TEST(LRULimitEvictTest)
  {
    EntryFactory* entryFactory = LRUEntryFactory::singleton;
    EntriesMap* entries = new LRUEntriesMap(entryFactory, NULL,
                                            LRUAction::LOCAL_DESTROY, 5, false);
    entries->open();
    MapEntryImplPtr me;
    CacheablePtr ct = createCacheable("somevalue");
    CacheablePtr oldValue;
    CacheableKeyPtr keyPtr = CacheableKey::create("1");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    VersionTagPtr versionTag;
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 1, "expected size 1.");
    keyPtr = CacheableKey::create("2");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 2, "expected size 2.");
    keyPtr = CacheableKey::create("3");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 3, "expected size 3.");
    keyPtr = CacheableKey::create("4");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 4, "expected size 4.");
    keyPtr = CacheableKey::create("5");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    ASSERT(entries->size() == 5, "expected size 5.");
    LOG("Map is now at the limit.");
    keyPtr = CacheableKey::create("6");
    ASSERT(keyPtr != NULLPTR, "expected keyPtr non-NULL");
    LOG("About to spill over.");
    entries->put(keyPtr, ct, me, oldValue, -1, 0, versionTag);
    LOG("Spilled over.");
    ASSERT(entries->size() == 5, "expected size 5.");
    LOG("Limit was preserved.");
  }
END_TEST(LRULimitEvictTest)

#endif

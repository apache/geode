#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

#include "locator_globals.hpp"
#include "LocatorHelper.hpp"

CacheableStringPtr getUString(int index) {
  std::wstring baseStr(40, L'\x20AC');
  wchar_t indexStr[15];
  swprintf(indexStr, 14, L"%10d", index);
  baseStr.append(indexStr);
  return CacheableString::create(baseStr.data(),
                                 static_cast<int32_t>(baseStr.length()));
}

CacheableStringPtr getUAString(int index) {
  std::wstring baseStr(40, L'A');
  wchar_t indexStr[15];
  swprintf(indexStr, 14, L"%10d", index);
  baseStr.append(indexStr);
  return CacheableString::create(baseStr.data(),
                                 static_cast<int32_t>(baseStr.length()));
}

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, populateServer)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int i = 0; i < 5; i++) {
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[i]);
      regPtr->create(keyPtr, vals[i]);
    }
    SLEEP(200);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, setupClient2)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locatorsG,
                                    "__TEST_POOL1__", true, true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->registerAllKeys(false, NULLPTR, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verify)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int i = 0; i < 5; i++) {
      CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[i]);
      char buf[1024];
      sprintf(buf, "key[%s] should have been found", keys[i]);
      ASSERT(regPtr->containsKey(keyPtr1), buf);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, updateKeys)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[index]);
      regPtr->put(keyPtr, nvals[index]);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verifyUpdates)
  {
    SLEEP(2000);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableKeyPtr keyPtr = CacheableKey::create(keys[index]);
      char buf[1024];
      sprintf(buf, "key[%s] should have been found", keys[index]);
      ASSERT(regPtr->containsKey(keyPtr), buf);
      CacheableStringPtr val =
          dynCast<CacheableStringPtr>(regPtr->getEntry(keyPtr)->getValue());
      ASSERT(strcmp(val->asChar(), nvals[index]) == 0,
             "Incorrect value for key");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutUnicodeStrings)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUString(index);
      CacheablePtr val = Cacheable::create(index + 100);
      reg0->put(key, val);
    }
    LOG("PutUnicodeStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, GetUnicodeStrings)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUString(index);
      CacheableInt32Ptr val = dynCast<CacheableInt32Ptr>(reg0->get(key));
      ASSERT(val != NULLPTR, "expected non-null value in get");
      ASSERT(val->value() == (index + 100), "unexpected value in get");
    }
    reg0->unregisterAllKeys();
    VectorOfCacheableKey vec;
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUString(index);
      vec.push_back(key);
    }
    reg0->registerKeys(vec);
    LOG("GetUnicodeStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UpdateUnicodeStrings)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUString(index);
      CacheablePtr val = CacheableFloat::create(index + 20.0F);
      reg0->put(key, val);
    }
    LOG("UpdateUnicodeStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CheckUpdateUnicodeStrings)
  {
    SLEEP(2000);
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUString(index);
      CacheableFloatPtr val =
          dynCast<CacheableFloatPtr>(reg0->getEntry(key)->getValue());
      ASSERT(val != NULLPTR, "expected non-null value in get");
      ASSERT(val->value() == (index + 20.0F), "unexpected value in get");
    }
    LOG("CheckUpdateUnicodeStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutASCIIAsWideStrings)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUAString(index);
      CacheablePtr val = getUString(index + 20);
      reg0->put(key, val);
    }
    LOG("PutASCIIAsWideStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, GetASCIIAsWideStrings)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUAString(index);
      CacheableStringPtr val = dynCast<CacheableStringPtr>(reg0->get(key));
      CacheableStringPtr expectedVal = getUString(index + 20);
      ASSERT(val != NULLPTR, "expected non-null value in get");
      ASSERT(wcscmp(val->asWChar(), expectedVal->asWChar()) == 0,
             "unexpected value in get");
      ASSERT(*val.ptr() == *expectedVal.ptr(), "unexpected value in get");
    }
    VectorOfCacheableKey vec;
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUAString(index);
      vec.push_back(key);
    }
    reg0->registerKeys(vec);
    LOG("GetASCIIAsWideStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UpdateASCIIAsWideStrings)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUAString(index);
      CacheablePtr val = getUAString(index + 10);
      reg0->put(key, val);
    }
    LOG("UpdateASCIIAsWideStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CheckUpdateASCIIAsWideStrings)
  {
    SLEEP(2000);
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    for (int index = 0; index < 5; ++index) {
      CacheableStringPtr key = getUAString(index);
      CacheableStringPtr val = dynCast<CacheableStringPtr>(reg0->get(key));
      CacheableStringPtr expectedVal = getUAString(index + 10);
      ASSERT(val != NULLPTR, "expected non-null value in get");
      ASSERT(*val.ptr() == *expectedVal.ptr(), "unexpected value in get");
    }
    LOG("CheckUpdateASCIIAsWideStrings complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CheckUpdateBug1001)
  {
    try {
      const wchar_t* str = L"Pivotal";
      CacheableStringPtr lCStringP = CacheableString::create(
          str, static_cast<int32_t>(wcslen(str) + 1) * sizeof(wchar_t));
      const wchar_t* lRtnCd ATTR_UNUSED = lCStringP->asWChar();
    } catch (const Exception& gemfireExcp) {
      printf("%s: %s", gemfireExcp.getName(), gemfireExcp.getMessage());
      FAIL("Should not have got exception.");
    }

    try {
      const wchar_t* str = L"Pivotal?17?";
      CacheableStringPtr lCStringP = CacheableString::create(
          str, static_cast<int32_t>(wcslen(str) + 1) * sizeof(wchar_t));
      const wchar_t* lRtnCd ATTR_UNUSED = lCStringP->asWChar();
    } catch (const Exception& gemfireExcp) {
      printf("%s: %s", gemfireExcp.getName(), gemfireExcp.getMessage());
      FAIL("Should not have got exception.");
    }

    LOG("CheckUpdateBug1001 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StopClient2)
  {
    cleanProc();
    LOG("CLIENT2 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1)
    CALL_TASK(CreateServer1_With_Locator);
    CALL_TASK(SetupClient1);
    CALL_TASK(populateServer);
    CALL_TASK(setupClient2);
    CALL_TASK(verify);
    CALL_TASK(updateKeys);
    CALL_TASK(verifyUpdates);
    CALL_TASK(PutUnicodeStrings);
    CALL_TASK(GetUnicodeStrings);
    CALL_TASK(UpdateUnicodeStrings);
    CALL_TASK(CheckUpdateUnicodeStrings);
    CALL_TASK(PutASCIIAsWideStrings);
    CALL_TASK(GetASCIIAsWideStrings);
    CALL_TASK(UpdateASCIIAsWideStrings);
    CALL_TASK(CheckUpdateASCIIAsWideStrings);
    CALL_TASK(CheckUpdateBug1001);
    CALL_TASK(StopClient1);
    CALL_TASK(StopClient2);
    CALL_TASK(StopServer);
    CALL_TASK(CloseLocator1);
  }
END_MAIN

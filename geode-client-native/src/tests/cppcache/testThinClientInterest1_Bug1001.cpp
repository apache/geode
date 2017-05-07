#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

bool isLocalServer = true;
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );

CacheableStringPtr getUString(int index)
{
  std::wstring baseStr(40, L'\x20AC');
  wchar_t indexStr[15];
  swprintf(indexStr, 14, L"%10d", index);
  baseStr.append(indexStr);
  return CacheableString::create(baseStr.data(), baseStr.length());
}

CacheableStringPtr getUAString(int index)
{
  std::wstring baseStr(40, L'A');
  wchar_t indexStr[15];
  swprintf(indexStr, 14, L"%10d", index);
  baseStr.append(indexStr);
  return CacheableString::create(baseStr.data(), baseStr.length());
}

DUNIT_TASK(SERVER1, StartServer)
{
  if ( isLocalServer )
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml");
  LOG("SERVER started");
}
END_TASK(StartServer)

DUNIT_TASK(CLIENT1, SetupClient1)
{
  initClient(true);
  createRegion( regionNames[0], false, endPoint, true);
}
END_TASK(SetupClient1)



DUNIT_TASK(CLIENT1, populateServer)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  for(int i=0; i < 5; i++)
  {
     CacheableKeyPtr keyPtr = CacheableKey::create(keys[i]);
     regPtr->create(keyPtr, vals[i]);
  }
  SLEEP(200);

}
END_TASK(populateServer)

DUNIT_TASK(CLIENT2, setupClient2)
{
  initClient(true);
  createRegion( regionNames[0], false, endPoint, true);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  regPtr->registerAllKeys(false, NULLPTR, true);
}
END_TASK(setupClient2)

DUNIT_TASK(CLIENT2, verify)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  for(int i=0; i<5; i++)
  {
      CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[i]);
      char buf[1024];
      sprintf(buf, "key[%s] should have been found", keys[i]);
      ASSERT( regPtr->containsKey( keyPtr1 ), buf);
  }
}
END_TASK(verify)

DUNIT_TASK(CLIENT1, updateKeys)
{
  RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
  for (int index = 0; index < 5; ++index) {
    CacheableKeyPtr keyPtr = CacheableKey::create(keys[index]);
    regPtr->put(keyPtr, nvals[index]);
  }
}
END_TASK(updateKeys)

DUNIT_TASK(CLIENT2, verifyUpdates)
{
  SLEEP(2000);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  for(int index = 0; index < 5; ++index) {
    CacheableKeyPtr keyPtr = CacheableKey::create(keys[index]);
    char buf[1024];
    sprintf(buf, "key[%s] should have been found", keys[index]);
    ASSERT(regPtr->containsKey( keyPtr), buf);
    CacheableStringPtr val = dynCast<CacheableStringPtr>(
        regPtr->getEntry(keyPtr)->getValue());
    ASSERT(strcmp(val->asChar(), nvals[index]) == 0, "Incorrect value for key");
  }
}
END_TASK(verifyUpdates)

DUNIT_TASK(CLIENT1, PutUnicodeStrings)
{
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
  for (int index = 0; index < 5; ++index) {
    CacheableStringPtr key = getUString(index);
    CacheablePtr val = Cacheable::create(index + 100);
    reg0->put(key, val);
  }
  LOG("PutUnicodeStrings complete.");
}
END_TASK(PutUnicodeStrings)

DUNIT_TASK(CLIENT2, GetUnicodeStrings)
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
END_TASK(GetUnicodeStrings)

DUNIT_TASK(CLIENT1, UpdateUnicodeStrings)
{
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
  for (int index = 0; index < 5; ++index) {
    CacheableStringPtr key = getUString(index);
    CacheablePtr val = CacheableFloat::create(index + 20.0F);
    reg0->put(key, val);
  }
  LOG("UpdateUnicodeStrings complete.");
}
END_TASK(UpdateUnicodeStrings)

DUNIT_TASK(CLIENT2, CheckUpdateUnicodeStrings)
{
  SLEEP(2000);
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
  for (int index = 0; index < 5; ++index) {
    CacheableStringPtr key = getUString(index);
    CacheableFloatPtr val = dynCast<CacheableFloatPtr>(
        reg0->getEntry(key)->getValue());
    ASSERT(val != NULLPTR, "expected non-null value in get");
    ASSERT(val->value() == (index + 20.0F), "unexpected value in get");
  }
  LOG("CheckUpdateUnicodeStrings complete.");
}
END_TASK(CheckUpdateUnicodeStrings)

DUNIT_TASK(CLIENT1, PutASCIIAsWideStrings)
{
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
  for (int index = 0; index < 5; ++index) {
    CacheableStringPtr key = getUAString(index);
    CacheablePtr val = getUString(index + 20);
    reg0->put(key, val);
  }
  LOG("PutASCIIAsWideStrings complete.");
}
END_TASK(PutASCIIAsWideStrings)

DUNIT_TASK(CLIENT2, GetASCIIAsWideStrings)
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
END_TASK(GetASCIIAsWideStrings)

DUNIT_TASK(CLIENT1, UpdateASCIIAsWideStrings)
{
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
  for (int index = 0; index < 5; ++index) {
    CacheableStringPtr key = getUAString(index);
    CacheablePtr val = getUAString(index + 10);
    reg0->put(key, val);
  }
  LOG("UpdateASCIIAsWideStrings complete.");
}
END_TASK(UpdateASCIIAsWideStrings)

DUNIT_TASK(CLIENT2, CheckUpdateASCIIAsWideStrings)
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
END_TASK(CheckUpdateASCIIAsWideStrings)

DUNIT_TASK(CLIENT1, CheckUpdateBug1001)
{
  try{ 
    const wchar_t* str = L"Pivotal";
    CacheableStringPtr lCStringP = CacheableString::create(str,(wcslen(str)+1)*sizeof(wchar_t));
    const wchar_t *lRtnCd = lCStringP->asWChar();
  }catch(const Exception & gemfireExcp){
    printf("%s: %s", gemfireExcp.getName(), gemfireExcp.getMessage());
    FAIL( "Should not have got exception." );
  }

  try{ 
    const wchar_t* str = L"Pivotal?17?";
    CacheableStringPtr lCStringP = CacheableString::create(str,(wcslen(str)+1)*sizeof(wchar_t));
    const wchar_t *lRtnCd = lCStringP->asWChar();
  }catch(const Exception & gemfireExcp){
    printf("%s: %s", gemfireExcp.getName(), gemfireExcp.getMessage());
    FAIL( "Should not have got exception." );
  }

  LOG("CheckUpdateBug1001 complete.");
}
END_TASK(CheckUpdateASCIIAsWideStrings)

DUNIT_TASK(CLIENT1, StopClient1)
{
  cleanProc();
  LOG("CLIENT1 stopped");
}
END_TASK(StopClient1)

DUNIT_TASK(CLIENT2, StopClient2)
{
  cleanProc();
  LOG("CLIENT2 stopped");
}
END_TASK(StopClient2)

DUNIT_TASK(SERVER1, StopServer)
{
  if ( isLocalServer )
    CacheHelper::closeServer( 1 );
  LOG("SERVER stopped");
}
END_TASK(StopServer)

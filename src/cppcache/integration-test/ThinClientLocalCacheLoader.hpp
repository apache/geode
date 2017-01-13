/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef THINCLIENTCACHELOADER_HPP_
#define THINCLIENTCACHELOADER_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "TallyLoader.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

TallyLoaderPtr reg1Loader1;
int numLoads = 0;
DistributedSystemPtr dSysPtr;
CachePtr cachePtr;
RegionPtr regionPtr;

class ThinClientTallyLoader : public TallyLoader {
 public:
  ThinClientTallyLoader() : TallyLoader() {}

  virtual ~ThinClientTallyLoader() {}

  CacheablePtr load(const RegionPtr& rp, const CacheableKeyPtr& key,
                    const UserDataPtr& aCallbackArgument) {
    int32_t loadValue = dynCast<CacheableInt32Ptr>(
                            TallyLoader::load(rp, key, aCallbackArgument))
                            ->value();
    char lstrvalue[32];
    sprintf(lstrvalue, "%i", loadValue);
    CacheableStringPtr lreturnValue = CacheableString::create(lstrvalue);
    if (key != NULLPTR && (NULL != rp->getAttributes()->getEndpoints() ||
                           rp->getAttributes()->getPoolName() != NULL)) {
      LOGDEBUG("Putting the value (%s) for local region clients only ",
               lstrvalue);
      rp->put(key, lreturnValue);
    }
    return lreturnValue;
  }

  void close(const RegionPtr& region) {
    LOG(" ThinClientTallyLoader::close() called");
    if (region != NULLPTR) {
      LOGINFO(" Region %s is Destroyed = %d ", region->getName(),
              region->isDestroyed());
      ASSERT(region->isDestroyed() == true,
             "region.isDestroyed should return true");
      /*
      if(region.ptr() != NULL && region.ptr()->getCache() != NULLPTR){
        LOGINFO(" Cache Name is Closed = %d ",
      region.ptr()->getCache()->isClosed());
      }else{
        LOGINFO(" regionPtr or cachePtr is NULLPTR");
      }
      */
    } else {
      LOGINFO(" region is NULLPTR");
    }
  }
};

void validateEventCount(int line) {
  LOGINFO("ValidateEvents called from line (%d).", line);
  ASSERT(reg1Loader1->getLoads() == numLoads,
         "Got wrong number of loader events.");
}

DUNIT_TASK_DEFINITION(SERVER1, StartServer)
  {
    CacheHelper::initServer(1, "cacheserver_loader.xml");
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient)
  {
    // Create a GemFire Cache with the "client_Loader.xml" Cache XML file.
    const char* clientXmlFile = "client_Loader.xml";
    static char* path = ACE_OS::getenv("TESTSRC");
    std::string clientXml = path;
    clientXml += "/";
    clientXml += clientXmlFile;
    CacheFactoryPtr cacheFactoryPtr = CacheFactory::createCacheFactory()
        ->set("cache-xml-file", clientXml.c_str());
    cachePtr =
        cacheFactoryPtr->create();
    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML
    // file.
    regionPtr = cachePtr->getRegion("/root/exampleRegion");
    LOGINFO("Obtained the Region from the Cache");

    // Plugin the ThinClientTallyLoader to the Region.
    AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
    reg1Loader1 = new ThinClientTallyLoader();
    attrMutatorPtr->setCacheLoader(reg1Loader1);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClientEvents)
  {
    numLoads = 0;
    regionPtr = NULLPTR;
    dSysPtr = NULLPTR;
    cachePtr = NULLPTR;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testLoader)
  {
    CacheableKeyPtr keyPtr = CacheableKey::create("Key0");

    ASSERT(!regionPtr->containsKey(keyPtr),
           "Key should not have been found in region.");
    // now having the Callbacks set, lets call the loader
    ASSERT(regionPtr->get(keyPtr) != NULLPTR, "Expected non null value");

    RegionEntryPtr regEntryPtr = regionPtr->getEntry(keyPtr);
    CacheablePtr valuePtr = regEntryPtr->getValue();
    int val = atoi(valuePtr->toString()->asChar());
    LOGFINE("val for keyPtr is %d", val);
    numLoads++;
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testDestroy)
  {
    CacheableKeyPtr keyPtr2 = CacheableKey::create("Key1");
    regionPtr->destroy(keyPtr2);
    // Verify the sequence destroy()->get() :- CacheLoader to be invoked.
    regionPtr->get(keyPtr2);
    RegionEntryPtr regEntryPtr2 = regionPtr->getEntry(keyPtr2);
    CacheablePtr valuePtr2 = regEntryPtr2->getValue();
    int val2 = atoi(valuePtr2->toString()->asChar());
    LOGFINE("val2 for keyPtr2 is %d", val2);
    numLoads++;
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testInvalidateKey)
  {
    CacheableKeyPtr keyPtr2 = CacheableKey::create("Key2");
    regionPtr->put(keyPtr2, "Value2");
    regionPtr->invalidate(keyPtr2);
    // Verify the sequence invalidate()->get() :- CacheLoader to be invoked.
    regionPtr->get(keyPtr2);
    RegionEntryPtr regEntryPtr = regionPtr->getEntry(keyPtr2);
    CacheablePtr valuePtr = regEntryPtr->getValue();
    int val = atoi(valuePtr->toString()->asChar());
    LOGFINE("val for keyPtr1 is %d", val);
    numLoads++;
    validateEventCount(__LINE__);

    // Verify the sequence put()->invalidate()->get()->invalidate()->get() :-
    // CacheLoader to be invoked twice
    // once after each get.
    CacheableKeyPtr keyPtr4 = CacheableKey::create("Key4");
    regionPtr->put(keyPtr4, "Value4");
    regionPtr->invalidate(keyPtr4);
    regionPtr->get(keyPtr4);
    RegionEntryPtr regEntryPtr1 = regionPtr->getEntry(keyPtr4);
    CacheablePtr valuePtr1 = regEntryPtr1->getValue();
    int val1 = atoi(valuePtr1->toString()->asChar());
    LOGFINE("val1 for keyPtr4 is %d", val1);
    numLoads++;
    validateEventCount(__LINE__);

    regionPtr->invalidate(keyPtr4);
    regionPtr->get(keyPtr4);
    RegionEntryPtr regEntryPtr2 = regionPtr->getEntry(keyPtr4);
    CacheablePtr valuePtr2 = regEntryPtr2->getValue();
    int val2 = atoi(valuePtr2->toString()->asChar());
    LOGFINE("val2 for keyPtr4 is %d", val2);
    numLoads++;
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testInvalidateRegion)
  {
    CacheableKeyPtr keyPtr3 = CacheableKey::create("Key3");
    regionPtr->put(keyPtr3, "Value3");
    // Verify the sequence invalidateRegion()->get() :- CacheLoader to be
    // invoked.
    regionPtr->invalidateRegion();
    regionPtr->get(keyPtr3);
    RegionEntryPtr regEntryPtr = regionPtr->getEntry(keyPtr3);
    CacheablePtr valuePtr = regEntryPtr->getValue();
    int val = atoi(valuePtr->toString()->asChar());
    LOGFINE("val for keyPtr3 is %d", val);
    numLoads++;
    validateEventCount(__LINE__);

    // Verify the sequence
    // put()->invalidateRegion()->get()->invalidateRegion()->get() :-
    // CacheLoader
    // to be invoked twice.
    // once after each get.
    CacheableKeyPtr keyPtr4 = CacheableKey::create("Key4");
    regionPtr->put(keyPtr4, "Value4");
    regionPtr->invalidateRegion();
    regionPtr->get(keyPtr4);
    RegionEntryPtr regEntryPtr1 = regionPtr->getEntry(keyPtr4);
    CacheablePtr valuePtr1 = regEntryPtr1->getValue();
    int val1 = atoi(valuePtr1->toString()->asChar());
    LOGFINE("val1 for keyPtr4 is %d", val1);
    numLoads++;
    validateEventCount(__LINE__);

    regionPtr->invalidateRegion();
    regionPtr->get(keyPtr4);
    RegionEntryPtr regEntryPtr2 = regionPtr->getEntry(keyPtr4);
    CacheablePtr valuePtr2 = regEntryPtr2->getValue();
    int val2 = atoi(valuePtr2->toString()->asChar());
    LOGFINE("val2 for keyPtr4 is %d", val2);
    numLoads++;
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    cleanProc();
  }
END_TASK_DEFINITION

void runCacheLoaderTest() {
  CALL_TASK(InitClientEvents);
  CALL_TASK(StartServer);
  CALL_TASK(SetupClient);
  CALL_TASK(testLoader);
  CALL_TASK(testDestroy);
  CALL_TASK(testInvalidateKey);
  CALL_TASK(testInvalidateRegion);
  CALL_TASK(CloseCache1);
  CALL_TASK(StopServer)
}
#endif /*THINCLIENTCACHELOADER_HPP_*/

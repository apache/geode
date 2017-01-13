/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef THINCLIENTDISTOPS_HPP_
#define THINCLIENTDISTOPS_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"
#include "TallyLoader.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

static bool isLocator = false;
static bool isLocalServer = true;
static int numberOfLocators = 1;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
const char* poolName = "__TESTPOOL1_";
TallyListenerPtr reg1Listener1, reg1Listener2;
TallyWriterPtr reg1Writer1, reg1Writer2;
TallyLoaderPtr reg1Loader1, reg1Loader2;
int numCreates = 0;
int numUpdates = 0;
int numLoads = 0;

#include "LocatorHelper.hpp"

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
};

void setCacheListener(const char* regName, TallyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void setCacheLoader(const char* regName, TallyLoaderPtr regLoader) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheLoader(regLoader);
}

void setCacheWriter(const char* regName, TallyWriterPtr regWriter) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheWriter(regWriter);
}

void validateEventCount(int line) {
  LOGINFO("ValidateEvents called from line (%d).", line);
  ASSERT(reg1Listener1->getCreates() == numCreates,
         "Got wrong number of creation events.");
  ASSERT(reg1Listener1->getUpdates() == numUpdates,
         "Got wrong number of update events.");
  ASSERT(reg1Loader1->getLoads() == numLoads,
         "Got wrong number of loader events.");
  ASSERT(reg1Writer1->getCreates() == numCreates,
         "Got wrong number of writer events.");
}

DUNIT_TASK_DEFINITION(SERVER1, StartServer)
  {
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClientEvents)
  {
    numCreates = 0;
    numUpdates = 0;
    numLoads = 0;
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient_Pooled_Locator)
  {
    initClient(true);
    reg1Listener1 = new TallyListener();
    createPooledRegion(regionNames[0], false, locatorsG, poolName, true,
                       reg1Listener1);
    reg1Loader1 = new ThinClientTallyLoader();
    reg1Writer1 = new TallyWriter();
    setCacheLoader(regionNames[0], reg1Loader1);
    setCacheWriter(regionNames[0], reg1Writer1);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testLoaderAndWriter)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr = CacheableKey::create(keys[0]);
    VectorOfCacheableKey keys;
    keys.push_back(keyPtr);
    regPtr->registerKeys(keys, NULLPTR);

    /*NIL: Changed the asserion due to the change in invalidate.
      Now we create new entery for every invalidate event received or
      localInvalidate call
      so expect  containsKey to returns true insted of false earlier. */
    ASSERT(regPtr->containsKey(keyPtr), "Key should found in region.");
    // now having all the Callbacks set, lets call the loader and writer
    ASSERT(regPtr->get(keyPtr) != NULLPTR, "Expected non null value");

    RegionEntryPtr regEntryPtr = regPtr->getEntry(keyPtr);
    CacheablePtr valuePtr = regEntryPtr->getValue();
    int val = atoi(valuePtr->toString()->asChar());
    LOGFINE("val for keyPtr is %d", val);
    ASSERT(val == 0, "Expected value CacheLoad value should be 0");
    numLoads++;
    numCreates++;

    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testCreatesAndUpdates)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);
    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);
    VectorOfCacheableKey keys;
    keys.push_back(keyPtr1);
    keys.push_back(keyPtr2);
    regPtr->registerKeys(keys, NULLPTR);

    // Do a create followed by a create on the same key
    /*NIL: Changed the asserion due to the change in invalidate.
      Now we create new entery for every invalidate event received or
      localInvalidate call
      so expect  containsKey to returns true insted of false earlier.
      and used put call to update value for that key, instead of create call
      earlier or will get
      Region::create: Entry already exists in the region  */
    ASSERT(regPtr->containsKey(keyPtr1), "Key should found in region.");
    regPtr->put(keyPtr1, vals[1]);
    numCreates++;
    validateEventCount(__LINE__);

    /**
    see bug #252
    try {
      regPtr->create(keyPtr1, nvals[1]);
      ASSERT(NULL, "Expected an EntryExistsException");
    } catch(EntryExistsException &) {
      //Expected Behavior
    }
    validateEventCount(__LINE__);
    **/

    // Trigger an update event from a put
    regPtr->put(keyPtr1, nvals[1]);
    numUpdates++;
    validateEventCount(__LINE__);

    // This put creates a new value, verify update event is not received
    regPtr->put(keyPtr2, vals[2]);
    numCreates++;
    validateEventCount(__LINE__);

    // Do a get on a preexisting value to confirm no events are triggered
    regPtr->get(keyPtr2);
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testDestroy)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);
    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);
    regPtr->localInvalidate(keyPtr1);
    // Verify no listener activity after the invalidate
    validateEventCount(__LINE__);

    // Verify after update listener activity after a get on an invalidated value
    regPtr->get(keyPtr1);
    RegionEntryPtr regEntryPtr = regPtr->getEntry(keyPtr1);
    CacheablePtr valuePtr = regEntryPtr->getValue();
    int val = atoi(valuePtr->toString()->asChar());
    LOGFINE("val for keyPtr1 is %d", val);
    ASSERT(val == 0, "Expected value CacheLoad value should be 0");
    numUpdates++;
    validateEventCount(__LINE__);

    regPtr->destroy(keyPtr2);
    regPtr->get(keyPtr2);
    RegionEntryPtr regEntryPtr1 = regPtr->getEntry(keyPtr2);
    CacheablePtr valuePtr1 = regEntryPtr1->getValue();
    int val1 = atoi(valuePtr1->toString()->asChar());
    LOGFINE("val1 for keyPtr2 is %d", val1);
    ASSERT(val1 == 1, "Expected value CacheLoad value should be 1");
    numLoads++;
    numCreates++;
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

#endif /*THINCLIENTLISTENERINIT_HPP_*/

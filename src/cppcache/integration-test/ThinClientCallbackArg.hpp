/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef THINCLIENTCALLBACKARG_HPP_
#define THINCLIENTCALLBACKARG_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

/*
 * start server with NBS true
 * start one client which will entry operations ( put/invalidate/destroy ).
 * start 2nd client with cache listener,  writer and client notification true.
 * verify that listener is invoked and writer is not being invoked in 2nd client
 * + callback Argument
 */

static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
const char* poolName = "__TESTPOOL1_";
TallyListenerPtr regListener;
TallyWriterPtr regWriter;

#include "LocatorHelper.hpp"

void setCacheListener(const char* regName, TallyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void setCacheWriter(const char* regName, TallyWriterPtr regWriter) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheWriter(regWriter);
}

void createClientPooledLocatorRegion() {
  initClient(true);
  CacheableKeyPtr key0 = CacheableKey::create(keys[0]);
  LOG("Creating region in CLIENT , no-ack, cacheing enable, with-listener and "
      "writer");
  createPooledRegion(regionNames[0], false, locatorsG, poolName, true,
                     regListener, true);
  regWriter = new TallyWriter();
  setCacheWriter(regionNames[0], regWriter);
  regListener = new TallyListener();
  setCacheListener(regionNames[0], regListener);
  regWriter->setCallBackArg(key0);
  regListener->setCallBackArg(key0);
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->registerAllKeys();
}
void validateLocalListenerWriterData() {
  SLEEP(2000);
  ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
  ASSERT(regListener->isListenerInvoked() == true,
         "Listener Should be invoked");
  ASSERT(regWriter->isCallBackArgCalled() == true,
         "Writer CallbackArg Should be invoked");
  ASSERT(regListener->isCallBackArgCalled() == true,
         "Listener CallbackArg Should be invoked");
  regListener->showTallies();
  regWriter->showTallies();
}

void validateRemoteListenerWriterData() {
  SLEEP(2000);
  ASSERT(regWriter->isWriterInvoked() == false, "Writer Should not be invoked");
  ASSERT(regListener->isListenerInvoked() == true,
         "Listener Should be invoked");
  ASSERT(regWriter->isCallBackArgCalled() == false,
         "Writer CallbackArg Should not be invoked");
  ASSERT(regListener->isCallBackArgCalled() == true,
         "Listener CallbackArg Should be invoked");
  regListener->showTallies();
}

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pooled_Locator)
  { createClientPooledLocatorRegion(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2_Pooled_Locator)
  { createClientPooledLocatorRegion(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, doOperations)
  {
    CacheableKeyPtr key0 = CacheableKey::create(keys[0]);
    LOG("do entry operation from client 1");
    RegionOperations region(regionNames[0]);
    region.putOp(5, key0);
    SLEEP(1000);  // let the events reach at other end.
    region.putOp(5, key0);
    SLEEP(1000);
    region.invalidateOp(5, key0);
    SLEEP(1000);
    region.destroyOp(5, key0);
    SLEEP(1000);
    region.putOp(5, key0);
    SLEEP(1000);  // let the events reach at other end.
    region.removeOp(5, key0);
    SLEEP(1000);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, validateLocalListenerWriter)
  {
    validateLocalListenerWriterData();
    ASSERT(regWriter->getCreates() == 10, "Should be 5 creates");
    ASSERT(regListener->getCreates() == 10, "Should be 5 creates");
    ASSERT(regListener->getUpdates() == 5, "Should be 5 updates");
    ASSERT(regWriter->getUpdates() == 5, "Should be 5 updates");
    ASSERT(regListener->getInvalidates() == 5, "Should be 5 Invalidate");
    ASSERT(regWriter->getInvalidates() == 0, "Should be 0 Invalidate");
    ASSERT(regListener->getDestroys() == 10,
           "Should be 5 destroy");  // 5 destroys + 5 removes
    ASSERT(regWriter->getDestroys() == 10,
           "Should be 5 destroy");  // 5 destroys + 5 removes
  }
END_TASK_DEFINITION

/*
 * Remote side listener callback argument not found. it seemd server doesnt send
 * callback argument.
 * will check later.
 */
/*
DUNIT_TASK_DEFINITION(CLIENT2, validateRemoteListenerWriter)
{
  validateRemoteListenerWriterData();
  ASSERT( regListener->getCreates() == 5, "Should be 5 creates" );
  ASSERT( regListener->getUpdates() == 5, "Should be 5 updates" );
  ASSERT( regListener->getInvalidates() == 0, "Should be 0 Invalidate" );
  ASSERT( regListener->getDestroys() == 5, "Should be 5 destroy" );
}
END_TASK_DEFINITION
*/
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
void runCallbackArg() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator);

  CALL_TASK(SetupClient1_Pooled_Locator);
  CALL_TASK(SetupClient2_Pooled_Locator);

  CALL_TASK(doOperations);
  CALL_TASK(validateLocalListenerWriter);
  // CALL_TASK(validateRemoteListenerWriter);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}
#endif /*THINCLIENTCALLBACKARG_HPP_*/

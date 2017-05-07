/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef THINCLIENTREMOVEALL_HPP_
#define THINCLIENTREMOVEALL_HPP_


#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "testobject/PdxType.hpp"
#include "testobject/VariousPdxTypes.hpp"
#include <string>

#include "BuiltinCacheableWrappers.hpp"
#include "impl/Utils.hpp"
#include <gfcpp/statistics/StatisticsFactory.hpp>


#define ROOT_NAME "Notifications"
#define ROOT_SCOPE DISTRIBUTED_ACK
#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2


CacheHelper* cacheHelper = NULL;
static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;
const char * endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 2);
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, numberOfLocators);
const char* poolName="__TESTPOOL1_";

#include "LocatorHelper.hpp"

void initClient( const bool isthinClient )
{
  if ( cacheHelper == NULL ) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT( cacheHelper, "Failed to create a CacheHelper client instance." );
}
void cleanProc()
{
  if ( cacheHelper != NULL ) {
    delete cacheHelper;
  cacheHelper = NULL;
  }
}

CacheHelper * getHelper()
{
  ASSERT( cacheHelper != NULL, "No cacheHelper initialized." );
  return cacheHelper;
}

const char * keys[] = { "Key-1", "Key-2", "Key-3", "Key-4" };
const char * vals[] = { "Value-1", "Value-2", "Value-3", "Value-4" };
const char * nvals[] = { "New Value-1", "New Value-2", "New Value-3", "New Value-4" };

const char * regionNames[] = { "DistRegionAck", "DistRegionNoAck" };

const bool USE_ACK = true;
const bool NO_ACK = false;

void createRegion( const char * name, bool ackMode, const char * endpoints , bool isCacheEnabled, bool clientNotificationEnabled = false)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createRegion( name, ackMode, isCacheEnabled,
      NULLPTR, endpoints, clientNotificationEnabled );
  ASSERT( regPtr != NULLPTR, "Failed to create region." );
  LOG( "Region created." );
}

void createRegionLocal( const char * name, bool ackMode, const char * endpoints , bool isCacheEnabled, bool clientNotificationEnabled = false)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createRegion( name, ackMode, isCacheEnabled,
      NULLPTR, NULL, clientNotificationEnabled, true );
  ASSERT( regPtr != NULLPTR, "Failed to create region." );
  LOG( "Region created." );
}

void createPooledRegion( const char * name, bool ackMode, const char * endpoints, const char* locators,const char* poolname, bool clientNotificationEnabled = false, bool cachingEnable = true)
{
  LOG( "createRegion_Pool() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createPooledRegion(name,ackMode,endpoints, locators, poolname ,cachingEnable, clientNotificationEnabled);
  ASSERT( regPtr != NULLPTR, "Failed to create region." );
  LOG( "Pooled Region created." );
}

void createPooledRegionConcurrencyCheckDisabled( const char * name, bool ackMode, const char * endpoints, const char* locators,const char* poolname, bool clientNotificationEnabled = false, bool cachingEnable = true, bool concurrencyCheckEnabled = true)
{
  LOG( "createRegion_Pool() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createPooledRegionConcurrencyCheckDisabled(name,ackMode,endpoints, locators, poolname ,cachingEnable, clientNotificationEnabled, concurrencyCheckEnabled);
  ASSERT( regPtr != NULLPTR, "Failed to create region." );
  LOG( "Pooled Region created." );
}

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  if ( isLocalServer ) {
    CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
    LOG("SERVER1 started");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
{
  if ( isLocalServer ) {
    CacheHelper::initServer( 2, "cacheserver_notify_subscription2.xml" );
    LOG("SERVER2 started");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  initClient(true);
  createRegion( regionNames[0], USE_ACK, endPoints, true, true);
  createRegion( regionNames[1], NO_ACK, endPoints, true, true);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOneLocal)
{
  initClient(true);
  createRegionLocal( regionNames[0], USE_ACK, NULL, true, false);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG, poolName, true, true);
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, poolName, true, true);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator_NoCaching)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG, poolName, false, false);
  LOG( "StepOne_Pooled_Locator_NoCaching complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator_ConcurrencyCheckDisabled)
{
  initClient(true);
  createPooledRegionConcurrencyCheckDisabled( regionNames[0], USE_ACK, NULL, locatorsG, poolName, true, true, false);
  LOG( "StepOne_Pooled_Locator_ConcurrencyCheckDisabled complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndPoint)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL, poolName, true, true);
  createPooledRegion( regionNames[1], NO_ACK, endPoints, NULL, poolName, true, true);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndPoint_NoCaching)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL, poolName, false, false);
  LOG( "StepOne_Pooled_EndPoint_NoCaching complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndPoint_ConcurrencyCheckDisabled)
{
  initClient(true);
  createPooledRegionConcurrencyCheckDisabled( regionNames[0], USE_ACK, endPoints, NULL, poolName, true, true, false);
  LOG( "StepOne_Pooled_EndPoint_NoCaching complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
{
  initClient(true);
  // Client2 has notifications enabled.
  createRegion( regionNames[0], USE_ACK, endPoints, true, true);
  createRegion( regionNames[1], NO_ACK, endPoints, true, true);
  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG, poolName, true, true);
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, poolName, true, true);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_EndPoint)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL, poolName, true, true);
  createPooledRegion( regionNames[1], NO_ACK, endPoints, NULL, poolName, true, true);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, removeAllValidation)
{
  char key[2048];
  char value[2048];
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  VectorOfCacheableKey removeallkeys;
  try {
    regPtr0->removeAll(removeallkeys);
    FAIL("Did not get expected IllegalArgumentException exception");
  }
  catch(IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, CacheableInt32::create(1));
    FAIL("Did not get expected IllegalArgumentException exception");
  }
  catch(IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, NULLPTR);
    FAIL("Did not get expected IllegalArgumentException exception");
  }
  catch(IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException found exception");
  }

  for (int32_t item = 0; item < 1; item++) {
    sprintf(key, "key-%d", item);
    removeallkeys.push_back(CacheableKey::create(key));
  }

  try {
    regPtr0->removeAll(removeallkeys);
  }
  catch(EntryNotFoundException&) {
    FAIL("Got un expected entry not found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, CacheableInt32::create(1));
  }
  catch(EntryNotFoundException&) {
    FAIL("Got un expected entry not found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, NULLPTR);
  }
  catch(EntryNotFoundException&) {
    FAIL("Got un expected entry not found exception");
  }
  LOG( "removeAllValidation complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, removeAllValidationLocal)
{
  char key[2048];
  char value[2048];
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  VectorOfCacheableKey removeallkeys;
  try {
    regPtr0->removeAll(removeallkeys);
    FAIL("Did not get expected IllegalArgumentException exception");
  }
  catch(IllegalArgumentException&) {
      LOG("Got expected IllegalArgumentException found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, CacheableInt32::create(1));
    FAIL("Did not get expected IllegalArgumentException exception");
  }
  catch(IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, NULLPTR);
    FAIL("Did not get expected IllegalArgumentException exception");
  }
  catch(IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException found exception");
  }

  for (int32_t item = 0; item < 1; item++) {
    sprintf(key, "key-%d", item);
    removeallkeys.push_back(CacheableKey::create(key));
  }

  try {
    regPtr0->removeAll(removeallkeys);
  }
  catch(EntryNotFoundException&) {
    FAIL("Got un expected entry not found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, CacheableInt32::create(1));
  }
  catch(EntryNotFoundException&) {
    FAIL("Got un expected entry not found exception");
  }

  try {
    regPtr0->removeAll(removeallkeys, NULLPTR);
  }
  catch(EntryNotFoundException&) {
    FAIL("Got un expected entry not found exception");
  }

  LOG( "removeAllValidation complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, removeAllOps)
{
  HashMapOfCacheable entryMap;
  entryMap.clear();
  char key[2048];
  char value[2048];
  for (int32_t item = 0; item < 1; item++) {
    sprintf(key, "key-%d", item);
    sprintf(value, "%d", item);
    entryMap.insert(CacheableKey::create(key), CacheableString::create(value));
  }

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->putAll(entryMap);
  LOG("putAll1 complete");

  VectorOfCacheableKey removeallkeys;
  for (int32_t item = 0; item < 1; item++) {
    sprintf(key, "key-%d", item);
    removeallkeys.push_back(CacheableKey::create(key));
  }

  regPtr0->removeAll(removeallkeys);
  ASSERT(regPtr0->size() == 0, "remove all should remove the entries specified.");
  LOG( "remove all complete." );
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT1, removeAllOpsLocal)
{
  HashMapOfCacheable entryMap;
  entryMap.clear();
  char key[2048];
  char value[2048];
  for (int32_t item = 0; item < 1; item++) {
    sprintf(key, "key-%d", item);
    sprintf(value, "%d", item);
    entryMap.insert(CacheableKey::create(key), CacheableString::create(value));
  }

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->putAll(entryMap);
  LOG("putAll1 complete");

  VectorOfCacheableKey removeallkeys;
  for (int32_t item = 0; item < 1; item++) {
    sprintf(key, "key-%d", item);
    removeallkeys.push_back(CacheableKey::create(key));
  }

  regPtr0->removeAll(removeallkeys);
  ASSERT(regPtr0->size() == 0, "remove all should remove the entries specified.");
  LOG( "remove all complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,CloseCache2)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseServer1)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2,CloseServer2)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer( 2 );
    LOG("SERVER2 stopped");
  }
}
END_TASK_DEFINITION


void runRemoveAll( bool isLocator = true )
{
  if(isLocator)
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(CreateServer2_With_Locator_XML);
    CALL_TASK(StepOne_Pooled_Locator);
    CALL_TASK(StepTwo_Pooled_Locator);
  }
 else
 {
   CALL_TASK( CreateServer1 );
   CALL_TASK( CreateServer2 );
   CALL_TASK(StepOne_Pooled_EndPoint);
   CALL_TASK(StepTwo_Pooled_EndPoint);
 }

 CALL_TASK(removeAllValidation);
 CALL_TASK(removeAllOps);
 CALL_TASK( CloseCache1 );
 CALL_TASK( CloseCache2 );
 CALL_TASK( CloseServer1 );
 CALL_TASK( CloseServer2 );
 if(isLocator )
   CALL_TASK(CloseLocator1);
}

void runRemoveAllLocal()
{

 CALL_TASK(StepOneLocal);
 CALL_TASK(removeAllValidation);
 CALL_TASK(removeAllOpsLocal);
 CALL_TASK( CloseCache1 );

}


#endif /* THINCLIENTREMOVEALL_HPP_ */

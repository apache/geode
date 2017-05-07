/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>

#include <string>
#include <vector>

#define ROOT_NAME "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"


using namespace gemfire;
using namespace test;

CacheHelper* cacheHelper = NULL;

bool isLocalServer = false;
const char * endpoints = CacheHelper::getTcrEndpoints( isLocalServer, 3 );

static bool isLocator = false;
static int numberOfLocators = 0;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, numberOfLocators);

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

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



void createRegion(const char* name, bool ackMode, const char* endpoints,
    bool clientNotificationEnabled = false, bool caching = true)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, caching,
      NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
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

const char * keys[] = { "Key-1", "Key-2", "Key-3", "Key-4" };
const char * vals[] = { "Value-1", "Value-2", "Value-3", "Value-4" };
const char * nvals[] = { "New Value-1", "New Value-2", "New Value-3", "New Value-4" };

const char * regionNames[] = { "DistRegionAck", "DistRegionNoAck" };

const bool USE_ACK = true;
const bool NO_ACK = false;

#include "LocatorHelper.hpp"
DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  //if ( isLocalServer ) CacheHelper::initServer( 5,"cacheserverGFMOn.xml" );
  if ( isLocalServer )
  {
    CacheHelper::initGFMOnAgent();
    CacheHelper::initServer(1);
  }
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
{
  initClient(true);
  CacheHelper::initGFMOnAgent();
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG, "__TESTPOOL1_" );
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, "__TESTPOOL2_" );
  LOG( "StepOne_Pooled complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  initClient(true);
  createRegion(regionNames[0], USE_ACK, endpoints,true);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepTwo_Locator)
{
#define SIZE 100000
  CacheableKeyPtr keyPtr = NULLPTR;
  for(int i =0;i<=SIZE;i++)
  {
    char keyVal[10];
    char valVal[10];
    sprintf(keyVal,"Key%d",i);
    sprintf(valVal,"Val%d",i);
    CacheableKeyPtr key = CacheableString::create(keyVal);
    CacheablePtr value = CacheableString::create(valVal);
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
    reg0->put(key,value);
    reg1->put(key,value);
    dunit::sleep( 250 );
    reg0->get(key); //for get
    reg1->get(key); //for get
    dunit::sleep( 250 );
    reg0->localInvalidate(key);
    reg1->localInvalidate(key);
    reg0->get(key); //for miss
    reg1->get(key); //for miss
    dunit::sleep( 250 );
    reg0->get(key); //for get
    reg1->get(key); //for get
    dunit::sleep( 250 );
  }
  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT1, StepTwo)
{
#define SIZE 100000
  CacheableKeyPtr keyPtr = NULLPTR;
  for(int i =0;i<=SIZE;i++)
  {
    char keyVal[10];
    char valVal[10];
    sprintf(keyVal,"Key%d",i);
    sprintf(valVal,"Val%d",i);
    CacheableKeyPtr key = CacheableString::create(keyVal);
    CacheablePtr value = CacheableString::create(valVal);
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    reg0->put(key,value);
    dunit::sleep( 250 );
    reg0->get(key); //for get
    dunit::sleep( 250 );
    reg0->localInvalidate(key);
    reg0->get(key); //for miss
    dunit::sleep( 250 );
    reg0->get(key); //for get
    dunit::sleep( 250 );
  }
  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  cleanProc();
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(SERVER1,CloseServer1)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer(1);
     CacheHelper::closeGFMOnAgent( 1 );
    LOG("SERVER1 stopped");

  }
}
END_TASK_DEFINITION

void runGFMON(bool notwithLocator)
{
  if( notwithLocator )
  {
    CALL_TASK( CreateServer1 );
    CALL_TASK( StepOne );
    CALL_TASK( StepTwo );
  }
  else
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator);
    CALL_TASK(StepOne_Pooled_Locator);
    CALL_TASK(StepTwo_Locator);
  }
}
DUNIT_MAIN
{
  //runGFMON(true);
  runGFMON(false);
}
END_MAIN


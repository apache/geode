/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;

bool isLocalServer = true;
static bool isLocator = false;
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, 1);
#include "LocatorHelper.hpp"
DUNIT_TASK_DEFINITION(SERVER1, StartServer)
{
  if ( isLocalServer )
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml");
  LOG("SERVER started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1)
{
  initClient(true);
  createRegion( regionNames[0], false, endPoint, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0],false/*ack mode*/,NULL/*endpoints*/,locatorsG, "__TEST_POOL1__",true/*client notification*/);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_EndPoint)
{
  initClient(true);
  createPooledRegion( regionNames[0],false/*ack mode*/,endPoint/*endpoints*/,NULL, "__TEST_POOL1__",true/*client notification*/);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, setupClient2)
{
  initClient(true);
  createRegion( regionNames[0], false, endPoint, true);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, setupClient2_Pool_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0],false/*ack mode*/,NULL/*endpoints*/,locatorsG, "__TEST_POOL1__",true/*client notification*/);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, setupClient2_Pool_EndPoint)
{
  initClient(true);
  createPooledRegion( regionNames[0],false/*ack mode*/,endPoint/*endpoints*/,NULL, "__TEST_POOL1__",true/*client notification*/);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, populateServer)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  for(int i=0; i < 5; i++)
  {
     CacheableKeyPtr keyPtr = CacheableKey::create(keys[i]);
     regPtr->create(keyPtr, vals[i]);
  }

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verify)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  for(int i=0; i<5; i++)
  {
      CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[i]);
      char buf[1024];
      sprintf(buf, "key[%s] should not have been found", keys[i]);
      ASSERT( !regPtr->containsKey( keyPtr1 ), buf);
      CacheablePtr checkPtr = regPtr->get( keyPtr1 );
      verifyEntry( regionNames[0], keys[i], vals[i]);
  }
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
  if ( isLocalServer )
    CacheHelper::closeServer( 1 );
  LOG("SERVER stopped");
}
END_TASK_DEFINITION

void runThinClientInterest2(bool poolConfig, bool isLocator = true)
{
  if( poolConfig && isLocator )
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML)
  }
  else
  {
    CALL_TASK( StartServer );
  }
  if( !poolConfig )
  {
    CALL_TASK( SetupClient1 );
    CALL_TASK( setupClient2 );
  }
  else if(isLocator)
  {
    CALL_TASK( SetupClient1_Pool_Locator );
    CALL_TASK( setupClient2_Pool_Locator );
  }
  else
  {
    CALL_TASK( SetupClient1_Pool_EndPoint );
    CALL_TASK( setupClient2_Pool_EndPoint )
  }
  CALL_TASK( populateServer );
  CALL_TASK( verify );
  CALL_TASK( StopClient1 );
  CALL_TASK( StopClient2 );
  CALL_TASK( StopServer  );
  if( poolConfig && isLocator )
  {
    CALL_TASK(CloseLocator1);
  }
}




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
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );

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
  createRegion( regionNames[0], false, endPoint, false);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  CacheableKeyPtr key = CacheableKey::create((const char*) "key01");
  ASSERT(!regPtr->containsKeyOnServer(key), "key should not be there");
}
END_TASK(SetupClient1)

DUNIT_TASK(CLIENT2, SetupClient2)
{
  initClient(true);
  createRegion( regionNames[0], false, endPoint, false);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  CacheableKeyPtr key = CacheableKey::create((const char*) "key01");
  ASSERT(!regPtr->containsKeyOnServer(key), "key should not be there");
}
END_TASK(SetupClient2)

DUNIT_TASK(CLIENT1, puts)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  CacheableKeyPtr keyPtr = CacheableKey::create( (const char*) "key01" );
  CacheableBytesPtr valPtr = CacheableBytes::create((uint8_t*)"value01", 7);
  regPtr->put( keyPtr, valPtr );
  ASSERT(regPtr->containsKeyOnServer(keyPtr), "key should be there");
}
END_TASK(puts)

DUNIT_TASK(CLIENT2, VerifyPuts)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  CacheableKeyPtr keyPtr = CacheableKey::create( (const char*) "key01" );
  ASSERT(regPtr->containsKeyOnServer(keyPtr), "key should be there");
}
END_TASK(VerifyPuts)

DUNIT_TASK(SERVER1, StopServer)
{
  if ( isLocalServer )
    CacheHelper::closeServer( 1 );
  LOG("SERVER stopped");
}
END_TASK(StopServer)
DUNIT_TASK(CLIENT1,CloseCache1)
{
  cleanProc();
}
END_TASK(CloseCache1)
DUNIT_TASK(CLIENT2,CloseCache2)
{
  cleanProc();
}
END_TASK(CloseCache2)

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "testUtils.hpp"

/*Tests backward comptibility
  Client with version 1 
  Client with version 0 
  Client with version 3 (Not Interpreted by server) Throws Exception
  Both these clients should be able to handshake and do PUT on region. 
  Server should interpret PUT for each client as specified above
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define FEEDER s2p2 // acts as third client

bool isLocalServer = false;
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );


DUNIT_TASK_DEFINITION(SERVER1, StartServer)
{
  if ( isLocalServer )
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml");
  LOG("SERVER started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederInit)  
{
  initClient(true);
  int8_t version = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getVersionOrdinalForTest();
  // suppose to pass but in current release i.e 5.7 it is not supported as version 5.6 is not supported
  // its an expected failure
  TestUtils::getCacheImpl(getHelper( )->cachePtr)->setVersionOrdinalForTest(--version); // version = 0
  createRegion( regionNames[0], USE_ACK, endPoint);
  RegionPtr regPtr0 = getHelper()->getRegion( regionNames[0] );
  char key[16] = {'\0'};
  char value[16] = {'\0'};
  for(int i=0; i<100; i++){
    ACE_OS::sprintf(key,"key-%d",i);
    ACE_OS::sprintf(value,"value-%d",i);
    regPtr0->put( key, value);
    LOGDEBUG("Entry created Number : %s", key);
    gemfire::millisleep(10);
  }
  
  LOG( "FeederInit complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER,CloseFeeder)
{
  cleanProc();
  LOG("FEEDER closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientInit1)  
{
  try{
    initClient(true);
    //int8_t version = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getVersionOrdinalForTest();
    //TestUtils::getCacheImpl(getHelper( )->cachePtr)->setVersionOrdinalForTest((version)); // version = 1
    // current version taken
    createRegion( regionNames[0], USE_ACK, endPoint);
    RegionPtr regPtr0 = getHelper()->getRegion( regionNames[0] );
    char key[16] = {'\0'};
    char value[16] = {'\0'};
    for(int i=200; i<300; i++){
      ACE_OS::sprintf(key,"key-%d",i);
      ACE_OS::sprintf(value,"value-%d",i);
      regPtr0->put( key, value);
      LOGDEBUG("Entry created Number : %s", key);
      gemfire::millisleep(10);
    }
  }catch(const gemfire::Exception&){
    LOG( "inside CacheServerException block" );
    LOG( "failure" );
    FAIL("should not Throw Exception"); 
  }
  LOG( "ClientInit1 complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,ClientClose1)
{
  cleanProc();
  LOG("CLIENT1 closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, ClientInit2)  
{
  try{
    initClient(true);
    int8_t version = TestUtils::getCacheImpl(getHelper( )->cachePtr)->getVersionOrdinalForTest();
    version+=4; // invalid version
    TestUtils::getCacheImpl(getHelper( )->cachePtr)->setVersionOrdinalForTest(version); // version = 3
    createRegion( regionNames[0], USE_ACK, endPoint);
    RegionPtr regPtr0 = getHelper()->getRegion( regionNames[0] );
    char key[16] = {'\0'};
    char value[16] = {'\0'};
    for(int i=400; i<500; i++){
      ACE_OS::sprintf(key,"key-%d",i);
      ACE_OS::sprintf(value,"value-%d",i);
      regPtr0->put( key, value);
      LOGDEBUG("Entry created Number : %s", key);
      gemfire::millisleep(10);
    }
    FAIL("should Throw Exception"); 
  }catch(const gemfire::Exception&){
     LOG( "inside CacheServerException block" );
     LOG( "successful" );
  }
  LOG( "ClientInit2 complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,ClientClose2)
{
  cleanProc();
  LOG("CLIENT2 closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1 , CloseServer)
{
  CacheHelper::closeServer( 1 );
  LOG("SERVER closed");
}
END_TASK_DEFINITION

DUNIT_MAIN
{
  CALL_TASK( StartServer );
  // current release i.e 5.7 it is not supported
  //CALL_TASK( FeederInit ); 
  CALL_TASK( ClientInit1 );
  CALL_TASK( ClientInit2 );
  // current release i.e 5.7 it is not supported
  //CALL_TASK( CloseFeeder );
  CALL_TASK( ClientClose1 );
  CALL_TASK( ClientClose2 );
  CALL_TASK( CloseServer );
}
END_MAIN


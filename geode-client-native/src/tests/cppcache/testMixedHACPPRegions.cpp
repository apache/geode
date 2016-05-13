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
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;
const char * endPoints = CacheHelper::getTcrEndpoints( isLocalServer, 2 );

void initClient( int redundancyLevel )
{
  if ( cacheHelper == NULL ) {
    cacheHelper = new CacheHelper( endPoints, redundancyLevel );
  }
  ASSERT( cacheHelper, "Failed to create a CacheHelper client instance." );
}

void initClient(const char* clientXmlFile)
{
  if ( cacheHelper == NULL ) {
    cacheHelper = new CacheHelper( NULL, clientXmlFile );
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

void _verifyEntry( const char * name, const char * key, const char * val, bool noKey )
{
  // Verify key and value exist in this region, in this process.
  const char * value = ( val == 0 ) ? "" : val;
  char * buf = (char *)malloc( 1024 + strlen( key ) + strlen( value ));
  ASSERT( buf, "Unable to malloc buffer for logging." );
  if ( noKey )
    sprintf( buf, "Verify key %s does not exist in region %s", key, name );
  else if ( val == 0 )
    sprintf( buf, "Verify value for key %s does not exist in region %s", key, name );
  else
    sprintf( buf, "Verify value for key %s is: %s in region %s", key, value, name );
  LOG( buf );
  free(buf);

  RegionPtr regPtr = getHelper()->getRegion( name );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableKeyPtr keyPtr = createKey( key );

  // if the region is no ack, then we may need to wait...
  if ( noKey == false ) { // need to find the key!
    ASSERT( regPtr->containsKey( keyPtr ), "Key not found in region." );
  }
  if (val != NULL) { // need to have a value!
    ASSERT( regPtr->containsValueForKey( keyPtr ), "Value not found in region." );
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  uint32_t SLEEP = 10; // milliseconds
  uint32_t containsKeyCnt = 0;
  uint32_t containsValueCnt = 0;
  uint32_t testValueCnt = 0;

  for (int i = MAX; i >= 0; i--)
  {
    if ( noKey ) {
      if (regPtr->containsKey( keyPtr ))
        containsKeyCnt++;
      else
        break;
      ASSERT( containsKeyCnt < MAX, "Key found in region." );
    }
    if (val == NULL) {
      if ( regPtr->containsValueForKey( keyPtr ) )
        containsValueCnt++;
      else
        break;
      ASSERT( containsValueCnt < MAX, "Value found in region." );
    }

    if (val != NULL) {
      CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>(regPtr->get( keyPtr ));

      ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");
      char buf[1024];
      sprintf( buf, "In verify loop, get returned %s for key %s", checkPtr->asChar(), key );
      LOG( buf );
      if ( strcmp( checkPtr->asChar(), value ) != 0 ){
        testValueCnt++;
      }else{
        break;
      }
      ASSERT( testValueCnt < MAX, "Incorrect value found." );
    }
    dunit::sleep( SLEEP );
  }
}

#define verifyInvalid(x,y) _verifyInvalid( x, y, __LINE__ )

void _verifyInvalid( const char * name, const char * key, int line )
{
  char logmsg[1024];
  sprintf( logmsg, "verifyInvalid() called from %d.\n", line );
  LOG( logmsg );
  _verifyEntry(name, key, 0, false );
  LOG( "Entry invalidated." );
}

#define verifyDestroyed(x,y) _verifyDestroyed( x, y, __LINE__ )

void _verifyDestroyed( const char * name, const char * key, int line )
{
  char logmsg[1024];
  sprintf( logmsg, "verifyDestroyed() called from %d.\n", line );
  LOG( logmsg );
  _verifyEntry(name, key, 0, true );
  LOG( "Entry destroyed." );
}

#define verifyEntry(x,y,z) _verifyEntry( x, y, z, __LINE__ )

void _verifyEntry( const char * name, const char * key, const char * val, int line )
{
  char logmsg[1024];
  sprintf( logmsg, "verifyEntry() called from %d.\n", line );
  LOG( logmsg );
  _verifyEntry(name, key, val, false );
  LOG( "Entry verified." );
}

void destroyEntry( const char * name, const char * key )
{
  LOG( "destroyEntry() entered." );
  fprintf( stdout, "Destroying entry -- key: %s  in region %s\n", key, name );
  fflush( stdout );
  // Destroy entry, verify entry is destroyed
  CacheableKeyPtr keyPtr = CacheableKey::create( key );

  RegionPtr regPtr = getHelper()->getRegion( name );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT( regPtr->containsKey( keyPtr ), "Key should have been found in region." );

  regPtr->destroy( keyPtr );
  LOG( "Destroy entry." );

  verifyDestroyed( name, key );
  LOG( "Entry destroyed." );
}

void localDestroyEntry( const char * name, const char * key )
{
  LOG( "destroyEntry() entered." );
  fprintf( stdout, "local Destroying entry -- key: %s  in region %s\n", key, name );
  fflush( stdout );
  // Destroy entry, verify entry is destroyed
  CacheableKeyPtr keyPtr = CacheableKey::create( key );

  RegionPtr regPtr = getHelper()->getRegion( name );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT( regPtr->containsKey( keyPtr ), "Key should have been found in region." );

  regPtr->localDestroy( keyPtr );
  LOG( "Destroy entry." );

  verifyDestroyed( name, key );
  LOG( "Entry destroyed." );
}

void createRegion( const char * name, bool ackMode, const char * endpoints ,bool clientNotificationEnabled = false)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true,
      NULLPTR /*listener*/, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG( "Region created." );
}

void createEntry( const char * name, const char * key, const char * value )
{
  LOG( "createEntry() entered." );
  fprintf( stdout, "Creating entry -- key: %s  value: %s in region %s\n", key, value, name );
  fflush( stdout );
  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey( key );
  CacheableStringPtr valPtr = CacheableString::create( value );

  RegionPtr regPtr = getHelper()->getRegion( name );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT( !regPtr->containsKey( keyPtr ), "Key should not have been found in region." );
  ASSERT( !regPtr->containsValueForKey( keyPtr ), "Value should not have been found in region." );

  //regPtr->create( keyPtr, valPtr );
  regPtr->put( keyPtr, valPtr );
  LOG( "Created entry." );

  verifyEntry( name, key, value );
  LOG( "Entry created." );
}

void updateEntry( const char * name, const char * key, const char * value )
{
  LOG( "updateEntry() entered." );
  fprintf( stdout, "Updating entry -- key: %s  value: %s in region %s\n", key, value, name );
  fflush( stdout );
  // Update entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey( key );
  CacheableStringPtr valPtr = CacheableString::create( value );

  RegionPtr regPtr = getHelper()->getRegion( name );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT( regPtr->containsKey( keyPtr ), "Key should have been found in region." );
  ASSERT( regPtr->containsValueForKey( keyPtr ), "Value should have been found in region." );

  regPtr->put( keyPtr, valPtr );
  LOG( "Put entry." );

  verifyEntry( name, key, value );
  LOG( "Entry updated." );
}

void doGetAgain( const char * name, const char * key, const char * value )
{
  LOG( "doGetAgain() entered." );
  fprintf( stdout, "get for entry -- key: %s  expecting value: %s in region %s\n", key, value, name );
  fflush( stdout );
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create( key );

  RegionPtr regPtr = getHelper()->getRegion( name );
  fprintf( stdout, "get  region name%s\n", regPtr->getName() );
  fflush( stdout );
  ASSERT(regPtr != NULLPTR, "Region not found.");


  CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>(regPtr->get( keyPtr)); // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
      char buf[1024];
      sprintf( buf, "In doGetAgain, get returned %s for key %s", checkPtr->asChar(), key );
      LOG( buf );
  }else{
    LOG("checkPtr is NULL");
  }
  verifyEntry( name, key, value );
  LOG( "GetAgain complete." );
}

void doNetsearch( const char * name, const char * key, const char * value )
{
  LOG( "doNetsearch() entered." );
  fprintf( stdout, "Netsearching for entry -- key: %s  expecting value: %s in region %s\n", key, value, name );
  fflush( stdout );
  static int count = 0;
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create( key );

  RegionPtr regPtr = getHelper()->getRegion( name );
  ASSERT(regPtr != NULLPTR, "Region not found.");
  fprintf( stdout, "netsearch  region %s\n", regPtr->getName() );
  fflush( stdout );

  if(count == 0){
    ASSERT( !regPtr->containsKey( keyPtr ), "Key should not have been found in region." );
    ASSERT( !regPtr->containsValueForKey( keyPtr ), "Value should not have been found in region." );
    count++;
  }
  CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>(regPtr->get( keyPtr)); // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
      char buf[1024];
      sprintf( buf, "In net search, get returned %s for key %s", checkPtr->asChar(), key );
      LOG( buf );
  }else{
    LOG("checkPtr is NULL");
  }
  verifyEntry( name, key, value );
  LOG( "Netsearch complete." );
}


const char * keys[] = { "Key-1", "Key-2", "Key-3", "Key-4" };
const char * vals[] = { "Value-1", "Value-2", "Value-3", "Value-4" };
const char * nvals[] = { "New Value-1", "New Value-2", "New Value-3", "New Value-4" };

const char * regionNames[] = { "DistRegionAck", "DistRegionNoAck" };

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
{
  if ( isLocalServer ) CacheHelper::initServer( 2, "cacheserver_notify_subscription2.xml" );
  LOG("SERVER2 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1_Prog)
{
  initClient(1);

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, InitClient2_Prog)
{
  initClient(1);

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  createRegion( regionNames[0], USE_ACK, endPoints);
  createRegion( regionNames[1], NO_ACK, endPoints);

  createEntry( regionNames[0], keys[0], vals[0] );
  createEntry( regionNames[1], keys[2], vals[2] );

  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
{
  createRegion( regionNames[0], USE_ACK, endPoints);
  createRegion( regionNames[1], NO_ACK, endPoints);

  createEntry( regionNames[0], keys[1], vals[1] );
  createEntry( regionNames[1], keys[3], vals[3] );

  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
{
  doNetsearch( regionNames[0], keys[1], vals[1] );
  doNetsearch( regionNames[1], keys[3], vals[3] );

  updateEntry( regionNames[0], keys[0], nvals[0] );
  updateEntry( regionNames[1], keys[2], nvals[2] );
  LOG( "StepThree complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
{
  doNetsearch( regionNames[0], keys[0], nvals[0] );
  doNetsearch( regionNames[1], keys[2], nvals[2] );

  updateEntry( regionNames[0], keys[1], nvals[1] );
  updateEntry( regionNames[1], keys[3], nvals[3] );
  LOG( "StepFour complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
{
  localDestroyEntry( regionNames[0], keys[1] );
  localDestroyEntry( regionNames[1], keys[3] );
  doNetsearch( regionNames[0], keys[1], nvals[1] );
  doNetsearch( regionNames[1], keys[3], nvals[3] );
  LOG( "StepFive complete." );
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


DUNIT_MAIN
{
  CALL_TASK(CreateServer1);
  CALL_TASK(CreateServer2);

  CALL_TASK( InitClient1_Prog);
  CALL_TASK( InitClient2_Prog);

  CALL_TASK(StepOne);
  CALL_TASK(StepTwo);
  CALL_TASK(StepThree);
  CALL_TASK(StepFour);
  CALL_TASK(StepFive);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);

  CALL_TASK(CloseServer1);
  CALL_TASK(CloseServer2);
}
END_MAIN

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
#include <vector>

#define ROOT_NAME "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

bool isLocalServer = false;
const char * endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 3);
CacheHelper* cacheHelper = NULL;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2
static bool isLocator = false;
//static int numberOfLocators = 0;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, 1);
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

  if ( noKey == false ) { // need to find the key!
    ASSERT( regPtr->containsKey( keyPtr ), "Key not found in region." );
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
      if ( regPtr->containsValueForKey( keyPtr ) ){
        containsValueCnt++;
      }
      else
        break;
      ASSERT( containsValueCnt < MAX, "Value found in region." );
    }

    if (val != NULL) {
      CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>( regPtr->get( keyPtr ) );

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

#define verifyEntry(x,y,z) _verifyEntry( x, y, z, __LINE__ )

void _verifyEntry( const char * name, const char * key, const char * val, int line )
{
  char logmsg[1024];
  sprintf( logmsg, "verifyEntry() called from %d.\n", line );
  LOG( logmsg );
  _verifyEntry(name, key, val, false );
  LOG( "Entry verified." );
}

void createRegion( const char * name, bool ackMode, const char * endpoints ,bool clientNotificationEnabled = false)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true,
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
  ASSERT( regPtr != NULLPTR, "Region not found.");

  ASSERT( regPtr->containsKey( keyPtr ), "Key should have been found in region." );

  regPtr->put( keyPtr, valPtr );
  LOG( "Put entry." );

  verifyEntry( name, key, value );
  LOG( "Entry updated." );
}

void doNetsearch( const char * name, const char * key, const char * value )
{
  LOG( "doNetsearch() entered." );
  fprintf( stdout, "Netsearching for entry -- key: %s  expecting value: %s in region %s\n", key, value, name );
  fflush( stdout );
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create( key );

  RegionPtr regPtr = getHelper()->getRegion( name );
  fprintf( stdout, "netsearch  region %s\n", regPtr->getName() );
  fflush( stdout );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>( regPtr->get( keyPtr) ); // force a netsearch

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

std::vector<char *> storeEndPoints( const char * points )
{
 std::vector<char *> endpointNames;
 if (points != NULL)
 {
   char * ep = strdup(points);
   char *token = strtok( ep, ",");
   while(token)
   {
     endpointNames.push_back(token);
     token = strtok(NULL, ",");
   }
 }
 ASSERT( endpointNames.size() == 3, "There should be 3 end points" );
 return endpointNames;
}

std::vector<char *> endpointNames = storeEndPoints(endPoints);
const char * keys[] = { "Key-1", "Key-2", "Key-3", "Key-4" };
const char * vals[] = { "Value-1", "Value-2", "Value-3", "Value-4" };
const char * nvals[] = { "New Value-1", "New Value-2", "New Value-3", "New Value-4" };

const char * regionNames[] = { "DistRegionAck", "DistRegionNoAck" };

const bool USE_ACK = true;
const bool NO_ACK = false;


DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  if ( isLocalServer ) CacheHelper::initServer( 1 );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  initClient(true);
  char endpoints[1024];
  sprintf( endpoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  LOG(endpoints);
  createRegion( regionNames[0], USE_ACK, endpoints);
  createRegion( regionNames[1], NO_ACK, endpoints);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG,"__TEST_POOL1__");
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, "__TEST_POOL1__");
  LOG( "StepOne_Pooled_Locator complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndPoint)
{
  initClient(true);
  char endpoints[1024];
  sprintf( endpoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  createPooledRegion( regionNames[0], USE_ACK, endpoints, NULL,"__TEST_POOL1__");
  createPooledRegion( regionNames[1], NO_ACK, endpoints,NULL,"__TEST_POOL1__");
  LOG( "StepOne_Pooled_EndPoint complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
{
  initClient(true);
  char endpoints[1024];
  sprintf( endpoints, "%s,%s", endpointNames.at(0),endpointNames.at(2));
  LOG(endpoints);
  createRegion( regionNames[0], USE_ACK, endpoints);
  createRegion( regionNames[1], NO_ACK, endpoints);
  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG,"__TEST_POOL1__");
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, "__TEST_POOL1__");
  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_EndPoint)
{
  initClient(true);
  char endpoints[1024];
  sprintf( endpoints, "%s,%s", endpointNames.at(0),endpointNames.at(2));
  createPooledRegion( regionNames[0], USE_ACK, endpoints, NULL,"__TEST_POOL1__");
  createPooledRegion( regionNames[1], NO_ACK, endpoints, NULL, "__TEST_POOL1__");
  LOG( "StepTwo_Pooled_EndPoint complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
{
  createEntry( regionNames[0], keys[0], vals[0] );
  createEntry( regionNames[1], keys[2], vals[2] );
  LOG( "StepThree complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
{
  doNetsearch( regionNames[0], keys[0], vals[0] );
  doNetsearch( regionNames[1], keys[2], vals[2] );
  createEntry( regionNames[0], keys[1], vals[1] );
  createEntry( regionNames[1], keys[3], vals[3] );
  LOG( "StepFour complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2and3)
{
  if ( isLocalServer ) CacheHelper::initServer( 2 );
  LOG("SERVER2 started");
  if ( isLocalServer ) CacheHelper::initServer( 3 );
  LOG("SERVER3 started");
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
DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
{
  doNetsearch( regionNames[0], keys[1], vals[1] );
  doNetsearch( regionNames[1], keys[3], vals[3] );
  updateEntry( regionNames[0], keys[0], nvals[0] );
  updateEntry( regionNames[1], keys[2], nvals[2] );
  LOG( "StepFive complete." );
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, StepSix)
{
  doNetsearch( regionNames[0], keys[0], vals[0] );
  doNetsearch( regionNames[1], keys[2], vals[2] );
  updateEntry( regionNames[0], keys[1], nvals[1] );
  updateEntry( regionNames[1], keys[3], nvals[3] );
  LOG( "StepSix complete." );
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

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
{
  if (isLocalServer) {
    CacheHelper::closeServer(2);
    LOG("SERVER2 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2,CloseServer2and3)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer( 2 );
    LOG("SERVER2 stopped");
  }
  if ( isLocalServer ) {
    CacheHelper::closeServer( 3 );
    LOG("SERVER3 stopped");
  }
}
END_TASK_DEFINITION


void runThinClientFailover3( bool poolConfig = true, bool isLocator = true )
{
  if( poolConfig && isLocator )
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)
  }
  else
  {
    CALL_TASK( CreateServer1 );
  }
  if( !poolConfig )
  {
   CALL_TASK( StepOne );
   CALL_TASK( StepTwo );
  }
  else if( isLocator )
  {
    CALL_TASK( StepOne_Pooled_Locator);
    CALL_TASK( StepTwo_Pooled_Locator);
  }
  else
  {
    CALL_TASK( StepOne_Pooled_EndPoint);
    CALL_TASK( StepTwo_Pooled_EndPoint);
  }
  CALL_TASK( StepThree );
  CALL_TASK( StepFour );
  if(poolConfig && isLocator)
  {
    CALL_TASK(CreateServer2_With_Locator);
  }
  else
  {
    CALL_TASK( CreateServer2and3 );
  }
  CALL_TASK( CloseServer1 );//FailOver
  CALL_TASK( StepFive );
  CALL_TASK( StepSix );
  CALL_TASK( CloseCache1 );
  CALL_TASK( CloseCache2 );
  if (poolConfig && isLocator)
  {
    CALL_TASK(CloseServer2);
  }
  else
  {
    CALL_TASK(CloseServer2and3);
  }
  if( poolConfig && isLocator ) {
    CALL_TASK( CloseLocator1 );
  }
}


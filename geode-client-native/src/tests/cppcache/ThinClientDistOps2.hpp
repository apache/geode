/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef THINCLIENTDISTOPS2_HPP_
#define THINCLIENTDISTOPS2_HPP_

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

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

CacheHelper* cacheHelper = NULL;
static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;
const char* poolName = "__TEST_POOL1__";
const char * endpoints = CacheHelper::getTcrEndpoints( isLocalServer, 3 );
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, numberOfLocators);

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

void _verifyEntry(const char* name, const char* key, const char* val,
    bool noKey, bool checkLocal = false)
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
      if (regPtr->containsValueForKey(keyPtr)) {
        containsValueCnt++;
      }
      else
        break;
      ASSERT( containsValueCnt < MAX, "Value found in region." );
    }

    if (val != NULL) {
      CacheableStringPtr checkPtr;
      if (checkLocal) {
        RegionEntryPtr entryPtr = regPtr->getEntry(keyPtr);
        checkPtr = dynCast<CacheableStringPtr>(entryPtr->getValue());
      }
      else {
        checkPtr = dynCast<CacheableStringPtr>(regPtr->get(keyPtr));
      }

      ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");
      char buf[1024];
      sprintf( buf, "In verify loop, get returned %s for key %s", checkPtr->asChar(), key );
      LOG( buf );
      if (strcmp(checkPtr->asChar(), value) != 0) {
        testValueCnt++;
      }else{
        break;
      }
      ASSERT( testValueCnt < MAX, "Incorrect value found." );
    }
    dunit::sleep( SLEEP );
  }
}

#define verifyEntry(a,b,c,d) _verifyEntry(a, b, c, d, __LINE__)

void _verifyEntry(const char* name, const char* key, const char* val,
    bool checkLocal, int line)
{
  char logmsg[1024];
  sprintf( logmsg, "verifyEntry() called from %d.\n", line );
  LOG( logmsg );
  _verifyEntry(name, key, val, false, checkLocal);
  LOG( "Entry verified." );
}

void createRegion(const char* name, bool ackMode, const char* endpoints,
    bool clientNotificationEnabled = false, bool caching = true)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, caching,
      NULLPTR, endpoints,clientNotificationEnabled);
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

  verifyEntry(name, key, value, false);
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

  regPtr->put( keyPtr, valPtr );
  LOG( "Put entry." );

  verifyEntry(name, key, value, false);
  LOG( "Entry updated." );
}

void doNetsearch( const char * name, const char * key, const char * value )
{
  LOG( "doNetsearch() entered." );
  static int count = 0;
  fprintf( stdout, "Netsearching for entry -- key: %s  expecting value: %s in region %s\n", key, value, name );
  fflush( stdout );
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create( key );

  RegionPtr regPtr = getHelper()->getRegion( name );
  fprintf( stdout, "netsearch  region %s\n", regPtr->getName() );
  fflush( stdout );
  ASSERT(regPtr != NULLPTR, "Region not found.");

  if (count == 0) {
    ASSERT( !regPtr->containsKey( keyPtr ), "Key should not have been found in region." );
    ASSERT( !regPtr->containsValueForKey( keyPtr ), "Value should not have been found in region." );
    count++;
  }
  CacheableStringPtr checkPtr;
  try {
    checkPtr = dynCast<CacheableStringPtr>( regPtr->get( keyPtr) ); // force a netsearch
  } catch( Exception & e ) {
    LOG( "Caught exception during netsearch." );
    LOG( e.getMessage() );
  }
  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
      char buf[1024];
      sprintf( buf, "In net search, get returned %s for key %s", checkPtr->asChar(), key );
      LOG( buf );
  }else{
    LOG("checkPtr is NULL");
  }
  verifyEntry(name, key, value, false);
  LOG( "Netsearch complete." );
}

std::vector<char *> storeEndPoints(const char *endPoints)
{
 std::vector<char *> endpointNames;
 if (endPoints != NULL)
 {
   char * ep = strdup(endPoints);
   char *token = strtok(ep, ",");
   while(token)
   {
     endpointNames.push_back(token);
     token = strtok(NULL, ",");
   }
 }
 ASSERT( endpointNames.size() == 3, "There should be 3 end points" );
 return endpointNames;
}

std::vector<char *> endpointNames = storeEndPoints( endpoints );

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

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2And3)
{
  if ( isLocalServer ) CacheHelper::initServer( 2 );
  if ( isLocalServer ) CacheHelper::initServer( 3 );
  LOG("SERVER23 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2And3_Locator)
{
  if ( isLocalServer ) CacheHelper::initServer( 2, NULL, locatorsG );
  if ( isLocalServer ) CacheHelper::initServer( 3, NULL, locatorsG);
  LOG("SERVER23 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  char endPoints[1024];
  sprintf( endPoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  LOG(endPoints);
  initClient(true);
  createRegion( regionNames[0], USE_ACK, endPoints);
  createRegion( regionNames[1], NO_ACK, endPoints);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
{
  char endPoints[1024];
  sprintf( endPoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  LOG(endPoints);
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG, poolName );
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, poolName);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndPoint)
{
  char endPoints[1024];
  sprintf( endPoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  LOG(endPoints);
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL, poolName);
  createPooledRegion( regionNames[1], NO_ACK, endPoints, NULL, poolName);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
{
  char endPoints[1024];
  sprintf( endPoints, "%s,%s", endpointNames.at(1),endpointNames.at(2));
  LOG(endPoints);
  initClient(true);
  createRegion( regionNames[0], USE_ACK, endPoints);
  createRegion( regionNames[1], NO_ACK, endPoints);
  LOG( "StepTwo complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
{
  char endPoints[1024];
  sprintf( endPoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  LOG(endPoints);
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG, poolName );
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, poolName);
  LOG( "StepOne complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_EndPoint)
{
  char endPoints[1024];
  sprintf( endPoints, "%s,%s", endpointNames.at(0),endpointNames.at(1));
  LOG(endPoints);
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL, poolName);
  createPooledRegion( regionNames[1], NO_ACK, endPoints, NULL, poolName);
  LOG( "StepOne complete." );
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

// Test for getAll
DUNIT_TASK_DEFINITION(CLIENT1, StepSeven)
{
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);

  VectorOfCacheableKey keys0;
  CacheableKeyPtr key0 = CacheableString::create(keys[0]);
  CacheableKeyPtr key1 = CacheableString::create(keys[1]);

  // test invalid combination with caching disabled for getAll
  reg0->localDestroyRegion();
  reg0 = NULLPTR;
  createRegion(regionNames[0], USE_ACK, endpoints, false, false);
  reg0 = getHelper()->getRegion(regionNames[0]);
  keys0.push_back(key0);
  keys0.push_back(key1);
  try {
    reg0->getAll(keys0, NULLPTR, NULLPTR, true);
    FAIL("Expected IllegalArgumentException");
  }
  catch (const IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException");
  }
  // re-create region with caching enabled
  reg0->localDestroyRegion();
  reg0 = NULLPTR;
  createRegion(regionNames[0], USE_ACK, endpoints);
  reg0 = getHelper()->getRegion(regionNames[0]);
  // check for IllegalArgumentException for empty key list
  HashMapOfCacheablePtr values(new HashMapOfCacheable());
  HashMapOfExceptionPtr exceptions(new HashMapOfException());
  keys0.clear();
  try {
    reg0->getAll(keys0, values, exceptions);
    FAIL("Expected IllegalArgumentException");
  }
  catch (const IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException");
  }
  keys0.push_back(key0);
  keys0.push_back(key1);
  try {
    reg0->getAll(keys0, NULLPTR, NULLPTR, false);
    FAIL("Expected IllegalArgumentException");
  }
  catch (const IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException");
  }

  reg0->getAll(keys0, values, exceptions);
  ASSERT(values->size() == 2, "Expected 2 values");
  ASSERT(exceptions->size() == 0, "Expected no exceptions");
  CacheableStringPtr val0 = dynCast<CacheableStringPtr>(values->operator[](key0));
  CacheableStringPtr val1 = dynCast<CacheableStringPtr>(values->operator[](key1));
  ASSERT(strcmp(nvals[0], val0->asChar()) == 0, "Got unexpected value");
  ASSERT(strcmp(nvals[1], val1->asChar()) == 0, "Got unexpected value");

  // for second region invalidate only one key to have a partial get
  // from java server
  RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
  CacheableKeyPtr key2 = CacheableString::create(keys[2]);
  CacheableKeyPtr key3 = CacheableString::create(keys[3]);
  reg1->localInvalidate(key2);
  VectorOfCacheableKey keys1;
  keys1.push_back(key2);
  keys1.push_back(key3);

  values->clear();
  exceptions->clear();
  reg1->getAll(keys1, values, exceptions, true);
  ASSERT(values->size() == 2, "Expected 2 values");
  ASSERT(exceptions->size() == 0, "Expected no exceptions");
  CacheableStringPtr val2 = dynCast<CacheableStringPtr>(values->operator[](key2));
  CacheableStringPtr val3 = dynCast<CacheableStringPtr>(values->operator[](key3));
  ASSERT(strcmp(nvals[2], val2->asChar()) == 0, "Got unexpected value");
  ASSERT(strcmp(vals[3], val3->asChar()) == 0, "Got unexpected value");

  // also check that the region is properly populated
  ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
  VectorOfRegionEntry regEntries;
  reg1->entries(regEntries, false);
  ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
  verifyEntry(regionNames[1], keys[2], nvals[2], true);
  verifyEntry(regionNames[1], keys[3], vals[3], true);

  // also check with NULL values that region is properly populated
  reg1->localInvalidate(key3);
  values = NULLPTR;
  exceptions->clear();
  reg1->getAll(keys1, values, exceptions, true);
  // now check that the region is properly populated
  ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
  regEntries.clear();
  reg1->entries(regEntries, false);
  ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
  verifyEntry(regionNames[1], keys[2], nvals[2], true);
  verifyEntry(regionNames[1], keys[3], nvals[3], true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven_Pool)
{
  RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);

  VectorOfCacheableKey keys0;
  CacheableKeyPtr key0 = CacheableString::create(keys[0]);
  CacheableKeyPtr key1 = CacheableString::create(keys[1]);

  // test invalid combination with caching disabled for getAll
  reg0->localDestroyRegion();
  reg0 = NULLPTR;
  getHelper()->createRegionAndAttachPool( regionNames[0], USE_ACK, poolName, false);
  //createRegion(regionNames[0], USE_ACK, endpoints, false, false);
  reg0 = getHelper()->getRegion(regionNames[0]);
  keys0.push_back(key0);
  keys0.push_back(key1);
  try {
    reg0->getAll(keys0, NULLPTR, NULLPTR, true);
    FAIL("Expected IllegalArgumentException");
  }
  catch (const IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException");
  }
  // re-create region with caching enabled
  reg0->localDestroyRegion();
  reg0 = NULLPTR;
  getHelper()->createRegionAndAttachPool( regionNames[0], USE_ACK, poolName);
 // createRegion(regionNames[0], USE_ACK, endpoints);
  reg0 = getHelper()->getRegion(regionNames[0]);
  // check for IllegalArgumentException for empty key list
  HashMapOfCacheablePtr values(new HashMapOfCacheable());
  HashMapOfExceptionPtr exceptions(new HashMapOfException());
  keys0.clear();
  try {
    reg0->getAll(keys0, values, exceptions);
    FAIL("Expected IllegalArgumentException");
  }
  catch (const IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException");
  }
  keys0.push_back(key0);
  keys0.push_back(key1);
  try {
    reg0->getAll(keys0, NULLPTR, NULLPTR, false);
    FAIL("Expected IllegalArgumentException");
  }
  catch (const IllegalArgumentException&) {
    LOG("Got expected IllegalArgumentException");
  }

  reg0->getAll(keys0, values, exceptions);
  ASSERT(values->size() == 2, "Expected 2 values");
  ASSERT(exceptions->size() == 0, "Expected no exceptions");
  CacheableStringPtr val0 = dynCast<CacheableStringPtr>(values->operator[](key0));
  CacheableStringPtr val1 = dynCast<CacheableStringPtr>(values->operator[](key1));
  ASSERT(strcmp(nvals[0], val0->asChar()) == 0, "Got unexpected value");
  ASSERT(strcmp(nvals[1], val1->asChar()) == 0, "Got unexpected value");

  // for second region invalidate only one key to have a partial get
  // from java server
  RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
  CacheableKeyPtr key2 = CacheableString::create(keys[2]);
  CacheableKeyPtr key3 = CacheableString::create(keys[3]);
  reg1->localInvalidate(key2);
  VectorOfCacheableKey keys1;
  keys1.push_back(key2);
  keys1.push_back(key3);

  values->clear();
  exceptions->clear();
  reg1->getAll(keys1, values, exceptions, true);
  ASSERT(values->size() == 2, "Expected 2 values");
  ASSERT(exceptions->size() == 0, "Expected no exceptions");
  CacheableStringPtr val2 = dynCast<CacheableStringPtr>(values->operator[](key2));
  CacheableStringPtr val3 = dynCast<CacheableStringPtr>(values->operator[](key3));
  ASSERT(strcmp(nvals[2], val2->asChar()) == 0, "Got unexpected value");
  ASSERT(strcmp(vals[3], val3->asChar()) == 0, "Got unexpected value");

  // also check that the region is properly populated
  ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
  VectorOfRegionEntry regEntries;
  reg1->entries(regEntries, false);
  ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
  verifyEntry(regionNames[1], keys[2], nvals[2], true);
  verifyEntry(regionNames[1], keys[3], vals[3], true);

  // also check with NULL values that region is properly populated
  reg1->localInvalidate(key3);
  values = NULLPTR;
  exceptions->clear();
  reg1->getAll(keys1, values, exceptions, true);
  // now check that the region is properly populated
  ASSERT(reg1->size() == 2, "Expected 2 entries in the region");
  regEntries.clear();
  reg1->entries(regEntries, false);
  ASSERT(regEntries.size() == 2, "Expected 2 entries in the region.entries");
  verifyEntry(regionNames[1], keys[2], nvals[2], true);
  verifyEntry(regionNames[1], keys[3], nvals[3], true);
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
  if ( isLocalServer ) {
    CacheHelper::closeServer( 3 );
    LOG("SERVER3 stopped");
  }
}
END_TASK_DEFINITION

void runDistOps2( bool poolConfig = true, bool isLocator = true ) {

  if( poolConfig && isLocator )
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator)
    CALL_TASK(CreateServer2And3_Locator)
  }
  else
  {
    CALL_TASK( CreateServer1 );
    CALL_TASK( CreateServer2And3 );
  }
   if(!poolConfig)
   {
     CALL_TASK(StepOne);
     CALL_TASK(StepTwo);
   }
   else if(isLocator)
   {
     CALL_TASK( StepOne_Pooled_Locator );
     CALL_TASK( StepTwo_Pooled_Locator );
   }
   else
   {
     CALL_TASK( StepOne_Pooled_EndPoint );
     CALL_TASK( StepTwo_Pooled_EndPoint );
   }
   CALL_TASK(StepThree);
   CALL_TASK(StepFour);
   CALL_TASK(StepFive);
   CALL_TASK(StepSix);
   if( poolConfig )
  {
    CALL_TASK( StepSeven_Pool );
  }
  else
  {
    CALL_TASK( StepSeven );
  }
  CALL_TASK( CloseCache1 );
	CALL_TASK( CloseCache2 );
	CALL_TASK( CloseServer1 );
  CALL_TASK( CloseServer2 );
  if( poolConfig && isLocator )
  {
    CALL_TASK(CloseLocator1);
  }
}
#endif /* THINCLIENTDISTOPS2_HPP_ */

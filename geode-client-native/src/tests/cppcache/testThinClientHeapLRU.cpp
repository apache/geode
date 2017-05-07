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
#include "BuiltinCacheableWrappers.hpp"

#include <string>

#define ROOT_NAME "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;
const char * endPoints = CacheHelper::getTcrEndpoints( isLocalServer, 1 );

const char * keys[] = { "Key-1", "Key-2", "Key-3", "Key-4" };
const char * vals[] = { "Value-1", "Value-2", "Value-3", "Value-4" };
const char * nvals[] = { "New Value-1", "New Value-2", "New Value-3", "New Value-4" };

const char * regionNames[] = { "DistRegionAck", "DistRegionNoAck" };

const bool USE_ACK = true;
const bool NO_ACK = false;

void initThinClientWithClientTypeAsCLIENT( const bool isthinClient )
{
  if ( cacheHelper == NULL ) {
    PropertiesPtr pp = Properties::create();
    pp->insert("heap-lru-limit", 1);
    pp->insert("heap-lru-delta", 10);

    cacheHelper = new CacheHelper(isthinClient, pp);
  }
  ASSERT( cacheHelper, "Failed to create a CacheHelper client instance." );
}


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

void createRegion( const char * name, bool ackMode, const char * endpoints ,bool clientNotificationEnabled = false)
{
  LOG( "createRegion() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createRegion( name, ackMode, true,
      NULLPTR, endpoints,clientNotificationEnabled );
  ASSERT( regPtr != NULLPTR, "Failed to create region." );
  LOG( "Region created." );
}

void createOnekEntries() {
  CacheableHelper::registerBuiltins();
  RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);
  for(int i =0; i<2048; i++)
  {
    CacheableWrapper * tmpkey = CacheableWrapperFactory::createInstance(GemfireTypeIds::CacheableInt32);
    CacheableWrapper * tmpval = CacheableWrapperFactory::createInstance(GemfireTypeIds::CacheableBytes);
    tmpkey->initKey(i, 32);
    tmpval->initRandomValue(1024);
    ASSERT(tmpkey->getCacheable() != NULLPTR, "tmpkey->getCacheable() is NULL");
    ASSERT(tmpval->getCacheable() != NULLPTR, "tmpval->getCacheable() is NULL");
    dataReg->put( dynCast<CacheableKeyPtr>( tmpkey->getCacheable() ), tmpval->getCacheable());
  // delete tmpkey;
  //  delete tmpval;
  }
  dunit::sleep(10000);
  VectorOfRegionEntry me;
  dataReg->entries(me,false);
  LOG("Verifying size outside loop");
  char buf[1024];
  sprintf( buf, "region size is %d", me.size());
  LOG( buf );

  ASSERT(me.size() <= 1024, "Should have evicted anything over 1024 entries");

}

DUNIT_TASK(SERVER1, CreateServer1)
{
  if ( isLocalServer ) CacheHelper::initServer( 1 );
  LOG("SERVER1 started");
}
END_TASK(CreateServer1)

DUNIT_TASK(CLIENT1, StepOne)
{
  initThinClientWithClientTypeAsCLIENT(true);
  //initClient(true);
  createRegion( regionNames[0], USE_ACK, endPoints);
  createRegion( regionNames[1], NO_ACK, endPoints);
  LOG( "StepOne complete." );
}
END_TASK(StepOne)

DUNIT_TASK(CLIENT2, StepTwo)
{
  initThinClientWithClientTypeAsCLIENT(true);
//  initClient(true);
  createRegion( regionNames[0], USE_ACK, endPoints);
  createRegion( regionNames[1], NO_ACK, endPoints);
  LOG( "StepTwo complete." );
}
END_TASK(StepTwo)

DUNIT_TASK(CLIENT1, StepThree)
{
  //verfy that eviction works
  createOnekEntries();
  LOG( "StepThree complete." );
}
END_TASK(StepThree)

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

DUNIT_TASK(SERVER1,CloseServer1)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK(CloseServer1)




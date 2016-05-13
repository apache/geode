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
#include <ace/Task.h>
#include <string>

#define ROOT_NAME "TestThinClientCacheableStringArray"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include "Query.hpp"
#include "QueryService.hpp"

using namespace gemfire;
using namespace test;
using namespace testobject;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;
const char * endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 1);

void initClient( const bool isthinClient )
{
  Serializable::registerType( Position::createDeserializable);
  Serializable::registerType( Portfolio::createDeserializable);

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
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true,
      NULLPTR, endpoints,clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG( "Region created." );
}

const char * regionNames[] = { "Portfolios", "Positions" };

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK(SERVER1, CreateServer1)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "remotequery.xml");
  LOG("SERVER1 started");

}
END_TASK(CreateServer1)

DUNIT_TASK(CLIENT1, StepOne)
{
  initClient(true);

  createRegion( regionNames[0], USE_ACK, endPoints, true);

  RegionPtr regptr = getHelper()->getRegion(regionNames[0]);
  RegionAttributesPtr lattribPtr = regptr->getAttributes();
  RegionPtr subregPtr = regptr->createSubregion( regionNames[1], lattribPtr );

  QueryHelper * qh = &QueryHelper::getHelper();
  CacheableStringPtr cstr[4] = {
    CacheableStringPtr(CacheableString::create((const char*)"Taaa",4)),
    CacheableStringPtr(CacheableString::create((const char*)"Tbbb",4)),
    CacheableStringPtr(CacheableString::create((const char*)"Tccc",4)),
    CacheableStringPtr(CacheableString::create((const char*)"Tddd",4))};
  CacheableStringArrayPtr nm = CacheableStringArray::create(cstr, 4);
  qh->populatePortfolioData(regptr  , 4, 3, 2, nm);
  qh->populatePositionData(subregPtr, 4, 3);

  LOG( "StepOne complete." );
}
END_TASK(StepOne)

DUNIT_TASK(CLIENT1, StepThree)
{
try
{

  QueryServicePtr qs = getHelper()->cachePtr->getQueryService();

  char* qryStr = (char*)"select * from /Portfolios p where p.ID < 3";
  QueryPtr qry = qs->newQuery(qryStr);
  SelectResultsPtr results;
  results = qry->execute();

  SelectResultsIterator iter = results->getIterator();
  char buf[100];
  int count = results->size();
  sprintf(buf, "results size=%d", count);
  LOG(buf);
  while( iter.hasNext())
    {
        count--;
        SerializablePtr ser = iter.next();
        PortfolioPtr portfolio( dynamic_cast<Portfolio*> (ser.ptr() ));
        PositionPtr  position(dynamic_cast<Position*>  (ser.ptr() ));

        if (portfolio != NULLPTR) {
          printf("   query pulled portfolio object ID %d, pkid %s\n",
              portfolio->getID(), portfolio->getPkid()->asChar());
        }

        else if (position != NULLPTR) {
          printf("   query  pulled position object secId %s, shares %d\n",
              position->getSecId()->asChar(), position->getSharesOutstanding());
        }

        else {
          if (ser != NULLPTR) {
            printf (" query pulled object %s\n", ser->toString()->asChar());
          }
          else {
            printf("   query pulled bad object\n");
          }
        }

  }
  sprintf(buf, "results last count=%d", count);
  LOG(buf);
}
catch(IllegalStateException & ise)
{
  char isemsg[500] = {0};
  ACE_OS::snprintf(isemsg, 499, "IllegalStateException: %s", ise.getMessage());
  LOG(isemsg);
  FAIL(isemsg);
}
catch(Exception & excp)
{
  char excpmsg[500] = {0};
  ACE_OS::snprintf(excpmsg, 499, "Exception: %s", excp.getMessage());
  LOG(excpmsg);
  FAIL(excpmsg);
}
catch(...)
{
  LOG("Got an exception!");
  FAIL("Got an exception!");
}

  LOG( "StepThree complete." );
}
END_TASK(StepThree)


DUNIT_TASK(CLIENT1,CloseCache1)
{
  LOG("cleanProc 1...");
  cleanProc();
}
END_TASK(CloseCache1)

DUNIT_TASK(SERVER1,CloseServer1)
{
  LOG("closing Server1...");
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK(CloseServer1)

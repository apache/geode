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
#include "ThinClientHelper.hpp"

#include "QueryStrings.hpp"
#include "QueryHelper.hpp"

#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>

using namespace gemfire;
using namespace test;
using namespace testobject;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

static int numberOfLocators = 1;
bool isLocalServer = true;
bool isLocator = true;
const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

const char* _regionNames[] = {"Portfolios", "Positions"};

DUNIT_TASK(SERVER1, CreateServer1)
  {
    LOG("Starting SERVER1...");
    CacheHelper::initLocator(1);
    if (isLocalServer) {
      CacheHelper::initServer(1, "remotequery.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK(CreateServer1)

DUNIT_TASK(CLIENT1, StepOne)
  {
    Serializable::registerType(Position::createDeserializable);
    Serializable::registerType(Portfolio::createDeserializable);

    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    RegionPtr regptr = getHelper()->createPooledRegion(
        _regionNames[0], USE_ACK, locHostPort, "__TEST_POOL1__", true, true);
    RegionAttributesPtr lattribPtr = regptr->getAttributes();
    RegionPtr subregPtr = regptr->createSubregion(_regionNames[1], lattribPtr);

    QueryHelper* qh = &QueryHelper::getHelper();
    CacheableStringPtr cstr[4] = {
        CacheableStringPtr(CacheableString::create((const char*)"Taaa", 4)),
        CacheableStringPtr(CacheableString::create((const char*)"Tbbb", 4)),
        CacheableStringPtr(CacheableString::create((const char*)"Tccc", 4)),
        CacheableStringPtr(CacheableString::create((const char*)"Tddd", 4))};
    CacheableStringArrayPtr nm = CacheableStringArray::create(cstr, 4);
    qh->populatePortfolioData(regptr, 4, 3, 2, nm);
    qh->populatePositionData(subregPtr, 4, 3);

    LOG("StepOne complete.");
  }
END_TASK(StepOne)

DUNIT_TASK(CLIENT1, StepThree)
  {
    try {
      QueryServicePtr qs =
          getHelper()->cachePtr->getQueryService("__TEST_POOL1__");

      char* qryStr = (char*)"select * from /Portfolios p where p.ID < 3";
      QueryPtr qry = qs->newQuery(qryStr);
      SelectResultsPtr results;
      results = qry->execute();

      SelectResultsIterator iter = results->getIterator();
      char buf[100];
      int count = results->size();
      sprintf(buf, "results size=%d", count);
      LOG(buf);
      while (iter.hasNext()) {
        count--;
        SerializablePtr ser = iter.next();
        PortfolioPtr portfolio(dynamic_cast<Portfolio*>(ser.ptr()));
        PositionPtr position(dynamic_cast<Position*>(ser.ptr()));

        if (portfolio != NULLPTR) {
          printf("   query pulled portfolio object ID %d, pkid %s\n",
                 portfolio->getID(), portfolio->getPkid()->asChar());
        }

        else if (position != NULLPTR) {
          printf("   query  pulled position object secId %s, shares %d\n",
                 position->getSecId()->asChar(),
                 position->getSharesOutstanding());
        }

        else {
          if (ser != NULLPTR) {
            printf(" query pulled object %s\n", ser->toString()->asChar());
          } else {
            printf("   query pulled bad object\n");
          }
        }
      }
      sprintf(buf, "results last count=%d", count);
      LOG(buf);
    } catch (IllegalStateException& ise) {
      char isemsg[500] = {0};
      ACE_OS::snprintf(isemsg, 499, "IllegalStateException: %s",
                       ise.getMessage());
      LOG(isemsg);
      FAIL(isemsg);
    } catch (Exception& excp) {
      char excpmsg[500] = {0};
      ACE_OS::snprintf(excpmsg, 499, "Exception: %s", excp.getMessage());
      LOG(excpmsg);
      FAIL(excpmsg);
    } catch (...) {
      LOG("Got an exception!");
      FAIL("Got an exception!");
    }

    LOG("StepThree complete.");
  }
END_TASK(StepThree)

DUNIT_TASK(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    cleanProc();
  }
END_TASK(CloseCache1)

DUNIT_TASK(SERVER1, CloseServer1)
  {
    LOG("closing Server1...");
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK(CloseServer1)

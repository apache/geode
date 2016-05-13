/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * testThinClientPdxSerializer.cpp
 *
 *  Created on: Apr 19, 2012
 *      Author: vrao
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#include "ThinClientHelper.hpp"
//#include "QueryStrings.hpp"
//#include "QueryHelper.hpp"
#include "impl/Utils.hpp"
//#include "Query.hpp"
//#include "QueryService.hpp"
#include "testobject/PdxClassV1.hpp"
#include "testobject/PdxClassV2.hpp"
#include "testobject/NonPdxType.hpp"
#include "ThinClientPdxSerializers.hpp"

using namespace gemfire;
using namespace test;
using namespace PdxTests;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATOR s2p2
#define SERVER1 s2p1

bool isLocator = false;
bool isLocalServer = false;
const char * endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 1);
const char * poolNames[] = { "Pool1","Pool2","Pool3" };
const char * locHostPort = CacheHelper::getLocatorHostPort( isLocator, 1 );
bool isPoolConfig = false;    // To track if pool case is running
//const char * qRegionNames[] = { "Portfolios", "Positions", "Portfolios2", "Portfolios3" };
static bool m_useWeakHashMap = false;

void initClient( const bool isthinClient, bool  isPdxIgnoreUnreadFields)
{
  LOGINFO("initClient: isPdxIgnoreUnreadFields = %d ", isPdxIgnoreUnreadFields);
  if ( cacheHelper == NULL ) {
    cacheHelper = new CacheHelper(isthinClient, isPdxIgnoreUnreadFields, false, NULLPTR, false );
  }
  ASSERT( cacheHelper, "Failed to create a CacheHelper client instance." );
}

void stepOne(bool pool = false, bool locator = false, bool  isPdxIgnoreUnreadFields = false )
{
  //Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  if(!pool){
    createRegion( "DistRegionAck", USE_ACK, endPoints, false /*Caching disabled*/);
  }else if(locator){
    isPoolConfig = true;
    createPool(poolNames[0], locHostPort, NULL, NULL, 0, true );
    createRegionAndAttachPool( "DistRegionAck", USE_ACK, poolNames[0], false /*Caching disabled*/);
  }else {
    isPoolConfig = true;
    createPool(poolNames[0], NULL, NULL,endPoints, 0, true );
    createRegionAndAttachPool( "DistRegionAck", USE_ACK, poolNames[0], false);
  }
  LOG( "StepOne complete." );
}

DUNIT_TASK_DEFINITION(LOCATOR, StartLocator)
{
  //starting locator 1 2
  if ( isLocator ) {
    CacheHelper::initLocator( 1 );
  }
  LOG("Locator started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc_PDX)
{
  LOG("Starting Step One with Pool + Locator lists");
  stepOne(true, true, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
{
  LOG("Starting Step One with Pool + Locator lists");
  stepOne(true, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserverPdx.xml" );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserverPdx.xml",locHostPort );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserver.xml" );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator1)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserver.xml",locHostPort );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer2)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserverForPdx.xml" );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer3)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserverPdxSerializer.xml" );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator2)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserverForPdx.xml",locHostPort );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator3)
{
  LOG("Starting SERVER1...");
  if ( isLocalServer ) CacheHelper::initServer( 1, "cacheserverPdxSerializer.xml",locHostPort );
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  LOG("Starting Step One");
  stepOne();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolEP)
{
  LOG("Starting Step One with Pool + Explicit server list");
  stepOne(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolEP_PDX)
{
  LOG("Starting Step One with Pool + Explicit server list");
  stepOne(true, false, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolEP)
{
  LOG("Starting Step One with Pool + Explicit server list");
  stepOne(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolEP_PDX)
{
  LOG("Starting Step One with Pool + Explicit server list");
  stepOne(true, false, true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc)
{
  LOG("Starting Step Two with Pool + Locator");
  stepOne(true);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc_PDX)
{
  LOG("Starting Step Two with Pool + Locator");
  stepOne(true, false, true);
}
END_TASK_DEFINITION

void checkPdxInstanceToStringAtServer(RegionPtr regionPtr) {
	CacheableKeyPtr keyport = CacheableKey::create("success");
	CacheableBooleanPtr boolPtr = dynCast<CacheableBooleanPtr>(regionPtr->get(keyport));
	bool val = boolPtr->value();
	ASSERT(val==true, "checkPdxInstanceToStringAtServer: Val should be true");
}

DUNIT_TASK_DEFINITION(CLIENT1, JavaPutGet)
{
  Serializable::registerPdxSerializer(PdxSerializerPtr(new TestPdxSerializer));

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  
  PdxTests::NonPdxType * npt1 = new PdxTests::NonPdxType;
  PdxWrapperPtr pdxobj(new PdxWrapper(npt1, CLASSNAME1));
  regPtr0->put(keyport, pdxobj);

  PdxWrapperPtr obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(keyport));

  CacheableBooleanPtr boolPtr = dynCast<CacheableBooleanPtr>(regPtr0->get("success"));
  bool isEqual = boolPtr.ptr()->value();
  ASSERT(isEqual == true, "Task JavaPutGet:Objects of type NonPdxType should be equal");
  
  PdxTests::NonPdxType * npt2 = reinterpret_cast<PdxTests::NonPdxType*>(obj2->getObject());
  ASSERT(npt1->equals(*npt2, false), "NonPdxType compare");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, JavaGet)
{
  Serializable::registerPdxSerializer(PdxSerializerPtr(new TestPdxSerializer));

  LOGDEBUG("JavaGet-1 Line_309");
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport1 = CacheableKey::create(1);
  LOGDEBUG("JavaGet-2 Line_314");
  PdxWrapperPtr  obj1 = dynCast<PdxWrapperPtr>(regPtr0->get(keyport1));
  PdxTests::NonPdxType * npt1 = reinterpret_cast<PdxTests::NonPdxType*>(obj1->getObject());
  LOGDEBUG("JavaGet-3 Line_316");
  CacheableKeyPtr keyport2 = CacheableKey::create("putFromjava");
  LOGDEBUG("JavaGet-4 Line_316");
  PdxWrapperPtr  obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(keyport2));
  PdxTests::NonPdxType * npt2 = reinterpret_cast<PdxTests::NonPdxType*>(obj2->getObject());
  LOGDEBUG("JavaGet-5 Line_320");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putFromVersion1_PS)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  CacheableKeyPtr key = CacheableKey::create(1);

  PdxTests::TestDiffTypePdxSV1 * npt1 = new PdxTests::TestDiffTypePdxSV1(false);
  Serializable::registerPdxSerializer(PdxSerializerPtr(new TestPdxSerializerForV1));

  //Create New object and wrap it in PdxWrapper
  npt1 = new PdxTests::TestDiffTypePdxSV1(true);
  PdxWrapperPtr pdxobj(new PdxWrapper(npt1, V1CLASSNAME2));

  //PUT
  regPtr0->put(key, pdxobj);

  //GET
  PdxWrapperPtr obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(key));
  PdxTests::TestDiffTypePdxSV1 * npt2 = reinterpret_cast<PdxTests::TestDiffTypePdxSV1*>(obj2->getObject());

  //Equal check
  bool isEqual = npt1->equals(npt2);
  LOGDEBUG("putFromVersion1_PS isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Task putFromVersion1_PS:Objects of type TestPdxSerializerForV1 should be equal");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, putFromVersion2_PS)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  CacheableKeyPtr key = CacheableKey::create(1);

  PdxTests::TestDiffTypePdxSV2 * npt1 = new PdxTests::TestDiffTypePdxSV2(false);
  Serializable::registerPdxSerializer(PdxSerializerPtr(new TestPdxSerializerForV2));

  //New object
  npt1 = new PdxTests::TestDiffTypePdxSV2(true);
  PdxWrapperPtr pdxobj(new PdxWrapper(npt1, V2CLASSNAME4));

  //PUT
  regPtr0->put(key, pdxobj);

  //GET
  PdxWrapperPtr obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(key));
  PdxTests::TestDiffTypePdxSV2 * npt2 = reinterpret_cast<PdxTests::TestDiffTypePdxSV2*>(obj2->getObject());

  //Equal check
  bool isEqual = npt1->equals(npt2);
  LOGDEBUG("putFromVersion2_PS isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Task putFromVersion2_PS:Objects of type TestPdxSerializerForV2 should be equal");

  CacheableKeyPtr key2 = CacheableKey::create(2);
  regPtr0->put(key2, pdxobj);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getputFromVersion1_PS)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  CacheableKeyPtr key = CacheableKey::create(1);

  //GET
  PdxWrapperPtr obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(key));
  PdxTests::TestDiffTypePdxSV1 * npt2 = reinterpret_cast<PdxTests::TestDiffTypePdxSV1*>(obj2->getObject());

  //Create New object and Compare
  PdxTests::TestDiffTypePdxSV1 * npt1 = new PdxTests::TestDiffTypePdxSV1(true);
  bool isEqual = npt1->equals(npt2);
  LOGDEBUG("getputFromVersion1_PS-1 isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Task getputFromVersion1_PS:Objects of type TestPdxSerializerForV1 should be equal");

  //PUT
  regPtr0->put(key, obj2);

  CacheableKeyPtr key2 = CacheableKey::create(2);
  obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(key2));
  PdxTests::TestDiffTypePdxSV1 * pRet = reinterpret_cast<PdxTests::TestDiffTypePdxSV1*>(obj2->getObject());
  isEqual = npt1->equals(pRet);
  LOGDEBUG("getputFromVersion1_PS-2 isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Task getputFromVersion1_PS:Objects of type TestPdxSerializerForV1 should be equal");

  //Get then Put.. this should Not merge data back
  PdxWrapperPtr pdxobj = PdxWrapperPtr(new PdxWrapper(npt1, V1CLASSNAME2));
  regPtr0->put(key2, pdxobj);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getAtVersion2_PS)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  CacheableKeyPtr key = CacheableKey::create(1);

  //New object
  PdxTests::TestDiffTypePdxSV2* np = new PdxTests::TestDiffTypePdxSV2(true);

  //GET
  PdxWrapperPtr obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(key));
  PdxTests::TestDiffTypePdxSV2* pRet = reinterpret_cast<PdxTests::TestDiffTypePdxSV2*>(obj2->getObject());

  bool isEqual = np->equals(pRet);
  LOGDEBUG("getAtVersion2_PS-1 isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Task getAtVersion2_PS:Objects of type TestPdxSerializerForV2 should be equal");

  CacheableKeyPtr key2 = CacheableKey::create(2);
  np = new PdxTests::TestDiffTypePdxSV2(true);

  obj2 = dynCast<PdxWrapperPtr>(regPtr0->get(key2));
  pRet = reinterpret_cast<PdxTests::TestDiffTypePdxSV2*>(obj2->getObject());
  isEqual = np->equals(pRet);

  LOGDEBUG("getAtVersion2_PS-2 isEqual = %d", isEqual);
  ASSERT(isEqual == false, "Task getAtVersion2_PS:Objects of type TestPdxSerializerForV2 should be equal");

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  LOG("cleanProc 1...");
  isPoolConfig = false;
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,CloseCache2)
{
  LOG("cleanProc 2...");
  isPoolConfig = false;
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseServer)
{
  LOG("closing Server1...");
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATOR,CloseLocator)
{
  if ( isLocator ) {
    CacheHelper::closeLocator( 1 );
    LOG("Locator1 stopped");
  }
}
END_TASK_DEFINITION

/***************************************************************/

/*
DUNIT_TASK_DEFINITION(CLIENT2, putAtVersionTwoR21)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypesR2V2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  
  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesR2V2Ptr np(new PdxTypesR2V2());
  
  regPtr0->put(keyport, np );
  
  PdxTypesR2V2Ptr  pRet = dynCast<PdxTypesR2V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("putAtVersionTwoR21:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesR2V2 should be equal at putAtVersionTwoR21");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOneR22)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypesV1R2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesV1R2Ptr np(new PdxTypesV1R2());

  PdxTypesV1R2Ptr  pRet = dynCast<PdxTypesV1R2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("getPutAtVersionOneR22:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesV1R2 should be equal at getPutAtVersionOneR22");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwoR23)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypesR2V2Ptr np(new PdxTypesR2V2());

  PdxTypesR2V2Ptr  pRet = dynCast<PdxTypesR2V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("getPutAtVersionTwoR23:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesR2V2 should be equal at getPutAtVersionTwoR23");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOneR24)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesV1R2Ptr np(new PdxTypesV1R2());

  PdxTypesV1R2Ptr  pRet = dynCast<PdxTypesV1R2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("getPutAtVersionOneR24:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesV1R2 should be equal at getPutAtVersionOneR24");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putAtVersionOne31)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxType3V1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxType3V1Ptr np(new PdxType3V1());

  regPtr0->put(keyport, np );

  PdxType3V1Ptr  pRet = dynCast<PdxType3V1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:putAtVersionOne31: isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType3V1 should be equal at putAtVersionOne31");
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo32)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypes3V2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypes3V2Ptr np(new PdxTypes3V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypes3V2Ptr  pRet = dynCast<PdxTypes3V2Ptr>(regPtr0->get(keyport));
  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getPutAtVersionTwo32.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypes3V2 should be equal at getPutAtVersionTwo32");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne33)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxType3V1Ptr np(new PdxType3V1());

  PdxType3V1Ptr  pRet = dynCast<PdxType3V1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("getPutAtVersionOne33:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType3V1 should be equal at getPutAtVersionOne33");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo34)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  PdxTypes3V2Ptr np(new PdxTypes3V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypes3V2Ptr  pRet = dynCast<PdxTypes3V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getPutAtVersionTwo34: isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType3V1 should be equal at getPutAtVersionTwo34");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putAtVersionOne21)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxType2V1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxType2V1Ptr np(new PdxType2V1());

  regPtr0->put(keyport, np );

  PdxType2V1Ptr  pRet = dynCast<PdxType2V1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:putAtVersionOne21:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType2V1 should be equal at putAtVersionOne21");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo22)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypes2V2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypes2V2Ptr np(new PdxTypes2V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypes2V2Ptr  pRet = dynCast<PdxTypes2V2Ptr>(regPtr0->get(keyport));
  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getPutAtVersionTwo22.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypes2V2 should be equal at getPutAtVersionTwo22");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne23)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  PdxType2V1Ptr np(new PdxType2V1());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxType2V1Ptr  pRet = dynCast<PdxType2V1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getPutAtVersionOne23: isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType2V1 should be equal at getPutAtVersionOne23");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo24)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypes2V2Ptr np(new PdxTypes2V2());
  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypes2V2Ptr  pRet = dynCast<PdxTypes2V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getPutAtVersionTwo24.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypes2V2 should be equal at getPutAtVersionTwo24");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putAtVersionOne11)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxType1V1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTests::PdxType1V1Ptr np(new PdxTests::PdxType1V1());

  regPtr0->put(keyport, np );

  PdxTests::PdxType1V1Ptr  pRet = dynCast<PdxTests::PdxType1V1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:putAtVersionOne11:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType1V1 should be equal at putAtVersionOne11 Line_170");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, putAtVersionTwo1)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypesR1V2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  PdxTests::PdxTypesR1V2::reset(false);

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesR1V2Ptr np(new PdxTypesR1V2());

  regPtr0->put(keyport, np );

  PdxTypesR1V2Ptr  pRet = dynCast<PdxTypesR1V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:putAtVersionTwo1:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesR1V2 should be equal at putAtVersionTwo1");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne2)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypesV1R1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  PdxTypesV1R1Ptr np(new PdxTypesV1R1());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypesV1R1Ptr  pRet = dynCast<PdxTypesV1R1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:getPutAtVersionOne2:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesV1R1 should be equal at getPutAtVersionOne2");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo3)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypesR1V2Ptr np(new PdxTypesR1V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypesR1V2Ptr  pRet = dynCast<PdxTypesR1V2Ptr>(regPtr0->get(keyport));
  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:getPutAtVersionTwo3.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesR1V2 should be equal at getPutAtVersionTwo3");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne4)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypesV1R1Ptr np(new PdxTypesV1R1());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesV1R1Ptr  pRet = dynCast<PdxTypesV1R1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("getPutAtVersionOne4: isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesV1R1 should be equal at getPutAtVersionOne4");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo5)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypesR1V2Ptr np(new PdxTypesR1V2());

  //GET
  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypesR1V2Ptr  pRet = dynCast<PdxTypesR1V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getPutAtVersionTwo5.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesR1V2 should be equal at getPutAtVersionTwo5");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne6)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypesV1R1Ptr np(new PdxTypesV1R1());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesV1R1Ptr  pRet = dynCast<PdxTypesV1R1Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task getPutAtVersionOne6:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesV1R1 should be equal at getPutAtVersionOne6");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, putV2PdxUI)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypesIgnoreUnreadFieldsV2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  //PdxTests::PdxTypesIgnoreUnreadFieldsV2::reset(false);

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypesIgnoreUnreadFieldsV2Ptr np(new PdxTypesIgnoreUnreadFieldsV2());
  CacheableKeyPtr keyport = CacheableKey::create(1);
  regPtr0->put(keyport, np );

  PdxTypesIgnoreUnreadFieldsV2Ptr  pRet = dynCast<PdxTypesIgnoreUnreadFieldsV2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:putV2PdxUI:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesIgnoreUnreadFieldsV2 should be equal at putV2PdxUI ");

  regPtr0->put(keyport, pRet );

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putV1PdxUI)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypesIgnoreUnreadFieldsV1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  //PdxTests::PdxTypesIgnoreUnreadFieldsV1::reset(false);
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypesIgnoreUnreadFieldsV1Ptr  pRet = dynCast<PdxTypesIgnoreUnreadFieldsV1Ptr>(regPtr0->get(keyport));
  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getV2PdxUI)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypesIgnoreUnreadFieldsV2Ptr np(new PdxTypesIgnoreUnreadFieldsV2());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypesIgnoreUnreadFieldsV2Ptr  pRet = dynCast<PdxTypesIgnoreUnreadFieldsV2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("Task:getV2PdxUI:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypesIgnoreUnreadFieldsV2 should be equal at getV2PdxUI ");

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo12)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxTypes1V2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  PdxTypes1V2Ptr np(new PdxTypes1V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxTypes1V2Ptr  pRet = dynCast<PdxTypes1V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:getPutAtVersionTwo12:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType1V2 should be equal at getPutAtVersionTwo12 Line_197");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne13)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxType1V1Ptr np(new PdxType1V1());

  CacheableKeyPtr keyport = CacheableKey::create(1);

  PdxType1V1Ptr  pRet = dynCast<PdxType1V1Ptr>(regPtr0->get(keyport));
  bool isEqual = np->equals(pRet);

  LOGDEBUG("NIL:getPutAtVersionOne13:221.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxType1V2 should be equal at getPutAtVersionOne13 Line_215");

  LOGDEBUG("NIL:getPutAtVersionOne13: PUT remote object -1");
  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo14)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypes1V2Ptr np(new PdxTypes1V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypes1V2Ptr  pRet = dynCast<PdxTypes1V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:getPutAtVersionTwo14:241.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypes1V2 should be equal at getPutAtVersionTwo14 Line_242");

  regPtr0->put(keyport, pRet );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne15)
{
	RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
	PdxType1V1Ptr np(new PdxType1V1());

	//GET
	CacheableKeyPtr keyport = CacheableKey::create(1);

	PdxType1V1Ptr  pRet = dynCast<PdxType1V1Ptr>(regPtr0->get(keyport));

	bool isEqual = np->equals(pRet);
	LOGDEBUG("NIL:getPutAtVersionOne15:784.. isEqual = %d", isEqual);
	ASSERT(isEqual == true, "Objects of type PdxType1V2 should be equal at getPutAtVersionOne15 Line_272");

	regPtr0->put(keyport, pRet );
    LOGDEBUG("NIL:getPutAtVersionOne15 m_useWeakHashMap = %d and TestUtils::testNumberOfPreservedData() = %d", m_useWeakHashMap, TestUtils::testNumberOfPreservedData());
	if (m_useWeakHashMap == false)
	{
		ASSERT(TestUtils::testNumberOfPreservedData()== 0, "testNumberOfPreservedData should be zero at Line_288");
	}
	else
	{
		ASSERT(TestUtils::testNumberOfPreservedData() > 0, "testNumberOfPreservedData should be Greater than zero at Line_292");
	}

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo16)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxTypes1V2Ptr np(new PdxTypes1V2());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTypes1V2Ptr  pRet = dynCast<PdxTypes1V2Ptr>(regPtr0->get(keyport));

  bool isEqual = np->equals(pRet);
  LOGDEBUG("NIL:getPutAtVersionTwo14:.. isEqual = %d", isEqual);
  ASSERT(isEqual == true, "Objects of type PdxTypes1V2 should be equal at getPutAtVersionTwo14");

  regPtr0->put(keyport, pRet );

  if (m_useWeakHashMap == false)
  {
    ASSERT(TestUtils::testNumberOfPreservedData()== 0, "getPutAtVersionTwo16:testNumberOfPreservedData should be zero");
  }
  else
  {
    //it has extra fields, so no need to preserve data
    ASSERT(TestUtils::testNumberOfPreservedData()== 0, "getPutAtVersionTwo16:testNumberOfPreservedData should be zero");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyPdxInGet)
{
  try{
    Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(Address::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);

  CacheablePtr pdxobj(new PdxTests::PdxType());

  regPtr0->put(keyport, pdxobj );

  PdxTests::PdxTypePtr  obj2 = dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));

  checkPdxInstanceToStringAtServer(regPtr0);

  ASSERT(cacheHelper->getCache()->getPdxReadSerialized() == false, "Pdx read serialized property should be false.");

  LOG( "StepThree complete.\n" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyNestedPdxInGet)
{
  LOG( "PutAndVerifyNestedPdxInGet started." );

  try{
	  Serializable::registerPdxType(NestedPdx::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

  try{
	  Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

  try{
	  Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}
  
	NestedPdxPtr p1(new NestedPdx());	
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);

  regPtr0->put(keyport, p1 );

  NestedPdxPtr obj2 = dynCast<NestedPdxPtr>(regPtr0->get(keyport));  

  ASSERT(obj2->equals(p1) == true, "Nested pdx objects should be equal");

  LOG( "PutAndVerifyNestedPdxInGet complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyPdxInGFSInGet)
{

  try{
    Serializable::registerType(PdxInsideIGFSerializable::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  try{
    Serializable::registerPdxType(NestedPdx::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes2::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes3::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes4::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes5::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes6::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes7::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes8::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxInsideIGFSerializablePtr np(new PdxInsideIGFSerializable());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  regPtr0->put(keyport, np );

  //GET
  PdxInsideIGFSerializablePtr pRet = dynCast<PdxInsideIGFSerializablePtr>(regPtr0->get(keyport));
  ASSERT(pRet->equals(np) == true, "TASK PutAndVerifyPdxInIGFSInGet: PdxInsideIGFSerializable objects should be equal");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyPdxInGFSGetOnly)
{
  try{
    Serializable::registerType(PdxInsideIGFSerializable::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  try{
    Serializable::registerPdxType(NestedPdx::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes1::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes2::createDeserializable);
  }catch (const IllegalStateException& ) {
  // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes3::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes4::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes5::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes6::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes7::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
    Serializable::registerPdxType(PdxTypes8::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  PdxInsideIGFSerializablePtr orig(new PdxInsideIGFSerializable());

  //GET
  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxInsideIGFSerializablePtr pRet = dynCast<PdxInsideIGFSerializablePtr>(regPtr0->get(keyport));
  ASSERT(pRet->equals(orig) == true, "TASK:VerifyPdxInIGFSGetOnly, PdxInsideIGFSerializable objects should be equal");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyNestedGetOnly)
{
  LOG( "VerifyNestedGetOnly started." );

 try{
	  Serializable::registerPdxType(NestedPdx::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

  try{
	  Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

  try{
	  Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}
  
	NestedPdxPtr p1(new NestedPdx());	
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);  

  NestedPdxPtr obj2 = dynCast<NestedPdxPtr>(regPtr0->get(keyport));  

  ASSERT(obj2->equals(p1) == true, "Nested pdx objects should be equal");

  LOG( "VerifyNestedGetOnly complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyGetOnly)
{
  try{
	Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  try{
      Serializable::registerPdxType(Address::createDeserializable);
  }catch (const IllegalStateException& ) {
      // ignore exception
  }

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  CacheableKeyPtr keyport = CacheableKey::create(1);
  PdxTests::PdxTypePtr  obj2 = dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));

  checkPdxInstanceToStringAtServer(regPtr0);

  LOG( "StepFour complete.\n" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyVariousPdxTypes)
{
	try{
	  Serializable::registerPdxType(PdxTypes1::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

	try{
	  Serializable::registerPdxType(PdxTypes2::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

	try{
	  Serializable::registerPdxType(PdxTypes3::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}
	try{
	 Serializable::registerPdxType(PdxTypes4::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

	try{
	 Serializable::registerPdxType(PdxTypes5::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

	try{
	  Serializable::registerPdxType(PdxTypes6::createDeserializable);
	}catch (const IllegalStateException& ) {
	 	    // ignore exception
	}

	try{
	  Serializable::registerPdxType(PdxTypes7::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}

	try{
	  Serializable::registerPdxType(PdxTypes8::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}
	try{
	  Serializable::registerPdxType(PdxTypes9::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}
	try{
	  Serializable::registerPdxType(PdxTypes10::createDeserializable);
	}catch (const IllegalStateException& ) {
		    // ignore exception
	}
	  //TODO
	//Serializable::registerPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
	//Serializable::registerPdxType(PdxTests.PositionPdx.CreateDeserializable);

  //Region region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
	RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
	bool flag = false;
	{
		PdxTypes1Ptr p1(new PdxTypes1());
		CacheableKeyPtr keyport = CacheableKey::create(11);
		regPtr0->put(keyport, p1 );
		PdxTypes1Ptr  pRet = dynCast<PdxTypes1Ptr>(regPtr0->get(keyport));

		flag = p1->equals(pRet);
		LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
		ASSERT(flag == true, "Objects of type PdxTypes1 should be equal");
		checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes2Ptr p2(new PdxTypes2());
    CacheableKeyPtr keyport2 = CacheableKey::create(12);
    regPtr0->put(keyport2, p2);
    PdxTypes2Ptr pRet2 = dynCast<PdxTypes2Ptr>(regPtr0->get(keyport2));

    flag = p2->equals(pRet2);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes2 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes3Ptr p3(new PdxTypes3());
    CacheableKeyPtr keyport3 = CacheableKey::create(13);
    regPtr0->put(keyport3, p3);
    PdxTypes3Ptr pRet3 = dynCast<PdxTypes3Ptr>(regPtr0->get(keyport3));

    flag = p3->equals(pRet3);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes3 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes4Ptr p4(new PdxTypes4());
    CacheableKeyPtr keyport4 = CacheableKey::create(14);
    regPtr0->put(keyport4, p4);
    PdxTypes4Ptr pRet4 = dynCast<PdxTypes4Ptr>(regPtr0->get(keyport4));

    flag = p4->equals(pRet4);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes4 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes5Ptr p5(new PdxTypes5());
    CacheableKeyPtr keyport5 = CacheableKey::create(15);
    regPtr0->put(keyport5, p5);
    PdxTypes5Ptr pRet5 = dynCast<PdxTypes5Ptr>(regPtr0->get(keyport5));

    flag = p5->equals(pRet5);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes5 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes6Ptr p6(new PdxTypes6());
    CacheableKeyPtr keyport6 = CacheableKey::create(16);
    regPtr0->put(keyport6, p6);
    PdxTypes6Ptr pRet6 = dynCast<PdxTypes6Ptr>(regPtr0->get(keyport6));

    flag = p6->equals(pRet6);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes6 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes7Ptr p7(new PdxTypes7());
    CacheableKeyPtr keyport7 = CacheableKey::create(17);
    regPtr0->put(keyport7, p7);
    PdxTypes7Ptr pRet7 = dynCast<PdxTypes7Ptr>(regPtr0->get(keyport7));

    flag = p7->equals(pRet7);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes7 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes8Ptr p8(new PdxTypes8());
    CacheableKeyPtr keyport8 = CacheableKey::create(18);
    regPtr0->put(keyport8, p8);
    PdxTypes8Ptr pRet8 = dynCast<PdxTypes8Ptr>(regPtr0->get(keyport8));

    flag = p8->equals(pRet8);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes8 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes9Ptr p9(new PdxTypes9());
    CacheableKeyPtr keyport9 = CacheableKey::create(19);
    regPtr0->put(keyport9, p9);
    PdxTypes9Ptr pRet9 = dynCast<PdxTypes9Ptr>(regPtr0->get(keyport9));

    flag = p9->equals(pRet9);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes9 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes10Ptr p10(new PdxTypes10());
    CacheableKeyPtr keyport10 = CacheableKey::create(20);
    regPtr0->put(keyport10, p10);
    PdxTypes10Ptr pRet10 = dynCast<PdxTypes10Ptr>(regPtr0->get(keyport10));

    flag = p10->equals(pRet10);
    LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
    ASSERT(flag == true, "Objects of type PdxTypes10 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }


	LOG( "NIL:329:StepFive complete.\n" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyVariousPdxGets)
{
  try{
	Serializable::registerPdxType(PdxTypes1::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes2::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes3::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes4::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes5::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes6::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes7::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes8::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes9::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  try{
    Serializable::registerPdxType(PdxTypes10::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }
  //TODO::Uncomment it once PortfolioPdx/PositionPdx Classes are ready
  //Serializable::registerPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
  //Serializable::registerPdxType(PdxTests.PositionPdx.CreateDeserializable);

  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
  bool flag = false;
	{
  	PdxTypes1Ptr p1(new PdxTypes1());
		CacheableKeyPtr keyport = CacheableKey::create(11);
		PdxTypes1Ptr  pRet = dynCast<PdxTypes1Ptr>(regPtr0->get(keyport));

		flag = p1->equals(pRet);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes1 should be equal");
		checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes2Ptr p2(new PdxTypes2());
    CacheableKeyPtr keyport2 = CacheableKey::create(12);
    PdxTypes2Ptr pRet2 = dynCast<PdxTypes2Ptr>(regPtr0->get(keyport2));

    flag = p2->equals(pRet2);
    LOGDEBUG("VerifyVariousPdxGets:. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes2 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes3Ptr p3(new PdxTypes3());
    CacheableKeyPtr keyport3 = CacheableKey::create(13);
    PdxTypes3Ptr pRet3 = dynCast<PdxTypes3Ptr>(regPtr0->get(keyport3));

    flag = p3->equals(pRet3);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes3 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes4Ptr p4(new PdxTypes4());
    CacheableKeyPtr keyport4 = CacheableKey::create(14);
    PdxTypes4Ptr pRet4 = dynCast<PdxTypes4Ptr>(regPtr0->get(keyport4));

    flag = p4->equals(pRet4);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes4 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes5Ptr p5(new PdxTypes5());
    CacheableKeyPtr keyport5 = CacheableKey::create(15);
    PdxTypes5Ptr pRet5 = dynCast<PdxTypes5Ptr>(regPtr0->get(keyport5));

    flag = p5->equals(pRet5);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes5 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes6Ptr p6(new PdxTypes6());
    CacheableKeyPtr keyport6 = CacheableKey::create(16);
    PdxTypes6Ptr pRet6 = dynCast<PdxTypes6Ptr>(regPtr0->get(keyport6));

    flag = p6->equals(pRet6);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes6 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes7Ptr p7(new PdxTypes7());
    CacheableKeyPtr keyport7 = CacheableKey::create(17);
    PdxTypes7Ptr pRet7 = dynCast<PdxTypes7Ptr>(regPtr0->get(keyport7));

    flag = p7->equals(pRet7);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes7 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes8Ptr p8(new PdxTypes8());
    CacheableKeyPtr keyport8 = CacheableKey::create(18);
    PdxTypes8Ptr pRet8 = dynCast<PdxTypes8Ptr>(regPtr0->get(keyport8));

    flag = p8->equals(pRet8);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes8 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes9Ptr p9(new PdxTypes9());
    CacheableKeyPtr keyport9 = CacheableKey::create(19);
    PdxTypes9Ptr pRet9 = dynCast<PdxTypes9Ptr>(regPtr0->get(keyport9));

    flag = p9->equals(pRet9);
    LOGDEBUG("VerifyVariousPdxGets:. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes9 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }

  {
  	PdxTypes10Ptr p10(new PdxTypes10());
    CacheableKeyPtr keyport10 = CacheableKey::create(20);
    PdxTypes10Ptr pRet10 = dynCast<PdxTypes10Ptr>(regPtr0->get(keyport10));

    flag = p10->equals(pRet10);
    LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
    ASSERT(flag == true, "VerifyVariousPdxGets:Objects of type PdxTypes10 should be equal");
    checkPdxInstanceToStringAtServer(regPtr0);
  }
  LOG( "NIL:436:StepSix complete.\n" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
{
  RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  //QueryHelper * qh = &QueryHelper::getHelper();
  try{
    Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
  }catch (const IllegalStateException& ) {
  	    // ignore exception
  }

  try{
    Serializable::registerPdxType(Address::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  LOG("PdxClassV1 Registered Successfully....");

  LOG( "Trying to populate PDX objects.....\n" );
  CacheablePtr pdxobj(new PdxTests::PdxType());
  CacheableKeyPtr keyport = CacheableKey::create(1);

  //PUT Operation
  regPtr0->put(keyport, pdxobj );
  LOG("PdxTests::PdxType: PUT Done successfully....");

  //locally destroy PdxTests::PdxType
  regPtr0->localDestroy(keyport);
  LOG("localDestroy() operation....Done");

  LOG( "Done populating PDX objects.....Success\n" );
  LOG( "StepThree complete.\n" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
{
	//initClient(true);
	RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

  //QueryHelper * qh = &QueryHelper::getHelper();

  LOG( "Trying to GET PDX objects.....\n" );
  try{
    Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
  }catch (const IllegalStateException& ) {
    	    // ignore exception
  }

  try{
    Serializable::registerPdxType(Address::createDeserializable);
  }catch (const IllegalStateException& ) {
    // ignore exception
  }

  PdxTests::PdxTypePtr localPdxptr(new PdxTests::PdxType());

  CacheableKeyPtr keyport = CacheableKey::create(1);
  LOG("Client-2 PdxTests::PdxType GET OP Start....");
  PdxTests::PdxTypePtr  remotePdxptr = dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));
  LOG("Client-2 PdxTests::PdxType GET OP Done....");

  //
  PdxTests::PdxType* localPdx  = localPdxptr.ptr();
    PdxTests::PdxType* remotePdx = dynamic_cast<PdxTests::PdxType*>(remotePdxptr.ptr());

    //ToDo open this equals check
  LOGINFO("testThinClientPdxTests:StepFour before equal() check");
  ASSERT(remotePdx->equals(*localPdx, false) == true, "PdxTests::PdxTypes should be equal.");
  LOGINFO("testThinClientPdxTests:StepFour equal check done successfully");
  LOGINFO("GET OP Result: Char Val=%lc", remotePdx->getChar());
  //LOGINFO("GET OP Result: Char[0] val=%lc", remotePdx->getCharArray()[0]);
  //LOGINFO("GET OP Result: Char[1] val=%lc", remotePdx->getCharArray()[1]);
  LOGINFO("GET OP Result: Array of byte arrays [0]=%x", remotePdx->getArrayOfByteArrays()[0][0]);
  LOGINFO("GET OP Result: Array of byte arrays [1]=%x", remotePdx->getArrayOfByteArrays()[1][0]);
  LOGINFO("GET OP Result: Array of byte arrays [2]=%x", remotePdx->getArrayOfByteArrays()[2][0]);

  CacheableInt32* element = dynamic_cast<CacheableInt32*>(remotePdx->getArrayList()->at(0).ptr());
  LOGINFO("GET OP Result_1233: Array List element Value =%d", element->value());

  CacheableInt32* remoteKey = NULL;
  CacheableString* remoteVal = NULL;

  for (CacheableHashTable::Iterator iter = remotePdx->getHashTable()->begin( ); iter != remotePdx->getHashTable()->end( ); ++iter )
  {
    remoteKey = dynamic_cast<CacheableInt32*>(iter.first().ptr());
    remoteVal = dynamic_cast<CacheableString*>(iter.second().ptr());
    LOGINFO("HashTable Key Val = %d", remoteKey->value());
    LOGINFO("HashTable Val = %s", remoteVal->asChar());
    //(*iter1).first.value();
    //output.writeObject( *iter );
  }


    //LOGINFO("GET OP Result: IntVal1=%d", obj2->getInt1());
    //LOGINFO("GET OP Result: IntVal2=%d", obj2->getInt2());
    //LOGINFO("GET OP Result: IntVal3=%d", obj2->getInt3());
    //LOGINFO("GET OP Result: IntVal4=%d", obj2->getInt4());
    //LOGINFO("GET OP Result: IntVal5=%d", obj2->getInt5());
    //LOGINFO("GET OP Result: IntVal6=%d", obj2->getInt6());



    //LOGINFO("GET OP Result: BoolVal=%d", obj2->getBool());
    //LOGINFO("GET OP Result: ByteVal=%d", obj2->getByte());
  	//LOGINFO("GET OP Result: ShortVal=%d", obj2->getShort());

  	//LOGINFO("GET OP Result: IntVal=%d", obj2->getInt());

  	//LOGINFO("GET OP Result: LongVal=%ld", obj2->getLong());
  	//LOGINFO("GET OP Result: FloatVal=%f", obj2->getFloat());
  	//LOGINFO("GET OP Result: DoubleVal=%lf", obj2->getDouble());
  	//LOGINFO("GET OP Result: StringVal=%s", obj2->getString());
  	//LOGINFO("GET OP Result: BoolArray[0]=%d", obj2->getBoolArray()[0]);
  	//LOGINFO("GET OP Result: BoolArray[1]=%d", obj2->getBoolArray()[1]);
  	//LOGINFO("GET OP Result: BoolArray[2]=%d", obj2->getBoolArray()[2]);

  	//LOGINFO("GET OP Result: ByteArray[0]=%d", obj2->getByteArray()[0]);
  	//LOGINFO("GET OP Result: ByteArray[1]=%d", obj2->getByteArray()[1]);

  	//LOGINFO("GET OP Result: ShortArray[0]=%d", obj2->getShortArray()[0]);
  	//LOGINFO("GET OP Result: IntArray[0]=%d", obj2->getIntArray()[0]);
  	//LOGINFO("GET OP Result: LongArray[1]=%lld", obj2->getLongArray()[1]);
  	//LOGINFO("GET OP Result: FloatArray[0]=%f", obj2->getFloatArray()[0]);
  	//LOGINFO("GET OP Result: DoubleArray[1]=%lf", obj2->getDoubleArray()[1]);

    LOG( "Done Getting PDX objects.....Success\n" );

  LOG( "StepFour complete.\n" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,SetWeakHashMapToTrueC1)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypesIgnoreUnreadFieldsV1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToTrueC2)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypesIgnoreUnreadFieldsV2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,setWeakHashMapToFlaseC1)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypesIgnoreUnreadFieldsV1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToFalseC2)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypesIgnoreUnreadFieldsV2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1,SetWeakHashMapToTrueC1BM)
{
  m_useWeakHashMap = true;
  PdxTests::PdxType1V1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToTrueC2BM)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypes1V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,setWeakHashMapToFlaseC1BM)
{
  m_useWeakHashMap = false;
  PdxTests::PdxType1V1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToFalseC2BM)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypes1V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1,SetWeakHashMapToTrueC1BM2)
{
  m_useWeakHashMap = true;
  PdxTests::PdxType2V1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToTrueC2BM2)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypes2V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,setWeakHashMapToFlaseC1BM2)
{
  m_useWeakHashMap = false;
  PdxTests::PdxType2V1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToFalseC2BM2)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypes2V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1,SetWeakHashMapToTrueC1BM3)
{
  m_useWeakHashMap = true;
  PdxTests::PdxType3V1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToTrueC2BM3)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypes3V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,setWeakHashMapToFlaseC1BM3)
{
  m_useWeakHashMap = false;
  PdxTests::PdxType3V1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToFalseC2BM3)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypes3V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1,SetWeakHashMapToTrueC1BMR1)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypesV1R1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToTrueC2BMR1)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypesR1V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,setWeakHashMapToFlaseC1BMR1)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypesV1R1::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToFalseC2BMR1)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypesR1V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1,SetWeakHashMapToTrueC1BMR2)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypesV1R2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToTrueC2BMR2)
{
  m_useWeakHashMap = true;
  PdxTests::PdxTypesR2V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,setWeakHashMapToFlaseC1BMR2)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypesV1R2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetWeakHashMapToFalseC2BMR2)
{
  m_useWeakHashMap = false;
  PdxTests::PdxTypesR2V2::reset(m_useWeakHashMap);
}
END_TASK_DEFINITION
///

void runPdxDistOps (bool poolConfig = false, bool withLocators = false)
{
	if(!poolConfig){
    CALL_TASK(CreateServer)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }
  //StepThree: Put some portfolio/Position objects
  CALL_TASK(PutAndVerifyPdxInGet)
  CALL_TASK(VerifyGetOnly)
  CALL_TASK(PutAndVerifyVariousPdxTypes)
  CALL_TASK(VerifyVariousPdxGets)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }

}
*/

/*
void runPdxPutGetTest(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }
  //StepThree: Put some portfolio/Position objects
  CALL_TASK(StepThree)
  CALL_TASK(StepFour)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOpsR2(bool poolConfig = false, bool withLocators = false){
  if(!poolConfig){
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionTwoR21)

  CALL_TASK(getPutAtVersionOneR22)

  for (int i = 0; i < 10; i++)
  {
    CALL_TASK(getPutAtVersionTwoR23);
    CALL_TASK(getPutAtVersionOneR24);
  }

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOpsR1(bool poolConfig = false, bool withLocators = false){
  if(!poolConfig){
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionTwo1)

  CALL_TASK(getPutAtVersionOne2)

  CALL_TASK(getPutAtVersionTwo3)

  CALL_TASK(getPutAtVersionOne4)

  for (int i = 0; i < 10; i++)
  {
    CALL_TASK(getPutAtVersionTwo5);
    CALL_TASK(getPutAtVersionOne6);
  }

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOps(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionOne11)

  CALL_TASK(getPutAtVersionTwo12)

  CALL_TASK(getPutAtVersionOne13)

  CALL_TASK(getPutAtVersionTwo14)


  for (int i = 0; i < 10; i++)
  {
    CALL_TASK(getPutAtVersionOne15);
    CALL_TASK(getPutAtVersionTwo16);

  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOps2(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionOne21)

  CALL_TASK(getPutAtVersionTwo22)

  for (int i = 0; i < 10; i++)
  {
    CALL_TASK(getPutAtVersionOne23);
    CALL_TASK(getPutAtVersionTwo24);

  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOps3(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionOne31)

  CALL_TASK(getPutAtVersionTwo32)

  for (int i = 0; i < 10; i++)
  {
    CALL_TASK(getPutAtVersionOne33);
    CALL_TASK(getPutAtVersionTwo34);

  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}
*/

void runBasicMergeOpsWithPdxSerializer(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer3)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator3)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer3)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putFromVersion1_PS)

  CALL_TASK(putFromVersion2_PS)

  CALL_TASK(getputFromVersion1_PS)

  CALL_TASK(getAtVersion2_PS)


  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runJavaInteroperableOps(bool poolConfig = false, bool withLocators = false)
{
  if(!poolConfig){
    CALL_TASK(CreateServer2)
    CALL_TASK(StepOne)
  }else if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator2)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer2)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(JavaPutGet)//c1
  CALL_TASK(JavaGet)//c2

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}
/*
void runNestedPdxOps(bool poolConfig = false, bool withLocators = false){
  if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(PutAndVerifyNestedPdxInGet)

  CALL_TASK(VerifyNestedGetOnly)  

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runPdxInGFSOps(bool poolConfig = false, bool withLocators = false){
  if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(PutAndVerifyPdxInGFSInGet)

  CALL_TASK(VerifyPdxInGFSGetOnly)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void runPdxIgnoreUnreadFieldTest(bool poolConfig = false, bool withLocators = false){
  if(withLocators){
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc_PDX)
    CALL_TASK(StepTwoPoolLoc_PDX)
  }else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP_PDX)
    CALL_TASK(StepTwoPoolEP_PDX)
  }

  CALL_TASK(putV2PdxUI)

  CALL_TASK(putV1PdxUI)

  CALL_TASK(getV2PdxUI)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if(poolConfig && withLocators){
    CALL_TASK(CloseLocator)
  }
}

void enableWeakHashMapC1(){
  CALL_TASK(SetWeakHashMapToTrueC1)
}
void enableWeakHashMapC2(){
  CALL_TASK(SetWeakHashMapToTrueC2)
}

void disableWeakHashMapC1(){
  CALL_TASK(setWeakHashMapToFlaseC1)
}
void disableWeakHashMapC2(){
  CALL_TASK(SetWeakHashMapToFalseC2)
}
/////
void enableWeakHashMapC1BM(){
  CALL_TASK(SetWeakHashMapToTrueC1BM)
}
void enableWeakHashMapC2BM(){
  CALL_TASK(SetWeakHashMapToTrueC2BM)
}

void disableWeakHashMapC1BM(){
  CALL_TASK(setWeakHashMapToFlaseC1BM)
}
void disableWeakHashMapC2BM(){
  CALL_TASK(SetWeakHashMapToFalseC2BM)
}
////
void enableWeakHashMapC1BM2(){
  CALL_TASK(SetWeakHashMapToTrueC1BM2)
}
void enableWeakHashMapC2BM2(){
  CALL_TASK(SetWeakHashMapToTrueC2BM2)
}

void disableWeakHashMapC1BM2(){
  CALL_TASK(setWeakHashMapToFlaseC1BM2)
}
void disableWeakHashMapC2BM2(){
  CALL_TASK(SetWeakHashMapToFalseC2BM2)
}
////
void enableWeakHashMapC1BM3(){
  CALL_TASK(SetWeakHashMapToTrueC1BM3)
}
void enableWeakHashMapC2BM3(){
  CALL_TASK(SetWeakHashMapToTrueC2BM3)
}

void disableWeakHashMapC1BM3(){
  CALL_TASK(setWeakHashMapToFlaseC1BM3)
}
void disableWeakHashMapC2BM3(){
  CALL_TASK(SetWeakHashMapToFalseC2BM3)
}
/////
void enableWeakHashMapC1BMR1(){
  CALL_TASK(SetWeakHashMapToTrueC1BMR1)
}
void enableWeakHashMapC2BMR1(){
  CALL_TASK(SetWeakHashMapToTrueC2BMR1)
}

void disableWeakHashMapC1BMR1(){
  CALL_TASK(setWeakHashMapToFlaseC1BMR1)
}
void disableWeakHashMapC2BMR1(){
  CALL_TASK(SetWeakHashMapToFalseC2BMR1)
}
///////
void enableWeakHashMapC1BMR2(){
  CALL_TASK(SetWeakHashMapToTrueC1BMR2)
}
void enableWeakHashMapC2BMR2(){
  CALL_TASK(SetWeakHashMapToTrueC2BMR2)
}

void disableWeakHashMapC1BMR2(){
  CALL_TASK(setWeakHashMapToFlaseC1BMR2)
}
void disableWeakHashMapC2BMR2(){
  CALL_TASK(SetWeakHashMapToFalseC2BMR2)
}

*/


DUNIT_MAIN
{

  //PDXTEST::Test with Pool with EP
  //runPdxPutGetTest(true);

  //PdxDistOps-PdxTests::PdxType PUT/GET Test across clients
  {
    /*
    runPdxDistOps(true);
    runPdxDistOps(true, true);
    */
  }

  //BasicMergeOps 
  {
    /*
    enableWeakHashMapC1BM();
    enableWeakHashMapC2BM();
    runBasicMergeOps(true);  //Only Pool with endpoints, No locator..

    disableWeakHashMapC1BM();
    disableWeakHashMapC2BM();
    runBasicMergeOps(true, true);  //pool with locators
    */
  }

  //BasicMergeOps2
  {
    /*
    enableWeakHashMapC1BM2();
    enableWeakHashMapC2BM2();
    runBasicMergeOps2(true); // pool with server endpoints.

    disableWeakHashMapC1BM2();
    disableWeakHashMapC2BM2();
    runBasicMergeOps2(true, true); // pool with locators
    */
  }

  //BasicMergeOps3
  {
    /*
    enableWeakHashMapC1BM3();
    enableWeakHashMapC2BM3();
    runBasicMergeOps3(true); // pool with server endpoints

    disableWeakHashMapC1BM3();
    disableWeakHashMapC2BM3();
    runBasicMergeOps3(true, true); // pool with locators
    */
  }

  //BasicMergeOpsR1 
  {
    /*
    enableWeakHashMapC1BMR1();
    enableWeakHashMapC2BMR1();
    runBasicMergeOpsR1(true); // pool with server endpoints

    disableWeakHashMapC1BMR1();
    disableWeakHashMapC2BMR1();
    runBasicMergeOpsR1(true, true); // pool with locators
    */
  }

  //BasicMergeOpsR2
  {
    /*
    enableWeakHashMapC1BMR2();
    enableWeakHashMapC2BMR2();
    runBasicMergeOpsR2(true); // pool with server endpoints

    disableWeakHashMapC1BMR2();
    disableWeakHashMapC2BMR2();
    runBasicMergeOpsR2(true, true); // pool with locators
    */
  }

  //JavaInteroperableOps
  {
    runJavaInteroperableOps(true);  //pool with server endpoints
    runJavaInteroperableOps(true, true);  // pool with locators
  }

  //BasicMergeOpsWithPdxSerializer
  {
    runBasicMergeOpsWithPdxSerializer(true); //pool with server endpoints
    runBasicMergeOpsWithPdxSerializer(true, true);  // pool with locators
  }

  //NestedPdxOps
  {
    /*
    runNestedPdxOps(true);  //pool with server endpoints
    runNestedPdxOps(true, true);  // pool with locators
    */
  }

  //Pdxobject In Gemfire Serializable Ops
  {
    /*
    runPdxInGFSOps(true);  //pool with server endpoints
    runPdxInGFSOps(true, true);  // pool with locators
    */
  }

  {
    /*
    enableWeakHashMapC1();
    enableWeakHashMapC2();
    runPdxIgnoreUnreadFieldTest(true);

    disableWeakHashMapC1();
    disableWeakHashMapC2();
    runPdxIgnoreUnreadFieldTest(true, true);
    */
  }

}
END_MAIN

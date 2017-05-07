/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#ifndef _SPARC_SOLARIS
// TODO performance - broken on SPARC
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"
#include "testobject/PdxType.hpp"
#include "testobject/VariousPdxTypes.hpp"
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1


#include "testobject/Portfolio.hpp"
using namespace testobject;

using namespace gemfire;
using namespace test;

bool isLocalServer = true;
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );
static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, 1);
#include "LocatorHelper.hpp"

//---------------------------------------------------------------------------------
using namespace gemfire;
class CallbackListener;

typedef gemfire::SharedPtr< CallbackListener >
   CallbackListenerPtr;

class CallbackListener : public CacheListener {

  private:

    int m_creates;
    int m_updates;
    int m_invalidates;
    int m_destroys;
    int m_regionInvalidate;
    int m_regionDestroy;
    int m_regionClear;
    //CacheableStringPtr m_callbackArg;
    CacheablePtr m_callbackArg;

  public:

  CallbackListener( )
  : CacheListener(),
    m_creates( 0 ),
    m_updates( 0 ),
    m_invalidates( 0 ),
    m_destroys( 0 ),
    m_regionInvalidate( 0 ),
    m_regionDestroy( 0 ),
    m_regionClear(0),
    m_callbackArg(NULLPTR)
  {
    LOG("CallbackListener contructor called");
  }

  virtual ~CallbackListener( )
  {
  }


  int getCreates( )
  {
    return m_creates;
  }

  int getUpdates( )
  {
    return m_updates;
  }
  int getInvalidates( )
  {
    return m_invalidates;
  }
  int getDestroys( )
  {
    return m_destroys;
  }
  int getRegionInvalidates( )
  {
    return m_regionInvalidate;
  }
  int getRegionDestroys( )
  {
    return m_regionDestroy;
  }
  int getRegionClear( )
  {
    return m_regionClear;
  }
  void setCallBackArg(const CacheablePtr& callbackArg)
  {
    m_callbackArg = callbackArg;
  }

  void check(CacheablePtr eventCallback, int & updateEvent)
  {
    if (m_callbackArg != NULLPTR) {

      try
      {
        PortfolioPtr mCallbkArg = dynCast<PortfolioPtr>(m_callbackArg);

        PortfolioPtr callbkArg = dynCast<PortfolioPtr>(eventCallback);


        CacheableStringPtr fromCallback = callbkArg->getPkid();
        CacheableStringPtr mCallback = mCallbkArg->getPkid();

        LOGFINE("HItesh values are %s === %s ", fromCallback->asChar(), mCallback->asChar() );

        if (*(fromCallback.ptr()) == *(mCallback.ptr()))
        {
          LOGFINE("values are same");
          updateEvent++;
        }
        else
        {
          LOGFINE("values are NOT same");
        }
      }catch(const ClassCastException& ex){

        LOGFINE("Hitesh in class cast exception %s ", ex.getMessage());
        try
        {
          CacheableStringPtr fromCallback = dynCast<CacheableStringPtr>(eventCallback);
          CacheableStringPtr mCallback = dynCast<CacheableStringPtr>(m_callbackArg);

          LOGFINE("HItesh values are %s === %s ", fromCallback->asChar(), mCallback->asChar() );

          if (*(fromCallback.ptr()) == *(mCallback.ptr()))
          {
            LOGFINE("values are same");
            updateEvent++;
          }
          else
          {
            LOGFINE("values are NOT same");
          }
        }catch(const ClassCastException& ex2)
        {
          LOGFINE("Hitesh in class cast second exception %s ", ex2.getMessage());
        }
      }

    }
  }

  void checkcallbackArg(const EntryEvent& event, int & updateEvent)
  {
    check(event.getCallbackArgument(), updateEvent);
  }

  void checkcallbackArg(const RegionEvent& event, int & updateEvent)
  {
    check(event.getCallbackArgument(), updateEvent);
  }

  virtual void afterCreate( const EntryEvent& event )
  {
    checkcallbackArg(event, m_creates);
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    checkcallbackArg(event, m_updates);
  }

  virtual void afterInvalidate( const EntryEvent& event )
  {
    checkcallbackArg(event, m_invalidates );
  }

  virtual void afterDestroy( const EntryEvent& event )
  {
    checkcallbackArg(event, m_destroys);
  }

  virtual void afterRegionInvalidate( const RegionEvent& event )
  {
    checkcallbackArg(event, m_regionInvalidate);
  }

  virtual void afterRegionDestroy( const RegionEvent& event )
  {
    checkcallbackArg(event, m_regionDestroy);
  }
  virtual void afterRegionClear( const RegionEvent& event )
  {
      checkcallbackArg(event, m_regionClear);
  }
};
//---------------------------------------------------------------------------------


CallbackListenerPtr reg1Listener1 = NULLPTR;
CacheableStringPtr callBackStrPtr;

CacheablePtr callBackPortFolioPtr;

void setCacheListener(const char *regName, CallbackListenerPtr regListener)
{
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void validateEventCount(int line) {
  LOGINFO("ValidateEvents called from line (%d).", line);
  int num = reg1Listener1->getCreates();
  char buf[1024];
  sprintf(buf, "Didn't get expected callback arg in aftercreate event" );
  ASSERT( 7 == num, buf );
  num = reg1Listener1->getUpdates();
  sprintf(buf, "Didn't get expected callback arg in afterupdate events");
  ASSERT( 3 == num, buf);
  num = reg1Listener1->getInvalidates();
    sprintf(buf, "Didn't get expected callback arg in afterInvalidates events");
  ASSERT( 2 == num, buf);
  num = reg1Listener1->getDestroys();  
    sprintf(buf, "Didn't get expected callback arg in afterdestroy events");
  ASSERT( 5 == num, buf);
  num = reg1Listener1->getRegionInvalidates();
  sprintf(buf, "Didn't get expected callback arg in afterRegionInvalidates events");
  ASSERT( 1 == num, buf);
  num = reg1Listener1->getRegionDestroys();
  sprintf(buf, "Didn't get expected callback arg in afterRegiondestroy events");
  ASSERT( 1 == num, buf);
  num = reg1Listener1->getRegionClear();
  sprintf(buf, "Didn't get expected callback arg in afterRegionClear events");
  ASSERT( 1 == num, buf);
}

bool wantToChangeCallback = false;

bool isRegistered = false;

DUNIT_TASK_DEFINITION(CLIENT1, ChangeCallbackArg)
{
  wantToChangeCallback = true;

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, ChangeCallbackArg2)
{
  wantToChangeCallback = true;

}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(SERVER1, StartServer)
{
  if ( isLocalServer )
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription5.xml");
  LOG("SERVER started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1)
{
  initClient(true);
  
  callBackStrPtr = CacheableString::create( "Gemstone's Callback" );
  
  createRegion( regionNames[0], false, endPoint, true);
  if(!isRegistered)
  {
    isRegistered = true;
    Serializable::registerType( Portfolio::createDeserializable);
    Serializable::registerType( Position::createDeserializable);
  }
  reg1Listener1 = new CallbackListener();
  if(!wantToChangeCallback)
  {
    LOGINFO("string reg");
    reg1Listener1->setCallBackArg(callBackStrPtr);
  }
  else
  {
    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    LOGINFO("Portfolio reg");
    reg1Listener1->setCallBackArg(callBackPortFolioPtr);
  }
  setCacheListener(regionNames[0],reg1Listener1);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  regPtr->registerAllKeys();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2)
{
  initClient(true);
  
  callBackStrPtr = CacheableString::create( "Gemstone's Callback" );
  
  createRegion( regionNames[0], false, endPoint, true);
  if(!isRegistered)
  {
      isRegistered = true;
      Serializable::registerType( Portfolio::createDeserializable);
      Serializable::registerType( Position::createDeserializable);
  }
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
{
  initClient(true);
  
  callBackStrPtr = CacheableString::create( "Gemstone's Callback" );
  
  createPooledRegion( regionNames[0],false/*ack mode*/,NULL/*endpoints*/,locatorsG, "__TEST_POOL1__",true/*client notification*/);
  reg1Listener1 = new CallbackListener();
  callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
  if(!wantToChangeCallback)
    reg1Listener1->setCallBackArg(callBackStrPtr);
  else
    reg1Listener1->setCallBackArg(callBackPortFolioPtr);
  setCacheListener(regionNames[0],reg1Listener1);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  regPtr->registerAllKeys();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2_Pool_Locator)
{
  initClient(true);
  
  callBackStrPtr = CacheableString::create( "Gemstone's Callback" );
  
  createPooledRegion( regionNames[0],false/*ack mode*/,NULL/*endpoints*/,locatorsG, "__TEST_POOL1__",true/*client notification*/);
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_EndPoint)
{
  initClient(true);
  
  callBackStrPtr = CacheableString::create( "Gemstone's Callback" );
  
  createPooledRegion( regionNames[0],false/*ack mode*/,endPoint/*endpoints*/,NULL, "__TEST_POOL1__",true/*client notification*/);
  reg1Listener1 = new CallbackListener();
  callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
  if(!wantToChangeCallback)
    reg1Listener1->setCallBackArg(callBackStrPtr);
  else
    reg1Listener1->setCallBackArg(callBackPortFolioPtr);
  setCacheListener(regionNames[0],reg1Listener1);
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );
  regPtr->registerAllKeys();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2_Pool_EndPoint)
{
  initClient(true);
  
  callBackStrPtr = CacheableString::create( "Gemstone's Callback" );
  
  createPooledRegion( regionNames[0],false/*ack mode*/,endPoint/*endpoints*/,NULL, "__TEST_POOL1__",true/*client notification*/);
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT2, testCreatesAndUpdates)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

  if(!wantToChangeCallback)
  {
    regPtr->create(keys[1], vals[1],callBackStrPtr  );
    regPtr->create("aaa", "bbb",callBackStrPtr  );
    regPtr->put(keys[1], nvals[1],callBackStrPtr  );
  }
  else
  {
    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    regPtr->create("aaa", "bbb",callBackPortFolioPtr  );
    regPtr->create(keys[1], vals[1],callBackPortFolioPtr  );
    regPtr->put(keys[1], nvals[1],callBackPortFolioPtr);
  }

}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testInvalidates)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

  if(!wantToChangeCallback)
  {
	regPtr->localCreate(1234, 1234,callBackStrPtr  );
	regPtr->localCreate(12345, 12345,callBackStrPtr  );
	regPtr->localCreate(12346, 12346,callBackStrPtr  );
	regPtr->localPut(1234, vals[1],callBackStrPtr);
	regPtr->localInvalidate(1234, callBackStrPtr  );
	ASSERT(regPtr->localRemove(12345, 12345  ,callBackStrPtr) == true ,"Result of remove should be true, as this value exists locally.");
	ASSERT(regPtr->containsKey(12345) == false, "containsKey should be false");
	ASSERT(regPtr->localRemoveEx(12346 ,callBackStrPtr) == true ,"Result of remove should be true, as this value exists locally.");
	ASSERT(regPtr->containsKey(12346) == false, "containsKey should be false");
    regPtr->invalidate(keys[1], callBackStrPtr  );
    regPtr->invalidateRegion( callBackStrPtr );
  }
  else
  {
    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    regPtr->localCreate(1234, 1234,callBackPortFolioPtr  );
    regPtr->localCreate(12345, 12345,callBackPortFolioPtr  );
    regPtr->localCreate(12346, 12346,callBackPortFolioPtr  );
    regPtr->localPut(1234, vals[1],callBackPortFolioPtr);
    regPtr->localInvalidate(1234, callBackPortFolioPtr  );
    ASSERT(regPtr->localRemove(12345, 12345  ,callBackPortFolioPtr  ) == true ,"Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr->containsKey(12345) == false, "containsKey should be false");
    ASSERT(regPtr->localRemoveEx(12346 ,callBackPortFolioPtr) == true ,"Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr->containsKey(12346) == false, "containsKey should be false");
    regPtr->invalidate(keys[1], callBackPortFolioPtr  );
    regPtr->invalidateRegion( callBackPortFolioPtr );
  }


}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testDestroy)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

  if(!wantToChangeCallback)
  {
    regPtr->destroy(keys[1], callBackStrPtr  );
    ASSERT(regPtr->removeEx("aaa", callBackStrPtr  ) == true, "Result of remove should be true, as this value exists.");
    //regPtr->destroyRegion( callBackStrPtr  );
  }
  else
  {
    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    regPtr->destroy(keys[1], callBackPortFolioPtr );
    ASSERT(regPtr->removeEx("aaa", callBackPortFolioPtr  ) == true, "Result of remove should be true, as this value exists.");
    //regPtr->destroyRegion( callBackPortFolioPtr );
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testRemove)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

  if(!wantToChangeCallback)
  {
    regPtr->remove(keys[1], nvals[1], callBackStrPtr  );
    regPtr->destroyRegion( callBackStrPtr  );
  }
  else
  {
    regPtr->remove(keys[1], nvals[1], callBackPortFolioPtr );
    regPtr->destroyRegion( callBackPortFolioPtr );
  }
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, testlocalClear)
{
  RegionPtr regPtr = getHelper()->getRegion( regionNames[0] );

  if(!wantToChangeCallback)
  {
    regPtr->localClear(callBackStrPtr);
  }
  else
  {
    regPtr->localClear(callBackPortFolioPtr );
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testValidate)
{
  dunit::sleep(10000);
  validateEventCount(__LINE__);
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


void runThinClientListenerCallbackArgTest( int wantToChangeCallback, bool poolConfig, bool isLocator = true )
{
  if( wantToChangeCallback == 1)
  {
    CALL_TASK(ChangeCallbackArg);
    CALL_TASK(ChangeCallbackArg2);
  }

  if( poolConfig && isLocator )
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML5)
  }
  else
  {
    CALL_TASK( StartServer );
  }
  if(!poolConfig)
  {
    CALL_TASK( SetupClient1 );
    CALL_TASK( SetupClient2 );
  }
  else if( isLocator )
  {
    CALL_TASK( SetupClient1_Pool_Locator );
    CALL_TASK( SetupClient2_Pool_Locator );
  }
  else
  {
    CALL_TASK( SetupClient1_Pool_EndPoint );
    CALL_TASK( SetupClient2_Pool_EndPoint );
  }
  CALL_TASK( testCreatesAndUpdates );
  CALL_TASK( testInvalidates );
  CALL_TASK( testDestroy );
  CALL_TASK( testCreatesAndUpdates );
  CALL_TASK( testlocalClear );
  CALL_TASK( testRemove );
  CALL_TASK( testValidate );
  CALL_TASK( StopClient1 );
  CALL_TASK( StopClient2 );
  CALL_TASK ( StopServer );
  if( poolConfig && isLocator )
  {
    CALL_TASK(CloseLocator1);
  }
}


DUNIT_MAIN
{
  for(int i = 0; i < 2; i++)
  {
    runThinClientListenerCallbackArgTest( i, false );
    runThinClientListenerCallbackArgTest( i, true, true );
    runThinClientListenerCallbackArgTest( i, true, false );
  }
}
END_MAIN

#endif

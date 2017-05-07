/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurableFailover.hpp
 *
 *  Created on: Nov 4, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLEFAILOVER_HPP_
#define THINCLIENTDURABLEFAILOVER_HPP_



#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* Testing Parameters              Param's Value
Termination :                   Keepalive = true/ false, Client crash
Restart Time:                   Before Timeout / After Timeout
Register Interest               Durable/ Non Durable

Descripton:  There is One server , one feeder and two clients. Both clients comes up ->
feeder feed -> both clients go down in same way ( keepalive = true/ false , crash )->
feeder feed -> Client1 comes up -> Client2 comes up after timeout -> verify -> Shutdown

*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define FEEDER s2p2

class OperMonitor : public CacheListener
{
  int m_ops;
  HashMapOfCacheable m_map;

  void check(const EntryEvent& event)
  {
    m_ops++;

    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = NULLPTR;
    try {
      value = dynCast<CacheableInt32Ptr>(event.getNewValue());
    }
    catch ( Exception ) {
      // ARB: do nothing.
    }

    CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(key);
    if (keyPtr != NULLPTR && value!= NULLPTR) {
      char buf[256] = {'\0'};
      sprintf( buf, " Got Key: %s, Value: %d",keyPtr->toString(), value->value( ) );
      LOG(buf);
    }

    if (value != NULLPTR) {
      HashMapOfCacheable::Iterator item = m_map.find(key);

      if (item != m_map.end())
      {
        m_map.update(key, value);
      }
      else
      {
        m_map.insert(key, value);
      }
    }
  }

  public:

  OperMonitor():m_ops(0) {}

  ~OperMonitor()
  {
    m_map.clear();
  }

  void validate(int keyCount, int eventcount, int durableValue, int nonDurableValue)
  {
    LOG("validate called");
    char buf[256] = {'\0'};

    sprintf(buf,"Expected %d keys for the region, Actual = %d",keyCount,m_map.size());
    ASSERT(m_map.size() == keyCount, buf);

    sprintf(buf,"Expected %d events for the region, Actual = %d",eventcount,m_ops);
    ASSERT(m_ops == eventcount, buf);

    for (HashMapOfCacheable::Iterator item = m_map.begin(); item != m_map.end(); item++)
    {
      CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(item.first());
      CacheableInt32Ptr valuePtr = dynCast<CacheableInt32Ptr>(item.second());

      if( strchr( keyPtr->toString(), 'D' ) == NULL ) {/*Non Durable Key */
        sprintf( buf, "Expected final value for nonDurable Keys = %d, Actual = %d", nonDurableValue, valuePtr->value( ) );
        ASSERT( valuePtr->value( ) == nonDurableValue, buf );
      }
      else {                                 /*Durable Key */
        sprintf( buf, "Expected final value for Durable Keys = %d, Actual = %d", durableValue, valuePtr->value( ) );
        ASSERT( valuePtr->value() == durableValue, buf );
      }
    }
  }

  virtual void afterCreate( const EntryEvent& event )
  {
    LOG("afterCreate called");
    check(event);
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    LOG("afterUpdate called");
    check(event);
  }

  virtual void afterDestroy( const EntryEvent& event )
  {
    LOG("afterDestroy called");
    check(event);
  }

  virtual void afterRegionInvalidate( const RegionEvent& event ) {};
  virtual void afterRegionDestroy( const RegionEvent& event ) {};
};
typedef SharedPtr<OperMonitor> OperMonitorPtr;

void setCacheListener(const char *regName, OperMonitorPtr monitor)
{
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(monitor);
}

OperMonitorPtr mon1 = NULLPTR;
OperMonitorPtr mon2 = NULLPTR;

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"

/* Total 10 Keys , alternate durable and non-durable */
const char *mixKeys[] = { "Key-1", "D-Key-1", "L-Key", "LD-Key" };
const char *testRegex[] = { "D-Key-.*" , "Key-.*" };

void initClientCache( int redundancy, int durableTimeout, OperMonitorPtr& mon, int sleepDuration = 0, int durableIdx = 0 )
{
  if ( sleepDuration )
    SLEEP( sleepDuration );

  if (mon == NULLPTR) {
    mon = new OperMonitor( );
  }

  // VJR: 35 sec ack interval to ensure primary clears its Q only
  // after the secondary comes up and is able to receive the QRM
  // otherwise it will get the unacked events from GII causing the
  // client to get 2 extra / replayed events.
  initClientAndRegion( redundancy, durableIdx, endPoints, 1, 1, 300 );

  setCacheListener( regionNames[0], mon );

  getHelper( )->cachePtr->readyForEvents( );

  RegionPtr regPtr0 = getHelper()->getRegion( regionNames[0] );

  //for R =1 it will get a redundancy error
  try {
    regPtr0->registerRegex(testRegex[0],true );
  } catch ( Exception ) {
    // ARB: do nothing.
  }
  try {
    regPtr0->registerRegex(testRegex[1],false );
  } catch ( Exception ) {
    // ARB: do nothing.
  }

}

void feederUpdate( int value )
{
  createIntEntry( regionNames[0], mixKeys[0], value );
  gemfire::millisleep(10);
  createIntEntry( regionNames[0], mixKeys[1], value );
  gemfire::millisleep(10);
}

/* Close Client 1 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT1, Clnt1Down )
{
  // sleep 30 sec to allow clients' periodic acks (1 sec) to go out
  // this is along with the 5 sec sleep after feeder update and
  // tied to the notify-ack-interval setting of 35 sec.
  SLEEP(30000);
  getHelper()->disconnect(true);
  cleanProc();
  LOG( "Clnt1Down complete: Keepalive = True" );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServer)
{
  if ( isLocalServer ) {
    if ( g_poolConfig && g_poolLocators ) {
      CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml", locatorsG );
    }
    else {
      CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml" );
    }
  }

  LOG("SERVER started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServer2)
{
  if ( isLocalServer ) {
    if ( g_poolConfig && g_poolLocators ) {
      CacheHelper::initServer( 2 , "cacheserver_notify_subscription2.xml", locatorsG );
    }
    else {
      CacheHelper::initServer( 2 , "cacheserver_notify_subscription2.xml" );
    }
  }

  // ARB: sleep for 3 seconds to allow redundancy monitor to detect new server.
  gemfire::millisleep(3000);
  LOG("SERVER started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederInit)
{
  initClient(true);
  createRegion( regionNames[0], USE_ACK, endPoints, true);
  LOG( "FeederInit complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Clnt1Up)
{
  initClientCache( 0, 300, mon1 );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Clnt1UpR1)
{
  initClientCache( 1, 300, mon1 );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate1 )
{
  feederUpdate( 1 );

  // ARB: Wait 5 seconds for events to be removed from ha queues.
  gemfire::millisleep( 5000 );

  LOG( "FeederUpdate1 complete." );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate2 )
{
  feederUpdate( 2 );

  // ARB: Wait 5 seconds for events to be removed from ha queues.
  gemfire::millisleep( 5000 );

  LOG( "FeederUpdate2 complete." );
}
END_TASK_DEFINITION

// R =0 and clientDown, Intermediate events lost.
DUNIT_TASK_DEFINITION(CLIENT1, Verify1_C1 )
{
  LOG( "Client Verify 1." );
  mon1->validate( 2, 2, 1, 1 );
}
END_TASK_DEFINITION

// R =1 and clientDown, Durable events recieved
DUNIT_TASK_DEFINITION(CLIENT1, Verify2_C1 )
{
  LOG( "Client Verify 2." );
  mon1->validate( 2, 3, 2, 1 );
}
END_TASK_DEFINITION

// No clientDown, All events recieved
DUNIT_TASK_DEFINITION(CLIENT1, Verify3_C1 )
{
  LOG( "Client Verify 3." );
  mon1->validate( 2, 4, 2, 2 );
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER,CloseFeeder)
{
  cleanProc();
  LOG("FEEDER closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,CloseClient1)
{
  mon1 = NULLPTR;
  cleanProc();
  LOG("CLIENT1 closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1 , CloseServer)
{
  CacheHelper::closeServer( 1 );
  // ARB: Wait 2 seconds to allow client failover.
  gemfire::millisleep( 2000 );
  LOG("SERVER closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1 , CloseServer2)
{
  CacheHelper::closeServer( 2 );
  LOG("SERVER closed");
}
END_TASK_DEFINITION

void startClient( int redundancy )
{
  if ( redundancy == 1 ) {
    CALL_TASK( Clnt1UpR1 );
  }
  else {
    CALL_TASK( Clnt1Up );
  }
}

void doThinClientDurableFailover( bool poolConfig = true, bool poolLocators = false )
{
  initLocatorSettings( poolConfig, poolLocators );
  if ( poolConfig && poolLocators ) {
    CALL_TASK( StartLocator );
  }

  // Failover case 0: client is not closed during the test.
  // Failover case 1: client is closed during the test.

  for ( int failoverCase = 0; failoverCase <=1; failoverCase++ ) {
    for ( int redundancy = 0; redundancy <= 1; redundancy++ ) {
      CALL_TASK( StartServer );

      CALL_TASK( FeederInit );

      startClient( redundancy );

      CALL_TASK( StartServer2 );

      CALL_TASK( FeederUpdate1 );

      if ( failoverCase == 1 ) {
        // close client
        CALL_TASK( Clnt1Down );
      }

      CALL_TASK( CloseServer );

      CALL_TASK( FeederUpdate2 );

      if ( failoverCase == 1 ) {
        startClient( redundancy );
      }

      if ( failoverCase == 1 ) {
        if ( redundancy == 0 ) { // all intermediate events missed
          CALL_TASK( Verify1_C1 );
        }
        else {    // only durable events should be received.
          CALL_TASK( Verify2_C1 );
        }
      }
      else { // Normal failover, all event recieved.
        CALL_TASK( Verify3_C1 );
      }


      CALL_TASK( CloseClient1 );
      CALL_TASK( CloseFeeder );
      CALL_TASK( CloseServer2 );
    }
  }

  closeLocator( );
}

#endif /* THINCLIENTDURABLEFAILOVER_HPP_ */

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"

using namespace gemfire;

CacheListenerPtr nullListenerPtr;


class RegionWrapper
{
  public:
    RegionWrapper( const char* name )
    : m_regionPtr( cacheHelper->getRegion( name ) )
    {
      RegionAttributesPtr attrs;
      attrs = m_regionPtr->getAttributes( );
      m_noack = ( attrs->getScope() == ScopeType::DISTRIBUTED_NO_ACK );
    }

    void put( int key, int value )
    {
      char keybuf[100];
      char valbuf[100];
      sprintf( keybuf, "key%d", key );
      sprintf( valbuf, "%d", value );
      CacheableStringPtr valPtr = CacheableString::create( valbuf );
      m_regionPtr->put( keybuf, valPtr );
    }

    void waitForKey( CacheableKeyPtr& keyPtr )
    {
      if ( m_noack ) {
  // might have to wait for a moment.
  int tries = 0;
  while ( (tries < 100) && (! m_regionPtr->containsKey( keyPtr )) ) {
    SLEEP( 100 );
    tries++;
  }
      }
    }

    int waitForValue( CacheableKeyPtr& keyPtr, int expected,
                CacheableStringPtr& valPtr )
    {
      int tries = 0;
      int val = 0;
      do {
  valPtr = dynCast<CacheableStringPtr>( m_regionPtr->get( keyPtr ) );
  ASSERT(valPtr != NULLPTR, "value should not be null.");
  val = atoi( valPtr->asChar() );
  SLEEP( 100 );
  tries++;
      } while ( (val != expected) && (tries < 100) );
      return val;
    }

    // by convention, we'll accept value of -1 to mean not exists, 0 to mean invalid, and otherwise we'll compare.
    void test( int key, int value, int line )
    {
      char keybuf[100];
      sprintf( keybuf, "key%d", key );
      CacheableKeyPtr keyPtr = createKey( keybuf );

      if ( value == -1 ) {
  char ebuf[1024];
  sprintf( ebuf, "unexpected key found at %d", line );
  ASSERT( ! m_regionPtr->containsKey( keyPtr ), ebuf );
      } else if ( value == 0 ) {
  waitForKey( keyPtr );
  ASSERT( m_regionPtr->containsKey( keyPtr ), "missing key." );
  ASSERT( ! m_regionPtr->containsValueForKey( keyPtr ), "should have found invalid." );
      } else {
  waitForKey( keyPtr );
  ASSERT( m_regionPtr->containsKey( keyPtr ), "missing key." );
  CacheableStringPtr valPtr;
  int val = waitForValue( keyPtr, value, valPtr );
  char ebuf[1024];
  sprintf( ebuf, "unexpected value: \"%s\", expected \"%d\" from line %d",
                 valPtr->asChar(), value, line );
  ASSERT( val == value, ebuf );
      }
    }

    RegionPtr m_regionPtr;
    bool m_noack;
};


bool isLocalServer = false;
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );

TallyListenerPtr listener;

#define REGIONNAME "DistRegionAck"
DUNIT_TASK(s1p1,Setup)
{
  if ( isLocalServer )
    CacheHelper::initServer( 1 , "cacheserver_notify_subscription.xml");
  LOG("SERVER started");
}
ENDTASK

DUNIT_TASK(s1p1,CreateRegionNoCache)
{
  initClient(true);
  LOG( "Creating region in s1p1-pusher, no-ack, no-cache, no-listener" );
  createRegion(REGIONNAME, false, endPoint, true, NULLPTR, false);
}
ENDTASK

DUNIT_TASK(s1p2,CreateNoCacheWListener)
{
  LOG( "Creating region in s1p2-listener, no-ack, no-cache, with-listener" );
  initClient(true);
  listener = new TallyListener();
  createRegion(REGIONNAME, false, endPoint, true, listener, false);
}
ENDTASK

DUNIT_TASK(s2p1,CreateRegionCacheMirror)
{
  LOG( "Creating region in s2p1-storage, no-ack, cache, no-interestlist, no-listener" );
  initClient(true);
  createRegion(REGIONNAME, false, endPoint, true, NULLPTR, true);
}
ENDTASK

DUNIT_TASK(s2p2,CreateRegionCache)
{
  LOG( "Creating region in s2p2-subset, no-ack, no-mirror, cache, no-interestlist, with-listener" );
  initClient(true);
  listener = new TallyListener();
  createRegion(REGIONNAME, false, endPoint, true, listener, true);
}
ENDTASK

//Verify no events received by cacheless,nonmirror,w/listener
DUNIT_TASK(s1p2,NoEvents)
{
  LOG( "Verifying TallyListener has received nothing." );
  ASSERT( listener->getCreates() == 0, "Should be no creates" );
  ASSERT( listener->getUpdates() == 0, "Should be no updates" );
  ASSERT( listener->getLastKey() == NULLPTR, "Should be no key" );
  ASSERT( listener->getLastValue() == NULLPTR, "Should be no value" );
}
ENDTASK

//Put from remote
DUNIT_TASK(s1p1,SendCreate)
{
  LOG( "put(1,1) from s1p1-pusher" );
  RegionWrapper region( REGIONNAME );
  region.put( 1, 1 );
}
ENDTASK

//Create from remote
DUNIT_TASK(s2p1,SendUpdate)
{
  LOG( "update from s2p1-storage" );
  RegionWrapper region( REGIONNAME );
  region.put( 1, 2 );
}
ENDTASK

//Test cache stored update
DUNIT_TASK(s2p1,StoredUpdate)
{
  LOG( "check update on s2p1-storage" );
  RegionWrapper region( REGIONNAME );
  SLEEP(100); //let it do receiving...
  region.test( 1, 2, __LINE__ );
}
ENDTASK

DUNIT_TASK(s2p2,CheckEmpty)
{
  LOG( "check s2p2-subset is still empty." );
  RegionWrapper region( REGIONNAME );
  SLEEP(100); //let it do receiving...
  region.test( 1, -1, __LINE__ );
  ASSERT( listener->expectCreates( 0 ) == 0, "Should have been 0 create." );
  ASSERT( listener->expectUpdates( 0 ) == 0, "Should have been 0 updates." );
  region.put( 2, 1 );
  ASSERT( listener->expectCreates( 1 ) == 1, "Should have been 1 create." );
  ASSERT( listener->expectUpdates( 0 ) == 0, "Should have been 0 updates." );
}
ENDTASK


DUNIT_TASK(s1p1,CreateKey2Again)
{
  LOG( "Creating key2 in s1p1-pusher, should be consumed by s2p2, subset" );
  RegionWrapper region( REGIONNAME );
  region.test( 2, -1, __LINE__ );
  region.put( 2, 2 );
}
ENDTASK

DUNIT_TASK(s2p2,CheckNewValue)
{
  LOG( "Checking new value was received in cache from remote create." );
  SLEEP(100); //let it do receiving...
  RegionWrapper region( REGIONNAME );
  region.test( 2, 1, __LINE__ );
  ASSERT( listener->expectCreates( 2 ) == 1, "Should have been 1 create." );
}
ENDTASK

DUNIT_TASK(s1p1,CloseCache1)
{
  cleanProc();
}
ENDTASK

DUNIT_TASK(s1p2,CloseCache2)
{
  cleanProc();
}
ENDTASK

DUNIT_TASK(s2p1,CloseCache3)
{
  cleanProc();
}
ENDTASK

DUNIT_TASK(s1p1,CloseCache)
{
  CacheHelper::closeServer( 1 );
  LOG("SERVER closed");
}
ENDTASK

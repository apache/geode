/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

bool traderFailed = false;

#include "TraderEventListeners.hpp"
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>

#define VALSIZE 1000

using namespace gemfire;

DistributedSystemPtr systemPtr;
CachePtr cachePtr;

RegionPtr regFromAPtr;
RegionPtr regToAPtr;
RegionPtr regFromCPtr;
RegionPtr regToCPtr;

const char fromAName[] = REG_PREFIX "dunit_FromA";
const char toAName[] = REG_PREFIX "dunit_ToA";
const char fromCName[] = REG_PREFIX "dunit_FromC";
const char toCName[] = REG_PREFIX "dunit_ToC";

int receivedCount = 0;
class RegionWrapper;

CacheableKeyPtr createCacheableKey( const char* k )
{
  CacheableKeyPtr tmp = CacheableKey::create( k );
  return tmp;
}

CacheablePtr createCacheable( const char* v )
{
  CacheableStringPtr tmp = CacheableString::create( v );
  return tmp;
}
void checkValue(int batch, int* receivedCount, int expected)
{
  int lastReceivedCount = *receivedCount;
  int lastReceivedCountSame=0;

  char buf[1000];
  while( *receivedCount < expected ) 
  {
      ACE_OS::sleep( 1 ); // one second.
      sprintf( buf, "batch[%d] waiting to receive all sent messages. received: %d, expected: %d", batch, *receivedCount, expected );
      LOG( buf );
      if(lastReceivedCount!= *receivedCount)
      {
         lastReceivedCount= *receivedCount;
         lastReceivedCountSame = 0;
      } else if(++lastReceivedCountSame > 60)
      {
         sprintf(buf, "batch[%d] have not received any new entry for the past 1 minute", batch);
         ASSERT(false, buf);
      }
    }
    sprintf(buf, "batch[%d] finished receiving messages. total = %d", batch, *receivedCount );
    LOG( buf );
}

class RegionWrapper
{
    RegionPtr m_regionPtr;

  public:
    RegionWrapper( RegionPtr regPtr ) 
    : m_regionPtr( regPtr )
    {
    }

    template< class K, class V >
    void put( K& k, V& v ) 
    {
      CacheableKeyPtr keyPtr = createCacheableKey( k );
      CacheablePtr valPtr = createCacheable( v );
      m_regionPtr->put( keyPtr, valPtr );
    }
};
void sendValue(int batch, RegionWrapper& region, bool multicast, char* value, int begin, int end)
{
  for( int i = begin; i < end; i++ ) {
    char keybuf[100];
    sprintf( keybuf, "key%05d", i );
    region.put( keybuf, value );
    if(multicast==true)
    {
      ACE_OS::thr_yield();
    }
  }
  char msgbuf[100];
  sprintf(msgbuf, "finished sending messages for batch[%d]." , batch);
  LOG( msgbuf);
}

RegionPtr createRegion( CachePtr& cachePtr, const char* name, bool ack, bool caching, CacheListenerPtr& listener )
{
  AttributesFactory af;
  af.setScope( ack ? ScopeType::DISTRIBUTED_ACK : ScopeType::DISTRIBUTED_NO_ACK );
  af.setCachingEnabled( caching );
  if ( listener != NULL ) {
    af.setCacheListener( listener );
  }

  RegionAttributesPtr rattrsPtr = af.createRegionAttributes( );

  return cachePtr->createRegion( name, rattrsPtr );
}

RegionPtr createRegion( CachePtr& cachePtr, const char* name, bool ack, bool caching )
{
  CacheListenerPtr nullPtr;
  return createRegion( cachePtr, name, ack, caching, nullPtr );
}

void initCache( void ) 
{
  systemPtr = DistributedSystem::connect("trader");
  cachePtr = CacheFactory::create( (char *)"theCache", systemPtr );
}

void closeCache( void )
{
  regFromAPtr = NULL;
  regToAPtr = NULL;
  regFromCPtr = NULL;
  regToCPtr = NULL;
  cachePtr->close();
  cachePtr = NULL;
  systemPtr->disconnect();
  systemPtr = NULL;
}

DUNIT_TASK(s1p1,MirrorCreate)
{
  initCache();
  regFromAPtr = createRegion( cachePtr, fromAName, TRADER_SCOPE, true, true );
}
END_TASK(MirrorCreate)

DUNIT_TASK(s1p2,CreateProcBDNoCache)
{
  initCache();
  regToCPtr = createRegion( cachePtr, toCName, TRADER_SCOPE, false, false );
  CacheListenerPtr listener = new Forwarder( regToCPtr );
  regFromAPtr = createRegion( cachePtr, fromAName, TRADER_SCOPE, false, false, listener );
  regToAPtr = createRegion( cachePtr, toAName, TRADER_SCOPE, false, false );
  listener = new Swapper( regFromAPtr, regToAPtr );
  regFromCPtr = createRegion( cachePtr, fromCName, TRADER_SCOPE, false, false, listener );
}
END_TASK(CreateProcBDNoCache)

DUNIT_TASK(s2p1,CreateProcC)
{
  initCache();
  regFromCPtr = createRegion( cachePtr, fromCName, TRADER_SCOPE, false, false );
  CacheListenerPtr listener = new Forwarder( regFromCPtr );
  regToCPtr = createRegion( cachePtr, toCName, TRADER_SCOPE, false, false, listener );
}
END_TASK(CreateProcC)

DUNIT_TASK(s2p2,CreateProcA)
{
  initCache();
  regFromAPtr = createRegion( cachePtr, fromAName, TRADER_SCOPE, false, false );
  CacheListenerPtr listener = new ReceiveCounter( &receivedCount );
  regToAPtr = createRegion( cachePtr, toAName, TRADER_SCOPE, false, false, listener );
}
END_TASK(CreateProcA)

DUNIT_TASK(s2p2,DriverProcA)
{
  // put a bunch in FromA, and expect a bunch to be received by ToA.
  bool multicast = false;
  int expected = 50000;
  const char* tpp = DistributedSystem::getSystemProperties()->gfTransportProtocol();
  if(!ACE_OS::strncasecmp(tpp,"MULTICAST", 9))
  {
     multicast = true;
     expected = 2500;
  }
  char buf[1024];
  sprintf(buf, "expected=%d\n", expected);
  LOG(buf);
  RegionWrapper region( regFromAPtr );
  char value[VALSIZE];
  for( int v = 0; v < VALSIZE; v++ ) {
    value[v] = 'A';
  }
  value[VALSIZE - 1] = 0;

  int batch_count = 10;
  if(!multicast) batch_count = 2;
  int expectedCount=expected/batch_count;
  for(int j=0; j < batch_count; j++)
  {
     char msgbuf[100];
     int begin = j* expectedCount ;
     int end = (j+1)* expectedCount ;
     sprintf(msgbuf, "batch[%d]: from %d, to %d, total %d", j, begin, end, expectedCount);
     LOG(msgbuf);
     sendValue(j, region, multicast, value, begin, end);
     checkValue(j, &receivedCount, end);
  }
}
END_TASK(DriverProcA)


DUNIT_TASK(s2p2,s2p2Close)
{
  closeCache();
  ASSERT( traderFailed == false, "prior exception during queue processing." );
}
END_TASK(s2p2Close)

DUNIT_TASK(s2p1,s2p1Close)
{
  closeCache();
  ASSERT( traderFailed == false, "prior exception during queue processing." );
}
END_TASK(s2p1Close)

DUNIT_TASK(s1p2,s1p2Close)
{
  closeCache();
  ASSERT( traderFailed == false, "prior exception during queue processing." );
}
END_TASK(s1p2Close)

DUNIT_TASK(s1p1,s1p1Close)
{
  closeCache();
  ASSERT( traderFailed == false, "prior exception during queue processing." );
}
END_TASK(s1p1Close)


/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    DurableClientTest.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __DURABLE_TEST_HPP__
#define __DURABLE_TEST_HPP__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "fwklib/FrameworkTest.hpp"
#include <ace/Signal.h>
#include <stdlib.h>

#ifdef WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#endif


namespace gemfire {
 namespace testframework {
   namespace durable {

std::string DURABLEBB( "DURABLEBB" );
std::string REGIONSBB( "Regions" );

/*
This will hold all the data when client is Up and will dump when cache will be closed.
Also It will have  afterRegionLive() callback.
 */
class DurableCacheListener : public CacheListener
{
  int64_t  m_ops;
  std::string m_clntName;
  int32_t  m_prevValue;
  bool   m_result;
  std::string m_err;

  void check(const EntryEvent& event);

  public:

  DurableCacheListener();

  virtual ~DurableCacheListener(){}

  void dumpToBB();

  virtual void afterCreate( const EntryEvent& event )
  {
    check(event);
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    check(event);
  }

  virtual void afterInvalidate( const EntryEvent& event ){}

  virtual void afterDestroy( const EntryEvent& event ) {}

  virtual void afterRegionInvalidate( const RegionEvent& event ){}

  virtual void afterRegionDestroy( const RegionEvent& event )
  {
    dumpToBB();
  }

  virtual void afterRegionLive( const RegionEvent& event )
  {
    FWKINFO( "DurableCacheListener::afterRegionLive() called ");
  }

  virtual void close( const RegionPtr& region ) { }

};

//This will calculate max event rate and will log them when required.
class DurablePerfListener : public CacheListener
{
  int64_t  m_ops;
  int64_t  minTime;
  ACE_Time_Value startTime;
  ACE_Time_Value prevTime;  //Always holds prev event seen time

  void reCalculate(const EntryEvent& event)
  {
    if(m_ops == 0) { //first event
      startTime = ACE_OS::gettimeofday( );
    }
    m_ops++;
    ACE_Time_Value curTime = ACE_OS::gettimeofday( );
    ACE_Time_Value timeElapsed = curTime  - prevTime;

    int64_t diffTime = ( timeElapsed.sec( ) * 1000000 + timeElapsed.usec( ) );

    if(diffTime < minTime ) {
      minTime = diffTime;
    }
    prevTime = curTime;
  }

  public:

  DurablePerfListener():m_ops(0),minTime(99999999),
                        startTime(ACE_OS::gettimeofday( )),
                        prevTime(ACE_OS::gettimeofday( )){}

  virtual ~DurablePerfListener(){}

  void logPerformance() {
    ACE_Time_Value totalTime = prevTime - startTime;
    int64_t totalMicroTime = ( totalTime.sec( ) * 1000000 + totalTime.usec( ) );
    double averageRate = m_ops*1000000.0 / totalMicroTime;
    double maxRate = 1000000.0 / minTime;
    FWKINFO( "DurablePerfListener::Events = " << m_ops <<
             ", Time: " << totalTime.sec( ) << " Sec" <<
             ", Average Rate = " << averageRate << " Events/Sec" <<
             ", Max Rate = " << maxRate << " Events/Sec." );
  }

  virtual void afterCreate( const EntryEvent& event )
  {
    reCalculate(event);
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    reCalculate(event);
  }

};

class ConflationTestCacheListener : public CacheListener
{
public:
  ConflationTestCacheListener( const FrameworkTest * test ) :
    m_test( test ),
    m_numAfterCreate(0),
    m_numAfterUpdate(0),
    m_numAfterInvalidate(0),
    m_numAfterDestroy(0)
    {}
  virtual ~ConflationTestCacheListener(){}
  virtual void afterCreate( const EntryEvent& event );
  virtual void afterUpdate( const EntryEvent& event );
  virtual void afterInvalidate( const EntryEvent& event );
  virtual void afterDestroy( const EntryEvent& event );
  virtual void afterRegionLive( const RegionEvent& event )
  {
    FWKINFO( "ConflationTestCacheListener::afterRegionLive() called ");
  }
  virtual void afterRegionDestroy( const RegionEvent& event )
  {
    dumpToBB(event.getRegion());
  }

private:
  const FrameworkTest * m_test;
  long m_numAfterCreate;
  long m_numAfterUpdate;
  long m_numAfterInvalidate;
  long m_numAfterDestroy;
  void dumpToBB(const RegionPtr&);
  //std::string m_bb;
};

class DurableClientTest : public FrameworkTest
{
public:
  DurableClientTest( const char * initArgs ) :FrameworkTest( initArgs ),
                     m_KeysA( NULL ),m_MaxKeys( 0 ),isReady(false){}

  virtual ~DurableClientTest( void) {}

  void checkTest( const char * taskId );
  int32_t durableCacheFinalize(const char * taskId);
  int32_t closeNormalAndRestart(const char * taskId);
  int32_t callReadyForEvents(const char * taskId);
  int32_t restartClientAndRegInt(const char * taskId);
  int32_t durableClientVerify(const char * taskId);

  int32_t createRegion();
  int32_t registerInterestList();
  int32_t registerRegexList();
  int32_t registerAllKeys();
  int32_t incrementalPuts();
private:
  RegionPtr getRegionPtr( const char * reg = NULL );
  void clearKeys();
  void initStrKeys(int32_t low, int32_t high, const std::string & keyBase);
  void initIntKeys(int32_t low, int32_t high);

  CacheableKeyPtr * m_KeysA;
  int32_t m_MaxKeys;
public:
  bool isReady;

};

   } //   namespace durable
 } // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __DURABLE_TEST_HPP__

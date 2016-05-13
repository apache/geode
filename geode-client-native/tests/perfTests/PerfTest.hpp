/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    PerfTest.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __PERF_TEST_HPP__
#define __PERF_TEST_HPP__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"

#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Region.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "Query.hpp"
#include "QueryService.hpp"
#include "SelectResults.hpp"
#include "ResultSet.hpp"
#include "StructSet.hpp"
#include "Struct.hpp"
#include "SelectResultsIterator.hpp"

#include <stdlib.h>

#ifdef WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#endif

#define LAT_MARK 0x55667788

namespace gemfire {
 namespace testframework {
   namespace perf {

class DupChecker : public CacheListener
{
  /*

  Note:

  Currently, for failoverTestHAEventIDMap.xml, PerfTest::doSerialPuts has a hardcoded
  keycount of 1000 and values are put serially from 1 to 1000.

  */

  int m_ops;
  HashMapOfCacheable m_map;

  void check(const EntryEvent& event)
  {
    m_ops++;

    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = dynCast<CacheableInt32Ptr>(event.getNewValue());

    HashMapOfCacheable::Iterator item = m_map.find(key);

    if (item != m_map.end())
    {
      CacheableInt32Ptr check = dynCast<CacheableInt32Ptr>(item.second());

      if (value->value() != check->value() + 1)
      {
        char logmsg[100] = {0};
        sprintf(logmsg, "DupChecker: Duplicate detected. Existing value is %d, New value is %d.", check->value(), value->value());
        FWKSEVERE(logmsg);
        return;
      }

      m_map.update(key, value);
    }
    else
    {
      m_map.insert(key, value);
    }
  }

  void validate()
  {
    char infomsg[100] = {0};
    sprintf(infomsg, "DupChecker: got %d keys.", m_map.size());
    FWKINFO(infomsg);

    if (m_map.size() != 1000)
    {
      char logmsg[100] = {0};
      sprintf(logmsg, "DupChecker: Expected 1000 keys for the region, actual is %d.", m_map.size());
      FWKSEVERE(logmsg);
    }
    //ASSERT(m_ops <= 1000000, "Expected upto 1,000,000 events (1000 per key) for the region");

    sprintf(infomsg, "DupChecker: got %d ops.", m_ops);
    FWKINFO(infomsg);

    if (m_ops != 1000000)
    {
      char logmsg[100] = {0};
      sprintf(logmsg, "DupChecker: Expected 1,000,000 events (1000 per key) for the region, actual is %d.", m_ops);
      FWKSEVERE(logmsg);
    }

    for (HashMapOfCacheable::Iterator item = m_map.begin(); item != m_map.end(); item++)
    {
      CacheableInt32Ptr check = dynCast<CacheableInt32Ptr>(item.second());
      //ASSERT(check->value() == 1000, "Expected final value to be 10,000");
      if (check->value() != 1000)
      {
        char logmsg[100] = {0};
        sprintf(logmsg, "DupChecker: Expected 1000 as final value, actual is %d.", check->value());
        FWKSEVERE(logmsg);
      }
    }
  }

  public:

  DupChecker():m_ops(0)
  {
    FWKINFO("DupChecker: created");
  }

  virtual ~DupChecker()
  {
    FWKINFO("DupChecker: validating");
    validate();
    m_map.clear();
  }

  virtual void afterCreate( const EntryEvent& event )
  {
    check(event);
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    check(event);
  }

  virtual void afterRegionInvalidate( const RegionEvent& event ) {};
  virtual void afterRegionDestroy( const RegionEvent& event ) {};
};

// ----------------------------------------------------------------------------

class LatencyListener : public CacheListener
{
  // This class expects the new value to have the time "sent" as an int64_t

  FrameworkTest * m_test;
  int32_t m_maxLatency;
  int32_t m_minLatency;
  int64_t m_totLatency;
  int32_t m_samples;
  int32_t m_numAfterCreate;
  int32_t m_numAfterUpdate;

  public:

  LatencyListener( FrameworkTest * test ) :
    m_test( test ),
    m_maxLatency( 0 ),
    m_minLatency( 1000000000 ),
    m_totLatency( 0 ),
    m_samples( 0 ),
    m_numAfterCreate( 0 ),
    m_numAfterUpdate( 0 ) {}

  virtual void afterCreate( const EntryEvent& event ) {
    updateLatency( event );
    ++m_numAfterCreate;
  }

  virtual void afterUpdate( const EntryEvent& event ) {
    updateLatency( event );
    ++m_numAfterUpdate;
  }

  virtual ~LatencyListener() {
    std::string bb( "LatencyBB" );
    std::string key( "LatencyTag" );
    std::string tag = m_test->bbGetString( bb, key );
    if ( tag.empty() ) {
      tag = "No tag found";
    }
    int32_t avgLatency = 0;
    if ( m_samples != 0 ) {
      avgLatency = ( int32_t )( m_totLatency / ( int64_t )m_samples );
      FWKINFO( "LatencyCSV,MinMaxAvgSamples," << tag << "," << m_minLatency << ","
              << m_maxLatency << "," << avgLatency << "," << m_samples );
    FWKINFO( "LatencySuite: " << tag << " results:  " << m_minLatency
            << " micros min, " << m_maxLatency << " micros max, " << avgLatency << " micros avg, "
            << m_samples << " samples.  " << m_totLatency );
      FWKINFO( "Latency listener counters for " << tag << "   afterCreate: " << m_numAfterCreate
              << ", afterUpdate: " << m_numAfterUpdate );
    }
    else {
      FWKINFO( "LatencySuite: " << tag << " results: NO DATA SAMPLES TO REPORT ON." );
      FWKINFO( "Latency listener counters for " << tag << "   afterCreate: " << m_numAfterCreate
              << ", afterUpdate: " << m_numAfterUpdate );
    }
  }

  void updateLatency( const EntryEvent& event )
  {
    int64_t now = m_test->getAdjustedNowMicros();
    CacheableBytesPtr newVal = dynCast<CacheableBytesPtr>( event.getNewValue() );
    uint8_t * ptr = (uint8_t*)newVal->value();
    if ( LAT_MARK == *( int32_t * )( ptr ) ) {
      ptr += 4;
      int64_t * sendTime = ( int64_t * )( ptr );
      int32_t latency = ( int32_t )llabs( now - *sendTime );
//      if ( latency > 1000 ) {
//        FWKINFO( "LatencyListener::A Large latency: " << ( int32_t )latency );
//      }
      m_minLatency = std::min( latency, m_minLatency );
      m_maxLatency = std::max( latency, m_maxLatency );
      m_totLatency += latency;
      m_samples++;
//      FWKINFO( "LatencyListener::Average: " << ( int32_t )( m_totLatency / ( int64_t )m_samples ) );
    }
  }

};

class ConflationTestCacheListener : public CacheListener
{
public:
  ConflationTestCacheListener( const FrameworkTest * test ) :
    m_numAfterCreate(0),
    m_numAfterUpdate(0),
    m_numAfterInvalidate(0),
    m_numAfterDestroy(0),
    m_test( test ),
    m_bb( "ConflationCacheListener" )
  {
    std::string keyIsOldValue = std::string( "isOldValue");
    m_test->bbSet(m_bb, keyIsOldValue, "false");
  }
  virtual void afterCreate( const EntryEvent& event );

  virtual void afterUpdate( const EntryEvent& event );
  virtual void afterInvalidate( const EntryEvent& event );
  virtual void afterDestroy( const EntryEvent& event );
  virtual void afterRegionDestroy( const RegionEvent& event )
  {
    dumpToBB(event.getRegion());
  }

  virtual ~ConflationTestCacheListener(){}
private:
  long m_numAfterCreate;
  long m_numAfterUpdate;
  long m_numAfterInvalidate;
  long m_numAfterDestroy;
  const FrameworkTest * m_test;
  std::string m_bb;
  void dumpToBB(const RegionPtr&);
};

class PerfTestCacheListener : public CacheListener
{
public:
  PerfTestCacheListener():m_isCallbackArg(false),m_callBckArg(NULLPTR){
    reset();
  }
  
  virtual void setCallBackArg(CacheableKeyPtr callBackArg )
  {
	ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
	{
	 m_callBckArg = callBackArg;
	 if(m_callBckArg!=NULLPTR)
		m_isCallbackArg=true;
	}
  }

  virtual void validateCallBack(const EntryEvent& event)
  {
	ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
	{
	perf::sleepMillis(1000);
    try{
      CacheableKeyPtr myCallbkArg=m_callBckArg;
	  CacheableKeyPtr listnrCallbkArg = dynCast<CacheableKeyPtr>(event.getCallbackArgument());

	  if(listnrCallbkArg==NULLPTR)
	  {
		 FWKEXCEPTION("Listener CallBackArgument is NULL");
	  }
	 else{
		if(strcmp(myCallbkArg->toString()->asChar(),listnrCallbkArg->toString()->asChar())==0)
		      FWKINFO("SUCCESS ->CallBackArgument matched")
		    else
		    {
		     FWKEXCEPTION("MisMatch in  CallBackArgument");
		    }
		}

    }
    catch(Exception &e)
    {
    	FWKEXCEPTION("SP:Exception in calidateCallBack" << e.getMessage());
    }
	}
 }

  virtual void afterCreate( const EntryEvent& event ) {
	  if (m_sleep > 0) perf::sleepMillis(m_sleep);
	 if(event.getCallbackArgument()==NULLPTR){FWKINFO("Event without callBack Arg");}
	  else
	    validateCallBack(event);

	    ++m_numAfterCreate;

  }

  virtual void afterUpdate( const EntryEvent& event ) {
	  if (m_sleep > 0) perf::sleepMillis(m_sleep);

	  if(event.getCallbackArgument()==NULLPTR)
	  {FWKINFO("Event without callBack Arg");}
	  else
	    validateCallBack(event);

	    ++m_numAfterUpdate;//}

	// m_isCallbackArg=false;
  }

  virtual void afterInvalidate( const EntryEvent& event ) {
	  if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterInvalidate;
  }

  virtual void afterDestroy( const EntryEvent& event ) {
	  if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterDestroy;
		  if(event.getCallbackArgument()==NULLPTR){FWKINFO("Event without callBack Arg");}
	  else
		  validateCallBack(event);
	 // }
  }

  virtual void afterRegionInvalidate( const RegionEvent& event ){
	  if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterRegionInvalidate;
	}

  virtual void afterRegionDestroy( const RegionEvent& event ) {
	  if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterRegionDestroy;
	 }

  virtual void close( const RegionPtr& region ){if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numClose;}

  virtual ~PerfTestCacheListener() {
	  m_isCallbackArg=false;
    std::ostringstream summary;
    summary << "cache listener invoked afterCreate: " << m_numAfterCreate
            << " afterUpdate: " << m_numAfterUpdate
            << " afterInvalidate: " << m_numAfterInvalidate
            << " afterDestroy: " << m_numAfterDestroy
            << " afterRegionInvalidate: " << m_numAfterRegionInvalidate
            << " afterRegionDestroy: " << m_numAfterRegionDestroy
            << " region close: " << m_numClose << std::endl;
    FWKINFO(summary.str().c_str());
  }

  void reset(int32_t sleepTime = 0) {
    m_sleep = sleepTime;
    m_numAfterCreate = m_numAfterUpdate = m_numAfterInvalidate = 0;
    m_numAfterDestroy = m_numAfterRegionInvalidate = m_numAfterRegionDestroy = m_numClose = 0;
    m_isCallbackArg=false;
  }

private:
  long m_numAfterCreate;
  long m_numAfterUpdate;
  long m_numAfterInvalidate;
  long m_numAfterDestroy;
  long m_numAfterRegionInvalidate;
  long m_numAfterRegionDestroy;
  long m_numClose;
  bool m_isCallbackArg;
  CacheableKeyPtr m_callBckArg;
  int32_t m_sleep;
  const FrameworkTest * m_test;
  ACE_Recursive_Thread_Mutex m_lock;
};

class PerfTest : public FrameworkTest
{
public:
  PerfTest( const char * initArgs ) :
    FrameworkTest( initArgs ),
    m_KeysA( NULL ),
    m_MaxKeys( 0 ),
    m_KeyIndexBegin(0),
    m_MaxValues( 0 ),
    m_CValue( NULL ) {}

  virtual ~PerfTest( void ) {
    clearKeys();
    m_CValue = NULL;
  }

  void onRegisterMembers( void );

  int32_t createRegion();
  int32_t createPools();
  int32_t puts();
  int32_t putAll();
  int32_t putAllRegion();
  int32_t serialPuts();
  int32_t putBursts();
  int32_t popServers();
//  int32_t createLatencyRegion();
  int32_t latencyPuts();
  int32_t gets();
  int32_t popClient();
  int32_t destroys();
  int32_t netsearch();
  int32_t checkValues();
  int32_t localDestroyEntries();
  int32_t localDestroyRegion();
  int32_t populateRegion();
  int32_t destroysKeys();
  int32_t resetListener() ;
  int32_t popClientMS();
  int32_t registerInterestList();
  int32_t registerRegexList();
  int32_t unregisterRegexList();
  int32_t registerAllKeys();
  int32_t registerAllKeysWithResultKeys();
  int32_t verifyInterestList();
  int32_t doServerKeys();
  int32_t doIterateInt32Keys();
  int32_t validateQConflation();
  int32_t validateCreditSuisseTest();
  int32_t checkOverFlow();
  int32_t verifyDB();

  // Internal DataOutput, Queue and AtomicInc tests
  int32_t byteArrayConcat( );
  int32_t queuePopulate( );
  int32_t queueGetPut( );
  int32_t queueClean( );
  int32_t atomicIncDec( );
  int32_t getAllAndVerification( );
  int32_t destroyRegion();
  int32_t queries();
  int32_t createUpdateDestroy();
  // --------

  void checkTest( const char * taskId );
  void getClientSecurityParams(PropertiesPtr prop, std::string credentials);
  void measureMemory(std::string location, double & vs, double & rs);
  int32_t measureMemory();

private:

  int32_t initKeys(bool useDefault = true);
  void clearKeys();
  void initStrKeys(int32_t low, int32_t high, const std::string & keyBase);
  void initIntKeys(int32_t low, int32_t high);
  int32_t initValues( int32_t num, int32_t siz = 0, bool useDefault = true);
  RegionPtr getRegionPtr( const char * reg = NULL );
  bool checkReady(int32_t numClients);

  // Private methods for DataOutput and Queue tests
  void byteArrayVec( int32_t minSize, int32_t maxSize, int32_t numArrays );
  void byteArrayDeque( int32_t minSize, int32_t maxSize, int32_t numArrays );
  void byteArraySList( int32_t minSize, int32_t maxSize, int32_t numArrays );
  void byteArrayString( int32_t minSize, int32_t maxSize, int32_t numArrays );
  void byteArrayRope( int32_t minSize, int32_t maxSize, int32_t numArrays );
  void byteArrayDOut( int32_t minSize, int32_t maxSize, int32_t numArrays );
  void queueGetPutP( ClientTask* task, const char* taskName,
      int32_t numOps, int32_t numThreads );
  // --------

  CacheableKeyPtr * m_KeysA;
  int32_t m_MaxKeys;
  int32_t m_KeyIndexBegin;
  int32_t m_MaxValues;

  CacheableBytesPtr * m_CValue;
  PerfSuite m_PerfSuite;

};

   } //   namespace perf
 } // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __PERF_TEST_HPP__

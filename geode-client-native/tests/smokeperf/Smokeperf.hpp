/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    Smokeperf.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __SMOKEPERF_TEST_HPP__
#define __SMOKEPERF_TEST_HPP__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"

#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Region.hpp"
#include "CqAttributesFactory.hpp"
#include "CqAttributes.hpp"
#include "CqListener.hpp"
#include "CqQuery.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "PerfStat.hpp"
#include "testobject/PSTObject.hpp"
#include "testobject/ArrayOfByte.hpp"
#include "testobject/FastAssetAccount.hpp"
#include "testobject/FastAsset.hpp"
#include "testobject/DeltaFastAssetAccount.hpp"
#include "testobject/DeltaPSTObject.hpp"

#include <stdlib.h>

#ifdef WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#endif

#define LAT_MARK 0x55667788
#define LATENCY_SPIKE_THRESHOLD 10000000

using namespace testobject;
using namespace perf;
namespace gemfire {
 namespace testframework {
   namespace smokeperf {

class PerfCacheListener
{
public:
  PerfCacheListener(PerfStat *perfstat):statistics(perfstat){
   }
protected:
  PerfStat *statistics;
  void recordLatency(CacheablePtr& objValue) {
    ACE_Time_Value startTime = ACE_OS::gettimeofday();
    ACE_UINT64 tusec = 0;
    startTime.to_usec(tusec);
    uint64_t now = tusec * 1000;
    uint64_t then;
    if (objValue->typeId() == GemfireTypeIds::CacheableBytes) {
      then
          = ArrayOfByte::getTimestamp(dynCast<CacheableBytesPtr> (objValue));
    } else {
      then = dynCast<TimestampedObjectPtr> (objValue)->getTimestamp();
    }
    uint64_t latency = now - then;
    //FWKINFO("PerfCacheListener::recordLatency now = " << now << " then =" << then << " diff = " << latency );
    if (latency > LATENCY_SPIKE_THRESHOLD) {
      statistics->incLatencySpikes(1);
    }
    if (latency < 0) {
      statistics->incNegativeLatencies(1);
    } else {
      statistics->incUpdateLatency(latency);
    }
  }
};

// ----------------------------------------------------------------------------
class LatencyListener : public PerfCacheListener, public CacheListener
{
  public:
    LatencyListener(PerfStat *perfstat):PerfCacheListener(perfstat){
   }
   void afterCreate( const EntryEvent& event ) {
   }
   void afterUpdate( const EntryEvent& event ) {
     CacheableKeyPtr key = event.getKey();
     CacheablePtr value = event.getNewValue() ;
     recordLatency(value);
   }
};

class CQLatencyListener : public PerfCacheListener, public CqListener
{
  public:
    CQLatencyListener(PerfStat *perfstat):PerfCacheListener(perfstat){
    }
    void onEvent(const CqEvent& event) {
      CacheablePtr value = event.getNewValue() ;
      recordLatency(value);
    }
    void onError(const CqEvent& event) {
    }
    void close() {
    }
};

class perfCacheLoader : virtual public CacheLoader {
    int32_t m_loads;
  public:
    perfCacheLoader():CacheLoader(),m_loads( 0 ){}
    virtual ~perfCacheLoader() {}
    CacheablePtr load(const RegionPtr& rp,
                      const CacheableKeyPtr& key,
                      const UserDataPtr& aCallbackArgument) {
      return CacheableInt32::create(m_loads++);
    }
    virtual void close( const RegionPtr& region ){}

};

class DurableCacheListener: public CacheListener {
  int64_t m_ops;
  std::string m_clntName;
  int32_t m_prevValue;

  void check(const EntryEvent& event);

public:

  DurableCacheListener();

  virtual ~DurableCacheListener() {
  }

  void dumpToBB();

  virtual void afterCreate(const EntryEvent& event) {
    //FWKINFO( "In DurableCacheListener afterCreate" );
    check(event);
  }

  virtual void afterUpdate(const EntryEvent& event) {
    //FWKINFO( "In DurableCacheListener afterUpdate" );
    check(event);
  }

  virtual void afterInvalidate(const EntryEvent& event) {
  }

  virtual void afterDestroy(const EntryEvent& event) {
  }

  virtual void afterRegionInvalidate(const RegionEvent& event) {
  }

  virtual void afterRegionDestroy(const RegionEvent& event) {
    dumpToBB();
  }

  virtual void afterRegionLive(const RegionEvent& event) {
    FWKINFO( "DurableCacheListener::afterRegionLive() called ");
  }

  virtual void close(const RegionPtr& region) {
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

class SmokeperfCacheListener : public CacheListener
{
public:
  SmokeperfCacheListener() {
    reset();
  }

  virtual void afterCreate( const EntryEvent& event ) {if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterCreate;}

  virtual void afterUpdate( const EntryEvent& event ) {if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterUpdate;}

  virtual void afterInvalidate( const EntryEvent& event ) {if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterInvalidate;}

  virtual void afterDestroy( const EntryEvent& event ) {if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterDestroy;}

  virtual void afterRegionInvalidate( const RegionEvent& event ){if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterRegionInvalidate;}

  virtual void afterRegionDestroy( const RegionEvent& event ) {if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numAfterRegionDestroy;}

  virtual void close( const RegionPtr& region ){if (m_sleep > 0) perf::sleepMillis(m_sleep); ++m_numClose;}

  virtual ~SmokeperfCacheListener() {
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

  void reset(long sleepTime = 0) {
    m_sleep = sleepTime;
    m_numAfterCreate = m_numAfterUpdate = m_numAfterInvalidate = 0;
    m_numAfterDestroy = m_numAfterRegionInvalidate = m_numAfterRegionDestroy = m_numClose = 0;
  }

private:
  long m_numAfterCreate;
  long m_numAfterUpdate;
  long m_numAfterInvalidate;
  long m_numAfterDestroy;
  long m_numAfterRegionInvalidate;
  long m_numAfterRegionDestroy;
  long m_numClose;
  long m_sleep;
};

class Smokeperf : public FrameworkTest
{
public:
  Smokeperf( const char * initArgs ) :
    FrameworkTest( initArgs ),
    m_KeysA( NULL ),
    m_MaxKeys( 0 ),
    m_KeyIndexBegin(0),
    m_MaxValues( 0 ),
    m_CValue( NULL ),
    m_isObjectRegistered(false) {}

  virtual ~Smokeperf( void ) {
    clearKeys();
    m_CValue = NULL;
  }

  void onRegisterMembers( void );

  int32_t createRegion();
  int32_t puts();
  int32_t gets();
  int32_t populateRegion();
  int32_t registerInterestList();
  int32_t registerRegexList();
  int32_t unregisterRegexList();
  int32_t registerAllKeys();
    // Internal DataOutput, Queue and AtomicInc tests
  int32_t openStatistic();
  int32_t closeStatistic();
  int32_t trimSpecData();
  int32_t createPools();
  int32_t cyclePoolTask();
  int32_t cycleBridgeConnectionTask();
  int32_t mixPutGetDataTask();
  int32_t queries();
  int32_t registerCQs();
  int32_t putBatchObj();
  int32_t cycleDurableBridgeConnection(const char * taskId);
  int32_t cycleDurableClientTask(const char * taskId);
  int32_t createEntryMapTask();
  int32_t putAllEntryMapTask();
  int32_t updateDelta();
  //function created for scale perf tests
  int32_t createData();
  int32_t getDataTask();
  int32_t putDataTask();
  int32_t createOrPutBulkDataTask();
  int32_t getBulkDataTask();
  int32_t scaleupdateDelta();
  int32_t getFunctionExecutionData();
  int32_t putFunctionExecutionData();
  int32_t trimScaleSpecData();
  // --------

  void checkTest( const char * taskId );
  void getClientSecurityParams(PropertiesPtr prop, std::string credentials);

private:

  int32_t initKeys(bool useDefault = true, bool useAllClientID = false);
  void clearKeys();
  void initStrKeys(int32_t low, int32_t high, const std::string & keyBase, uint32_t clientId,bool useAllClientID = false);
  void initIntKeys(int32_t low, int32_t high);
  int32_t initValues( int32_t num, int32_t siz = 0, bool useDefault = true);
  RegionPtr getRegionPtr( const char * reg = NULL );
  bool checkReady(int32_t numClients);

  void setTrimTime(std::string op, bool endTime = false);
  std::string getQuery(int i);
  int32_t initBatchKeys(bool useDefault = true);
  void trimrecord(FILE* trimfile);
  // --------

  CacheableKeyPtr * m_KeysA;
  int32_t m_MaxKeys;
  int32_t m_KeyIndexBegin;
  int32_t m_MaxValues;

  CacheableBytesPtr * m_CValue;
  bool m_isObjectRegistered;
  PerfSuite m_PerfSuite;
  std::vector < HashMapOfCacheablePtr > maps;

};

   } //   namespace smokeperf
 } // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __SMOKEPERF_TEST_HPP__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __SmokeTasks_hpp__
#define __SmokeTasks_hpp__

#include <GemfireCppCache.hpp>
#include "fwklib/ClientTask.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/FwkLog.hpp"
#include "ObjectHelper.hpp"
#include <memory.h>

using namespace gemfire::testframework;
using namespace testobject;
using namespace perf;

namespace gemfire {
  namespace testframework {
    namespace smokeperf {
class PutGetTask : public ClientTask
{
protected:
  RegionPtr m_Region;
  CacheableKeyPtr * m_Keys;
  CacheableBytesPtr * m_Value;
  uint32_t m_MaxKeys;
  uint32_t m_size;
  std::string m_objectType;
  bool m_encodeKey;
  bool m_encodeTimestamp;
  bool m_isMainWorkLoad;
  AtomicInc m_Cntr;
  ACE_TSS<perf::Counter> m_count;
  ACE_TSS<perf::Counter> m_MyOffset;

  uint32_t m_iters;

public:
  static PerfStat *perfstat[10];

  PutGetTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t max, CacheableBytesPtr * value )
    : m_Region( reg ), m_Keys(keys), m_Value( value ), m_MaxKeys( max ),
    m_MyOffset(), m_iters( 100 )  {}

  PutGetTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t max, bool isMainWorkLoad )
    : m_Region( reg ), m_Keys(keys), m_MaxKeys( max ),m_isMainWorkLoad(isMainWorkLoad),
    m_MyOffset(), m_iters( 100 )  {}

  PutGetTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t max ,uint32_t size,
    std::string objectType, bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad)
    : m_Region( reg ), m_Keys(keys), m_MaxKeys( max ),m_size(size), m_objectType(objectType),
    m_encodeKey(encodeKey),m_encodeTimestamp(encodeTimestamp),m_isMainWorkLoad(isMainWorkLoad),m_MyOffset(), m_iters( 100 )  {}

  PutGetTask( RegionPtr reg, uint32_t max ,uint32_t size,
     std::string objectType, bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad)
     : m_Region( reg ), m_MaxKeys( max ),m_size(size), m_objectType(objectType),
     m_encodeKey(encodeKey),m_encodeTimestamp(encodeTimestamp),m_isMainWorkLoad(isMainWorkLoad),m_MyOffset(), m_iters( 100 )  {}


  PutGetTask( RegionPtr reg, uint32_t max, bool isMainWorkLoad )
    : m_Region( reg ),m_MaxKeys( max ),m_isMainWorkLoad(isMainWorkLoad),
    m_MyOffset(), m_iters( 100 )  {}

  PutGetTask(RegionPtr reg)
        : m_Region( reg ),m_iters( 100 )  {}
  PutGetTask()
      : m_iters( 100 )  {}


  virtual bool doSetup( int32_t id ) {
    // per thread iteration offset
    double max = m_MaxKeys;
    srand( (++m_Cntr * id) + (unsigned int)time(0) );
    m_MyOffset->add( (int) (( ( max * rand() ) / ( RAND_MAX + 1.0 ))) );
    if ( m_Iterations > 0 )
      m_Loop = m_Iterations;
    else
      m_Loop = -1;
    return true;
  }

  virtual void doCleanup( int32_t id ) {}

  virtual void setKeys( CacheableKeyPtr * keys ) { m_Keys = keys; }
  virtual void setValue( CacheableBytesPtr * val ) { m_Value = val; }

  virtual ~PutGetTask() {}
};

class InitPerfStat : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    InitPerfStat():m_cnt(0){}

    virtual uint32_t doTask( int32_t id )
    {
      int localcnt = m_cnt++;
      perfstat[localcnt] = new PerfStat(( uint32_t )( ACE_Thread::self()));
      return localcnt;
    }
};
class PutsTask : public PutGetTask
{
public:
  PutsTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt, CacheableBytesPtr * value)
    : PutGetTask( reg, keys, keyCnt, value ) {
  }

  virtual uint32_t doTask( int32_t id )
  {
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      m_Region->put( m_Keys[idx], m_Value[idx] );
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

class PutAllTask : public PutGetTask
{
public:
  PutAllTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt, CacheableBytesPtr * value )
    : PutGetTask( reg, keys, keyCnt, value ) {}

  virtual uint32_t doTask( int32_t id )
  {
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    HashMapOfCacheable map;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      map.insert(m_Keys[idx], m_Value[idx]);
      count++;
    }
    ACE_Time_Value startTime = ACE_OS::gettimeofday();
    m_Region->putAll(map);
    ACE_Time_Value interval = ACE_OS::gettimeofday() - startTime;
    FWKINFO("Time Taken to execute putAll for " << m_MaxKeys << " is: " <<
         interval.sec() << "." << interval.usec() << " sec");
    return ( count - m_MyOffset->value() );
  }
};

class GetsTask : public PutGetTask
{
 public:
  AtomicInc m_cnt;
  GetsTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,bool isMainWorkLoad)
    : PutGetTask( reg, keys, keyCnt ,isMainWorkLoad),m_cnt(0) {
    }

  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      CacheableKeyPtr keyPtr = m_Keys[count % m_MaxKeys];
      startTime = perfstat[localcnt]->startGet();
      CacheablePtr valPtr = m_Region->get( keyPtr );
      perfstat[localcnt]->endGet(startTime,m_isMainWorkLoad);
      if (valPtr == NULLPTR) {
        char buf[ 2048 ];
        sprintf( buf, "Could not find key %s in region %s",
            keyPtr->toString( )->asChar( ), m_Region->getName( ) );
        throw gemfire::EntryNotFoundException( buf );
      }
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class CreateTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_assetAcSize;
    int32_t m_assetmaxVal;
    CreateTask(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,int32_t assetACsize=0, int32_t assetMaxVal = 0)
      :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
      m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal) {
     }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal,idx);
      startTime = perfstat[localcnt]->startCreate();
      m_Region->create( m_Keys[idx], obj );
      //FWKINFO("create key  " << m_Keys[idx]->toString( )->asChar( ) << " and value ");
      perfstat[localcnt]->endCreate(startTime,m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

class PutTask : public PutGetTask
{
  AtomicInc m_cnt;
  public:
    PutTask(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad)
      :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0){
    }

  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp);
      startTime = perfstat[localcnt]->startPut();
      m_Region->put( m_Keys[idx], obj );
      perfstat[localcnt]->endPut(startTime,m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class MeteredPutTask:public PutTask
{
  AtomicInc m_cnt;
  int32_t m_opSec;
  public:
  MeteredPutTask(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad, int32_t opSec = 0)
      :PutTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),m_opSec(opSec) {
    }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    int64_t startTime;
    //for the slow put
    PaceMeter meter( m_opSec );
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp);
      startTime = perfstat[localcnt]->startPut();
      m_Region->put( m_Keys[idx], obj );
      perfstat[localcnt]->endPut(startTime,m_isMainWorkLoad);
      meter.checkPace();
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class PutGetMixTask : public PutGetTask
{
  int32_t m_putPercentage;
  int m_cnt;
  public:
    PutGetMixTask(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,int32_t putpercentage)
      :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad)
      ,m_putPercentage(putpercentage),m_cnt(0)
       {
       }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      uint32_t n = GsRandom::random( (uint32_t)1, (uint32_t)100 );
      idx = count % m_MaxKeys;
      if(n < (uint32_t)m_putPercentage){
        CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp);
        startTime = perfstat[localcnt]->startPut();
        m_Region->put( m_Keys[idx], obj );
        perfstat[localcnt]->endPut(startTime,m_isMainWorkLoad);
      }
      else{
        CacheableKeyPtr keyPtr = m_Keys[idx];
        startTime = perfstat[localcnt]->startGet();
        CacheablePtr valPtr = m_Region->get( keyPtr );
        perfstat[localcnt]->endGet(startTime,m_isMainWorkLoad);
        if (valPtr == NULLPTR) {
          char buf[ 2048 ];
          sprintf( buf, "Could not find key %s in region %s",
              keyPtr->toString( )->asChar( ), m_Region->getName( ) );
          throw gemfire::EntryNotFoundException( buf );
        }
      }
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class RegionQueryTask : public PutGetTask
{
  std::string m_queryString;
  AtomicInc m_cnt;
public:
  RegionQueryTask( RegionPtr reg,std::string queryString)
    : PutGetTask(reg),m_queryString(queryString),m_cnt(0){}

  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    int32_t count = m_MyOffset->value();
    int32_t loop = m_Loop;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      startTime = perfstat[localcnt]->startQuery();
      SelectResultsPtr sptr = m_Region->query(m_queryString.c_str(),600);
      perfstat[localcnt]->endQuery(startTime,m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

class PutBatchObjectTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_batchSize;
    int32_t m_batchObjSize;
    PutBatchObjectTask(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,int32_t batchSize=0, int32_t objsize = 0)
      :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
      m_batchSize(batchSize),m_batchObjSize(objsize){
     }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_batchSize,m_batchObjSize,idx);
      startTime = perfstat[localcnt]->startPut();
      m_Region->put( m_Keys[idx], obj );
      perfstat[localcnt]->endPut(startTime,m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class CreatePutAllMap : public PutGetTask
{
  AtomicInc m_cnt;
  std::vector < HashMapOfCacheablePtr > &m_maps;
  public:
    CreatePutAllMap(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
        std::vector < HashMapOfCacheablePtr > &maps, bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad = false)
      :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),m_maps(maps) {
    }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    HashMapOfCacheablePtr hmoc(new HashMapOfCacheable());
    m_maps.push_back(hmoc);
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp);
      (m_maps[localcnt])->insert( m_Keys[idx], obj );
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class PutAll : public PutGetTask
{
  AtomicInc m_cnt;
  std::vector < HashMapOfCacheablePtr > &m_maps;
  public:
    PutAll(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
        std::vector < HashMapOfCacheablePtr > &maps,bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad = false)
      :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),m_maps(maps) {
    }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int64_t startTime;
    while ( m_Run && loop-- ) {
      startTime = perfstat[localcnt]->startPut();
      m_Region->putAll( *(m_maps[localcnt].ptr()),60);
      perfstat[localcnt]->endPut(startTime,m_MaxKeys,m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

class UpdateDeltaTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_assetAcSize;
    int32_t m_assetmaxVal;
    UpdateDeltaTask(RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt ,uint32_t size, std::string objectType,
       bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,int32_t assetACsize=0, int32_t assetMaxVal = 0)
       :PutGetTask( reg, keys, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
       m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal) {
      }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    int64_t startTime;
    CacheablePtr obj = NULLPTR;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      startTime = perfstat[localcnt]->startUpdate();
      if(m_encodeKey){ // m_encode works as getBeforeUpdate for delta perf tests
        obj = m_Region->get(m_Keys[idx]);
        if (obj == NULLPTR) {
                char buf[ 2048 ];
                sprintf( buf, "Key has not been created in region %s",
                    m_Region->getName( ) );
                throw gemfire::EntryNotFoundException( buf );
              }
      } else {
        obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal,idx);
      }
      if (instanceOf<DeltaFastAssetAccountPtr> (obj)) {
        DeltaFastAssetAccountPtr obj1 = dynCast<DeltaFastAssetAccountPtr> (obj);
        obj1->update();
        m_Region->put(m_Keys[idx], obj1);
      } else if (instanceOf<DeltaPSTObjectPtr> (obj)) {
        DeltaPSTObjectPtr obj1 = dynCast<DeltaPSTObjectPtr> (obj);
        obj1->update();
        m_Region->put(m_Keys[idx], obj1);

      } else {
        m_Region->put(m_Keys[idx], obj);
      }
      perfstat[localcnt]->endUpdate(startTime, m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

//-----------------------functions written for scale perf tests---------------------------

class CreateDataTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_assetAcSize;
    int32_t m_assetmaxVal;
    FrameworkTest * m_test;
    int32_t m_cid;
    int32_t m_numThreads;
    uint32_t numKeyPerThread;
    CreateDataTask(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,FrameworkTest * test,int32_t assetACsize=0, int32_t assetMaxVal = 0)
      :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
      m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal),m_test(test){
    	 m_cid= m_test->getClientId(); // should be unsigned value. Client should decleared first in the xml
    	 m_test->resetValue("numThreads");
    	 m_numThreads = m_test->getIntValue("numThreads");
    	 numKeyPerThread = m_MaxKeys/m_numThreads;
     }

  virtual bool doSetup( int32_t id ) {
    // per thread iteration offset
    if ( m_Iterations > 0 )
      m_Loop = m_Iterations;
    else
      m_Loop = -1;
    return true;
  }



  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    //uint32_t count = m_MyOffset->value() + (m_MaxKeys*(localcnt+1));
    uint32_t count = m_MyOffset->value() + m_Loop*(localcnt+1);
    uint32_t cnt = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int32_t idx;
    int64_t startTime;
    int32_t startpoint = (m_MaxKeys * (m_cid -1)) + (numKeyPerThread * ((localcnt+1) -1)) +1;
    //int64_t startpoint = (m_MaxKeys * (m_cid -1)) + (numKeyPerThread * (localcnt -1));
    //FWKINFO("rjk m_cid = " << m_cid << " m_numThreads= " << m_numThreads << " numKeyPerThread =" <<numKeyPerThread);
    while ( m_Run && loop-- ) {
        	//idd = m_cid*m_numThreads*numKeyPerThread;
          idx =  startpoint + loop;
          //FWKINFO("rjk CreateDataTask - 2  startpoint " << startpoint <<" idx " << idx << " count = " << count << " loop = " << loop << " id = " <<id  << " keyCnt = "<< m_MaxKeys );
          CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal,idx);
          startTime = perfstat[localcnt]->startCreate();
          m_Region->create( CacheableKey::create(idx), obj );
          //FWKINFO("create key  " << idx );
          perfstat[localcnt]->endCreate(startTime,m_isMainWorkLoad);
          cnt++;
        }
    return ( cnt - m_MyOffset->value() );
  }
};

class GetDataTask : public PutGetTask
  {
   public:
    AtomicInc m_cnt;
    GetDataTask( RegionPtr reg, uint32_t keyCnt ,bool isMainWorkLoad)
      : PutGetTask( reg, keyCnt ,isMainWorkLoad),m_cnt(0) {
      }

    virtual uint32_t doTask( int32_t id )
    {
      int localcnt = m_cnt++;
      uint32_t count = m_MyOffset->value();
      uint32_t loop = m_Loop;
      int64_t startTime;
      int32_t idx;
      while ( m_Run && loop-- ) {
    	idx = count % m_MaxKeys + 1;
        CacheableKeyPtr keyPtr = CacheableKey::create(idx);
        startTime = perfstat[localcnt]->startGet();
        CacheablePtr valPtr = m_Region->get( CacheableKey::create(idx) );
        perfstat[localcnt]->endGet(startTime,m_isMainWorkLoad);
        if (valPtr == NULLPTR) {
          char buf[ 2048 ];
          sprintf( buf, "Could not find key %s in region %s",
              keyPtr->toString( )->asChar( ), m_Region->getName( ) );
          throw gemfire::EntryNotFoundException( buf );
        }
        count++;
      }
      return ( count - m_MyOffset->value() );
    }
  };
class CreateBatchTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_batchSize;
    int32_t m_batchObjSize;
    int32_t m_cid;
    int32_t m_numThreads;
    uint32_t numKeyPerThread;
    FrameworkTest * m_test;
    CreateBatchTask(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,FrameworkTest * test,int32_t batchSize=0, int32_t objsize = 0)
      :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
       m_batchSize(batchSize),m_batchObjSize(objsize),m_test(test){
    	m_cid= m_test->getClientId();
    	m_numThreads = m_test->getIntValue("numThreads");
    	numKeyPerThread = m_MaxKeys/m_numThreads;
     }
    virtual bool doSetup( int32_t id ) {
        // per thread iteration offset
      if ( m_Iterations > 0 )
       m_Loop = m_Iterations;
      else
      m_Loop = -1;
      return true;
  }
  virtual uint32_t doTask( int32_t id )
  {
	int localcnt = m_cnt++;
	uint32_t count = m_MyOffset->value() + m_Loop*(localcnt+1);
	uint32_t cnt = m_MyOffset->value();
	uint32_t loop = m_Loop;
	int32_t idx;
	int64_t startTime;
	int32_t startpoint = (m_MaxKeys * (m_cid -1)) + (numKeyPerThread * ((localcnt+1) -1)) +1;
    while ( m_Run && loop-- ) {
      //idx =  (m_cid*m_numThreads*numKeyPerThread) - count + 1;
      idx =  startpoint + loop;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_batchSize,m_batchObjSize,idx);
      startTime = perfstat[localcnt]->startCreate();
      m_Region->put(  CacheableKey::create(idx), obj );
      perfstat[localcnt]->endCreate(startTime,m_isMainWorkLoad);
      //count--;
      cnt++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class PutDataTask:public PutGetTask
{
  AtomicInc m_cnt;
  int32_t m_opSec;
  int32_t m_assetAcSize;
  int32_t m_assetmaxVal;
  bool isupdateTrim;
  FrameworkTest * m_test;
  public:
  PutDataTask(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
        bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,FrameworkTest * test,int32_t assetACsize=0, int32_t assetMaxVal = 0)
        :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
         m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal),m_test(test) {
	    m_opSec = m_test->getIntValue( "opsSecond" );
	    m_opSec = ( m_opSec < 1 ) ? 0 : m_opSec;
	    isupdateTrim = m_test->getIntValue( "isUpdateTrim" );
      }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int32_t idx;
    int64_t startTime;
    //for the slow put
    PaceMeter meter( m_opSec );
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys +1;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal);
      if(isupdateTrim)
    	  startTime = perfstat[localcnt]->startUpdate();
      else
      startTime = perfstat[localcnt]->startPut();
      m_Region->put( CacheableKey::create(idx), obj );
      if(isupdateTrim)
    	  perfstat[localcnt]->endUpdate(startTime,m_isMainWorkLoad);
      else
        perfstat[localcnt]->endPut(startTime,m_isMainWorkLoad);
      meter.checkPace();
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

class PutBatchTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_batchSize;
    int32_t m_batchObjSize;
    PutBatchTask(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
      bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,int32_t batchSize=0, int32_t objsize = 0)
      :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
      m_batchSize(batchSize),m_batchObjSize(objsize){
     }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int32_t idx;
    int64_t startTime;
    while ( m_Run && loop-- ) {
    	idx = count % m_MaxKeys +1;
      CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_batchSize,m_batchObjSize,idx);
      startTime = perfstat[localcnt]->startPut();
      m_Region->put(CacheableKey::create(idx), obj );
      perfstat[localcnt]->endPut(startTime,m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
class createAllData : public PutGetTask
{
  AtomicInc m_cnt;
  std::vector < HashMapOfCacheablePtr > &m_maps;
  FrameworkTest * m_test;
  int32_t m_cid;
  int32_t m_numThreads;
  uint32_t numKeyPerThread;
  int32_t m_assetAcSize;
  int32_t m_assetmaxVal;
  int32_t m_sizePerThread;
  public:
  createAllData(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
        std::vector < HashMapOfCacheablePtr > &maps, bool encodeKey,bool encodeTimestamp,FrameworkTest * test,bool isMainWorkLoad = false,
        int32_t assetACsize=0, int32_t assetMaxVal = 0)
      :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),m_maps(maps),
       m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal),m_test(test) {
	  	  m_cid= m_test->getClientId(); // should be unsigned value. Client should decleared first in the xml
	      m_numThreads = m_test->getIntValue("numThreads");
	      numKeyPerThread = m_MaxKeys/m_numThreads;
	      m_sizePerThread = m_test->getIntValue("bulkOpMapSize");
    }

  virtual bool doSetup( int32_t id ) {
      // per thread iteration offset
      if ( m_Iterations > 0 )
        m_Loop = m_Iterations;
      else
        m_Loop = -1;
      return true;
    }


  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    HashMapOfCacheablePtr hmoc(new HashMapOfCacheable());
    m_maps.push_back(hmoc);
    uint32_t count = m_MyOffset->value() + m_Loop*(localcnt+1);
    uint32_t cnt = m_MyOffset->value();
    int64_t startTime;
    uint32_t loop = m_Loop;
    int32_t idx;
    int32_t startpoint = (m_MaxKeys * (m_cid -1)) + (numKeyPerThread * ((localcnt+1) -1)) +1;
    while ( m_Run && loop-- ) {
          //idx =  (m_cid*m_numThreads*numKeyPerThread) - count + 1;
      idx =  startpoint + loop;CacheablePtr obj =  ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal);
      if((m_maps[localcnt])->size() == m_sizePerThread)
      {
    	  startTime = perfstat[localcnt]->startCreate();
    	  m_Region->putAll( *(m_maps[localcnt].ptr()));
    	  perfstat[localcnt]->endCreate(startTime,m_isMainWorkLoad);
    	  if(m_maps[localcnt] != NULLPTR)
    	        (m_maps[localcnt])->clear();
      }
      else
      {
    	  (m_maps[localcnt])->insert(CacheableKey::create(idx), obj );
      }
      //count++;
      cnt++;
    }
   return ( cnt - m_MyOffset->value() );
  }
};

class getAllData : public PutGetTask
{
  AtomicInc m_cnt;
  std::vector < HashMapOfCacheablePtr > &m_maps;
  FrameworkTest * m_test;
  int32_t m_assetAcSize;
  int32_t m_assetmaxVal;
  int32_t m_sizePerThread;
  public:
  getAllData(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
        std::vector < HashMapOfCacheablePtr > &maps, bool encodeKey,bool encodeTimestamp,FrameworkTest * test,bool isMainWorkLoad = false,
        int32_t assetACsize=0, int32_t assetMaxVal = 0)
      :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),m_maps(maps),
       m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal),m_test(test) {
	    m_sizePerThread = m_test->getIntValue("bulkOpMapSize");
	  }


  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    HashMapOfCacheablePtr hmoc(new HashMapOfCacheable());
    m_maps.push_back(hmoc);
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int64_t startTime;
    int32_t idx;
    VectorOfCacheableKey keyVec;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys +1;
      CacheablePtr obj =  ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal);
      if((m_maps[localcnt])->size() == m_sizePerThread)
      {
    	  startTime = perfstat[localcnt]->startGet();
    	  m_Region->getAll(keyVec, m_maps[localcnt],NULLPTR);
    	  perfstat[localcnt]->endGetAll(startTime,keyVec.size(), m_isMainWorkLoad);
    	  keyVec.clear();
    	  if(m_maps[localcnt] != NULLPTR)
    	    (m_maps[localcnt])->clear();

      }
      else
      {
    	  keyVec.push_back(CacheableKey::create(idx));
      }
      count++;
    }
   return ( count - m_MyOffset->value() );
  }
};
class putAllData : public PutGetTask
{
  AtomicInc m_cnt;
  std::vector < HashMapOfCacheablePtr > &m_maps;
  FrameworkTest * m_test;
  int32_t m_assetAcSize;
  int32_t m_assetmaxVal;
  int32_t m_sizePerThread;
  public:
  putAllData(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
        std::vector < HashMapOfCacheablePtr > &maps, bool encodeKey,bool encodeTimestamp,FrameworkTest * test,bool isMainWorkLoad = false,
        int32_t assetACsize=0, int32_t assetMaxVal = 0)
      :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),m_maps(maps),
       m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal),m_test(test) {
	    m_sizePerThread = m_test->getIntValue("bulkOpMapSize");
	  }


  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    HashMapOfCacheablePtr hmoc(new HashMapOfCacheable());
    m_maps.push_back(hmoc);
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int64_t startTime;
    int32_t idx;
    while ( m_Run && loop-- ) {
      idx =  count % m_MaxKeys +1;
      CacheablePtr obj =  ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal);
      if((m_maps[localcnt])->size() == m_sizePerThread)
      {
    	  startTime = perfstat[localcnt]->startPut();
    	  m_Region->putAll( *(m_maps[localcnt].ptr()));
    	  perfstat[localcnt]->endPutAll(startTime,(m_maps[localcnt])->size(), m_isMainWorkLoad);
    	  if(m_maps[localcnt] != NULLPTR)
    	     (m_maps[localcnt])->clear();
      }
      else
      {
    	  (m_maps[localcnt])->insert(CacheableKey::create(idx), obj );
      }
      count++;
    }
   return ( count - m_MyOffset->value() );
  }
};

class UpdateScaleDeltaTask : public PutGetTask
{
  public:
    AtomicInc m_cnt;
    int32_t m_assetAcSize;
    int32_t m_assetmaxVal;
    UpdateScaleDeltaTask(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
       bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,int32_t assetACsize=0, int32_t assetMaxVal = 0)
       :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
       m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal) {
      }
  virtual uint32_t doTask( int32_t id )
  {
    int localcnt = m_cnt++;
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    int32_t idx;
    int64_t startTime;
    CacheablePtr obj = NULLPTR;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys + 1;
      startTime = perfstat[localcnt]->startUpdate();
      if(m_encodeKey){ // m_encode works as getBeforeUpdate for delta perf tests
        obj = m_Region->get(idx);
        if (obj == NULLPTR) {
                char buf[ 2048 ];
                sprintf( buf, "Key has not been created in region %s",
                    m_Region->getName( ) );
                throw gemfire::EntryNotFoundException( buf );
              }
      } else {
        obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal,idx);
      }
      if (instanceOf<DeltaFastAssetAccountPtr> (obj)) {
        DeltaFastAssetAccountPtr obj1 = dynCast<DeltaFastAssetAccountPtr> (obj);
        obj1->update();
        m_Region->put(idx, obj1);
      } else if (instanceOf<DeltaPSTObjectPtr> (obj)) {
        DeltaPSTObjectPtr obj1 = dynCast<DeltaPSTObjectPtr> (obj);
        obj1->update();
        m_Region->put(idx, obj1);

      } else {
        m_Region->put(idx, obj);
      }
      perfstat[localcnt]->endUpdate(startTime, m_isMainWorkLoad);
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};

class GetFEDataTask : public PutGetTask
  {
   public:
    AtomicInc m_cnt;
    FrameworkTest * m_test;
    std::string m_funcname;
    GetFEDataTask( RegionPtr reg, uint32_t keyCnt ,bool isMainWorkLoad,FrameworkTest * test)
      : PutGetTask( reg, keyCnt ,isMainWorkLoad),m_cnt(0),m_test(test) {
    	 m_funcname = m_test->getStringValue( "funcName" );
      }

    void verifyFEResult(CacheableVectorPtr exefuncResult, std::string funcName)
    {
    	if (exefuncResult != NULLPTR)
    	{
    	  for(int i = 0; i < exefuncResult->size(); i++)
    	  {
    		  CacheablePtr lastResult = dynCast<CacheablePtr>(exefuncResult->at(i));
    		  if(lastResult == NULLPTR)
    	        throw new Exception("GetFEDataTask:: %s failed, last result is not true",funcName.c_str());
    	  }
    	}
    }

    virtual uint32_t doTask( int32_t id )
        {
          int localcnt = m_cnt++;
          uint32_t count = m_MyOffset->value();
          uint32_t loop = m_Loop;
          int64_t startTime;
          int32_t idx;
          while ( m_Run && loop-- ) {
        	idx = count % m_MaxKeys + 1;
            CacheableKeyPtr keyPtr = CacheableKey::create(idx);
            CacheableVectorPtr filterObj = CacheableVector::create();
            filterObj->push_back(keyPtr);
            ExecutionPtr exc = FunctionService::onRegion(m_Region);
            startTime = perfstat[localcnt]->startGet();
            CacheableVectorPtr executeFunctionResult = exc->withFilter(filterObj)->execute(m_funcname.c_str())->getResult();
            verifyFEResult(executeFunctionResult, "GetFEDataTask::doTask");
            perfstat[localcnt]->endGet(startTime,1,m_isMainWorkLoad);
            count++;
          }
          return ( count - m_MyOffset->value() );
        }
};

class PutFEDataTask : public PutGetTask
  {
   public:
    AtomicInc m_cnt;
    FrameworkTest * m_test;
    int32_t m_assetAcSize;
    int32_t m_assetmaxVal;
    std::string m_funcname;
    PutFEDataTask(RegionPtr reg, uint32_t keyCnt ,uint32_t size, std::string objectType,
            bool encodeKey,bool encodeTimestamp,bool isMainWorkLoad,FrameworkTest * test,int32_t assetACsize=0, int32_t assetMaxVal = 0)
            :PutGetTask( reg, keyCnt ,size,objectType,encodeKey,encodeTimestamp,isMainWorkLoad),m_cnt(0),
             m_assetAcSize(assetACsize),m_assetmaxVal(assetMaxVal),m_test(test) {
    	 m_funcname = m_test->getStringValue( "funcName" );
      }

    void verifyFEResult(CacheableVectorPtr exefuncResult, std::string funcName)
    {
    	if (exefuncResult != NULLPTR)
    	{
    	  for(int i = 0; i < exefuncResult->size(); i++)
    	  {
    		  CacheableBooleanPtr lastResult = dynCast<CacheableBooleanPtr>(exefuncResult->at(i));
    		  if(lastResult->value() != true){
    			  FWKINFO("PutFEDataTask failed due to wrong value return from FE");
    	        throw new Exception("PutFEDataTask:: %s failed, last result is not true",m_funcname.c_str());
    		  }
    	  }
    	}
    }

    virtual uint32_t doTask( int32_t id )
    {
      int localcnt = m_cnt++;
      uint32_t count = m_MyOffset->value();
      uint32_t loop = m_Loop;
      int64_t startTime;
      int32_t idx;
      while ( m_Run && loop-- ) {
    	idx = count % m_MaxKeys + 1;
        CacheableKeyPtr keyPtr = CacheableKey::create(idx);
        CacheableVectorPtr filterObj = CacheableVector::create();
        filterObj->push_back(keyPtr);
        ExecutionPtr exc = FunctionService::onRegion(m_Region);
        CacheableVectorPtr args = CacheableVector::create();
        CacheablePtr obj = ObjectHelper::createObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,m_assetAcSize,m_assetmaxVal);
        args->push_back(obj);
        startTime = perfstat[localcnt]->startPut();
        CacheableVectorPtr executeFunctionResult = exc->withFilter(filterObj)->withArgs(args)->execute(m_funcname.c_str())->getResult();
        verifyFEResult(executeFunctionResult, "PutFEDataTask::doTask");
        perfstat[localcnt]->endPut(startTime,1,m_isMainWorkLoad);
        count++;
      }
      return ( count - m_MyOffset->value() );
    }
};

    }  // perf
  } // testframework
} // gemfire
#endif // __SmokeTasks_hpp__

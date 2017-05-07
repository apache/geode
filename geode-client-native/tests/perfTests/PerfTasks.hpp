/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __PerfTasks_hpp__
#define __PerfTasks_hpp__

#include <GemfireCppCache.hpp>
#include "fwklib/ClientTask.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/FwkLog.hpp"

#include <memory.h>

using namespace gemfire::testframework;

namespace gemfire {
  namespace testframework {
    namespace perf {

class PutGetTask : public ClientTask
{
protected:
  RegionPtr m_Region;
  CacheableKeyPtr * m_Keys;
  CacheableBytesPtr * m_Value;
  uint32_t m_MaxKeys;
  AtomicInc m_Cntr;
  ACE_TSS<perf::Counter> m_count;
  ACE_TSS<perf::Counter> m_MyOffset;

  uint32_t m_iters;

public:
  PutGetTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t max, CacheableBytesPtr * value )
    : m_Region( reg ), m_Keys(keys), m_Value( value ), m_MaxKeys( max ),
    m_MyOffset(), m_iters( 100 )  {}

  PutGetTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t max )
    : m_Region( reg ), m_Keys(keys), m_MaxKeys( max ),
    m_MyOffset(), m_iters( 100 )  {}

  PutGetTask()
    : m_iters( 100 )  {}

  PutGetTask(RegionPtr reg,uint32_t max)
    :m_Region( reg ),m_MaxKeys( max ),m_iters( 100 )  {}
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

class DestroyTask : public PutGetTask
{
public:
  DestroyTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt )
    : PutGetTask( reg, keys, keyCnt ) {}

  virtual uint32_t doTask( int32_t id )
  {
	uint32_t i = 0;
	while (m_Run && i < m_MaxKeys ) {
    try {
      m_Region->destroy( m_Keys[i++]);
    } catch (const EntryNotFoundException & ) {

    }
  }
	return i;
  }
};

class LatencyPutsTask : public PutGetTask
{
  FrameworkTest * m_test;
  int32_t m_opsSec;

public:
  LatencyPutsTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt,
    CacheableBytesPtr * value, FrameworkTest * test, int32_t opsSec )
    : PutGetTask( reg, keys, keyCnt, value ),
    m_test( test ),
    m_opsSec( opsSec ) {}

  virtual uint32_t doTask( int32_t id )
  {
    int32_t count = 0;
    int32_t loop = m_Loop;
    CacheableKeyPtr key = m_Keys[0];
    CacheableBytesPtr value = m_Value[0];
    PaceMeter pm( m_opsSec );
//FWKINFO( "TS Task ready, offset is :" << count << " Loop: " << m_Loop );
    uint8_t * ptr = 0;
    while ( m_Run && loop-- ) {
      ptr = (uint8_t*)value->value();
      *( int32_t * )( ptr ) = LAT_MARK;
      ptr += 4;
      *( int64_t * )( ptr ) = m_test->getAdjustedNowMicros();
      m_Region->put( key, value );
      count++;
      pm.checkPace();
    }
//FWKINFO( "TS Task complete for thread, did iterations: " << ( count - m_MyOffset->value() ) );
    return ( count );
  }
};

class MeteredPutsTask : public PutGetTask
{
  int32_t m_opsSec;

public:
  MeteredPutsTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt, CacheableBytesPtr * value, int32_t opsSec )
    : PutGetTask( reg, keys, keyCnt, value ), m_opsSec( opsSec ) {}

  virtual uint32_t doTask( int32_t id )
  {
    int32_t count = m_MyOffset->value();
    int32_t loop = m_Loop;
    int32_t idx;
    PaceMeter pm( m_opsSec );
//FWKINFO( "TS Task ready, offset is :" << count << " Loop: " << m_Loop );
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      m_Region->put( m_Keys[idx], m_Value[idx] );
      count++;
      pm.checkPace();
    }
//FWKINFO( "TS Task complete for thread, did iterations: " << ( count - m_MyOffset->value() ) );
    return ( count - m_MyOffset->value() );
  }
};

class PutsTask : public PutGetTask
{
public:
  PutsTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt, CacheableBytesPtr * value )
    : PutGetTask( reg, keys, keyCnt, value ) {}

  virtual uint32_t doTask( int32_t id )
  {
    int32_t count = m_MyOffset->value();
    int32_t loop = m_Loop;
    int32_t idx;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
//      FWKINFO("PutsTask done thread id :" << ( uint32_t )( ACE_Thread::self()) << "key = " << m_Keys[idx]->toString( )->asChar( )<< " value=" << m_Value[idx]->toString()->asChar() <<" loop = " << loop << " count = "<< count <<" idx "<< idx);
      m_Region->put( m_Keys[idx], m_Value[idx] );
      count++;
    }
//FWKINFO( "TS Task complete for thread, did iterations: " << ( count - m_MyOffset->value() ) );
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
    int32_t count = m_MyOffset->value();
    int32_t loop = m_Loop;
    int32_t idx;
    HashMapOfCacheable map;
    while ( m_Run && loop-- ) {
      idx = count % m_MaxKeys;
      FWKINFO("inserting key  " << m_Keys[idx]->toString( )->asChar( ) << " and value " <<
                m_Value[idx]->toString( )->asChar( ) << " count is " << count );
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
  GetsTask( RegionPtr reg, CacheableKeyPtr * keys, uint32_t keyCnt )
    : PutGetTask( reg, keys, keyCnt ) {}

  virtual uint32_t doTask( int32_t id )
  {
    int32_t count = m_MyOffset->value();
    int32_t loop = m_Loop;
    while ( m_Run && loop-- ) {
      CacheableKeyPtr keyPtr = m_Keys[count % m_MaxKeys];
      CacheablePtr valPtr = m_Region->get( keyPtr );
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
class QueryTask : public PutGetTask
{
  std::string m_queryString;
  std::string m_queryType;
  int32_t m_resultSize;
  FrameworkTest * m_test;

public:
  QueryTask( RegionPtr reg,uint32_t max, std::string queryString, std::string queryType ,int32_t resultSize,FrameworkTest * test)
    : PutGetTask( reg,max),m_queryString(queryString),m_queryType(queryType),m_resultSize(resultSize),m_test(test) {}

  virtual uint32_t doTask( int32_t id )
  {
    int32_t count = m_MyOffset->value();
    int32_t loop = m_Loop;
    char buf[1024];
    while ( m_Run && loop-- ) {
      sprintf(buf,"%s %d", m_queryString.c_str(), loop);
      QueryServicePtr qs=m_test->checkQueryService();
      QueryPtr q = qs->newQuery(buf);
      SelectResultsPtr sptr = q->execute();
      if(m_resultSize != (int32_t)sptr->size()){
        FWKSEVERE(" result size found is "<< sptr->size() << " and expected result size is " << m_resultSize);
      }
      count++;
    }
    return ( count - m_MyOffset->value() );
  }
};
    }  // perf
  } // testframework
} // gemfire
#endif // __PerfTasks_hpp__

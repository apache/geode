/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
  * @file    PerfInternalTest.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "PerfTest.hpp"

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"

#include "fwklib/FwkExport.hpp"

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;


extern PerfTest* g_test;

#ifndef _SOLARIS
TESTTASK doByteArrayConcat( const char* taskId )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doByteArrayConcat called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->byteArrayConcat( );
  } catch ( const FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doByteArrayConcat caught exception: " << ex.getMessage( ) );
  }

  return result;
}
#endif

TESTTASK doQueuePopulate( const char* taskId )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doQueuePopulate called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->queuePopulate( );
  } catch ( const FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doQueuePopulate caught exception: " << ex.getMessage( ) );
  }

  return result;
}

TESTTASK doQueueGetPut( const char* taskId )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doQueueGetPut called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->queueGetPut( );
  } catch ( const FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doQueueGetPut caught exception: " << ex.getMessage( ) );
  }

  return result;
}

TESTTASK doQueueClean( const char* taskId )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doQueueClean called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->queueClean( );
  } catch ( const FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doQueueClean caught exception: " << ex.getMessage( ) );
  }

  return result;
}

TESTTASK doAtomicIncDec( const char* taskId )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doAtomicIncDec called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->atomicIncDec( );
  } catch ( const FwkException& ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doAtomicIncDec caught exception: " << ex.getMessage( ) );
  }

  return result;
}

// ----------------------------------------------------------------------------
// Performance comparison of std::vector, std::deque, std::slist, std::rope
// and DataOutput for concatenation of byte arrays (e.g. in chunked response)
// ----------------------------------------------------------------------------

#ifndef _SOLARIS
#include <vector>
#include <deque>
#ifdef _LINUX
#include <ext/slist>
#include <ext/rope>
#define SLIST __gnu_cxx::slist
#define CROPE __gnu_cxx::crope
#endif
#ifdef _SOLARIS
#include <slist>
#include <rope>
#define SLIST std::slist
#define CROPE std::crope
#endif

int32_t PerfTest::byteArrayConcat( )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::byteArrayConcat()" );

  try {
    int32_t arraySize;
    int32_t totalSize;
    while ( ( arraySize = getIntValue( "arraySize" ) ) > 0 ) {
      while ( ( totalSize = getIntValue( "totalSize" ) ) > 0 ) {
        int32_t numArrays = totalSize / arraySize;
        int32_t minSize = arraySize / 5;
        int32_t maxSize = arraySize * 2;

        std::string algo;
        while ( ( algo = getStringValue( "algorithm" ) ), algo.size( ) > 0 ) {
          FWKINFO( "PerfTest::byteArrayConcat[" << algo << "] using " <<
              numArrays << " arrays of size " << minSize << '-' << maxSize );

          ACE_Time_Value startTime = ACE_OS::gettimeofday( );
          if ( algo == "vector" ) {
            byteArrayVec( minSize, maxSize, numArrays );
          } else if ( algo == "deque" ) {
            byteArrayDeque( minSize, maxSize, numArrays );
          } else if ( algo == "slist" ) {
#ifdef _WIN32
            FWKINFO( "Skipping slist on Win32." );
#else
            byteArraySList( minSize, maxSize, numArrays );
#endif
          } else if ( algo == "rope" ) {
#ifdef _WIN32
            FWKINFO( "Skipping rope on Win32." );
#else
            byteArrayRope( minSize, maxSize, numArrays );
#endif
          } else if ( algo == "string" ) {
            byteArrayString( minSize, maxSize, numArrays );
          } else if ( algo == "DataOutput" ) {
            byteArrayDOut( minSize, maxSize, numArrays );
          } else {
            FWKSEVERE( "PerfTest::byteArrayConcat() Unknown algo " << algo );
            return FWK_SEVERE;
          }
          ACE_Time_Value timeElapsed = ACE_OS::gettimeofday( ) - startTime;
          FWKINFO( "PerfTest::byteArrayConcat[" << algo << "] Elapsed time: " <<
              ( timeElapsed.sec( ) * 1000000 + timeElapsed.usec( ) ) <<
              " micros." );
        }
      }
    }
  } catch ( const Exception& ex ) {
    FWKSEVERE( "PerfTest::byteArrayConcat() Caught Exception: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( const FwkException& ex ) {
    FWKSEVERE( "PerfTest::byteArrayConcat() Caught FwkException: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::byteArrayConcat() Caught unknown exception." );
    result = FWK_SEVERE;
  }
  FWKINFO( "PerfTest::byteArrayConcat() complete." );

  return result;
}

void PerfTest::byteArrayVec( int32_t minSize, int32_t maxSize, int32_t numArrays )
{
  std::vector< char* > vec;
  std::vector< int32_t > sizeVec;
  int32_t totSiz = 0;
  for ( int32_t arrayNum = 0; arrayNum < numArrays; arrayNum++ ) {
    uint32_t siz = GsRandom::random( (uint32_t)minSize, (uint32_t)maxSize );
    if ( siz < 20 ) {
      siz = 20;
    }
    char* buf = new char[ siz ];
    memset( buf, 'V', siz );
    GsRandom::getAlphanumericString( siz, buf );
    vec.push_back( buf );
    sizeVec.push_back( siz );
    totSiz += siz;
  }
  char* fullBuf = new char[ totSiz ];
  int32_t currSiz = 0;
  std::vector< int32_t >::const_iterator sizeIter = sizeVec.begin( );
  for ( std::vector< char* >::const_iterator iter = vec.begin( );
      iter != vec.end( ); ++iter, ++sizeIter ) {
    int32_t siz = *sizeIter;
    memcpy( fullBuf + currSiz, *iter, siz );
    delete [] *iter;
    currSiz += siz;
  }
  delete [] fullBuf;
}

void PerfTest::byteArrayDeque( int32_t minSize, int32_t maxSize, int32_t numArrays )
{
  std::deque< char* > deq;
  std::vector< int32_t > sizeVec;
  int32_t totSiz = 0;
  for ( int32_t arrayNum = 0; arrayNum < numArrays; arrayNum++ ) {
    uint32_t siz = GsRandom::random( (uint32_t)minSize, (uint32_t)maxSize );
    if ( siz < 20 ) {
      siz = 20;
    }
    char* buf = new char[ siz ];
    memset( buf, 'V', siz );
    GsRandom::getAlphanumericString( siz, buf );
    deq.push_back( buf );
    sizeVec.push_back( siz );
    totSiz += siz;
  }
  char* fullBuf = new char[ totSiz ];
  int32_t currSiz = 0;
  std::vector< int32_t >::const_iterator sizeIter = sizeVec.begin( );
  for ( std::deque< char* >::const_iterator iter = deq.begin( );
      iter != deq.end( ); ++iter, ++sizeIter ) {
    int32_t siz = *sizeIter;
    memcpy( fullBuf + currSiz, *iter, siz );
    delete [] *iter;
    currSiz += siz;
  }
  delete [] fullBuf;
}

#ifndef _WIN32

void PerfTest::byteArraySList( int32_t minSize, int32_t maxSize, int32_t numArrays )
{
  SLIST< char* > lst;
  std::vector< int32_t > sizeVec;
  int32_t totSiz = 0;
  for ( int32_t arrayNum = 0; arrayNum < numArrays; arrayNum++ ) {
    uint32_t siz = GsRandom::random( (uint32_t)minSize, (uint32_t)maxSize );
    if ( siz < 20 ) {
      siz = 20;
    }
    char* buf = new char[ siz ];
    memset( buf, 'V', siz );
    GsRandom::getAlphanumericString( siz, buf );
    lst.push_front( buf );
    sizeVec.push_back( siz );
    totSiz += siz;
  }
  char* fullBuf = new char[ totSiz ];
  int32_t currSiz = 0;
  std::vector< int32_t >::const_reverse_iterator sizeIter = sizeVec.rbegin( );
  for ( SLIST< char* >::const_iterator iter = lst.begin( );
      iter != lst.end( ); ++iter, ++sizeIter ) {
    int32_t siz = *sizeIter;
    currSiz += siz;
    memcpy( fullBuf + totSiz - currSiz, *iter, siz );
    delete [] *iter;
  }
  delete [] fullBuf;
}

void PerfTest::byteArrayRope( int32_t minSize, int32_t maxSize, int32_t numArrays )
{
  CROPE rpe;
  for ( int32_t arrayNum = 0; arrayNum < numArrays; arrayNum++ ) {
    uint32_t siz = GsRandom::random( (uint32_t)minSize, (uint32_t)maxSize );
    if ( siz < 20 ) {
      siz = 20;
    }
    char* buf = new char[ siz ];
    memset( buf, 'V', siz );
    GsRandom::getAlphanumericString( siz, buf );
    rpe.append( buf, siz );
    delete [] buf;
  }
}

#endif

void PerfTest::byteArrayString( int32_t minSize, int32_t maxSize, int32_t numArrays )
{
  std::string str;
  for ( int32_t arrayNum = 0; arrayNum < numArrays; arrayNum++ ) {
    uint32_t siz = GsRandom::random( (uint32_t)minSize, (uint32_t)maxSize );
    if ( siz < 20 ) {
      siz = 20;
    }
    char* buf = new char[ siz ];
    memset( buf, 'V', siz );
    GsRandom::getAlphanumericString( siz, buf );
    str.append( buf, siz );
    delete [] buf;
  }
}

void PerfTest::byteArrayDOut( int32_t minSize, int32_t maxSize, int32_t numArrays )
{
  DataOutput output;
  for ( int32_t arrayNum = 0; arrayNum < numArrays; arrayNum++ ) {
    uint32_t siz = GsRandom::random( (uint32_t)minSize, (uint32_t)maxSize );
    if ( siz < 20 ) {
      siz = 20;
    }
    char* buf = new char[ siz ];
    memset( buf, 'V', siz );
    GsRandom::getAlphanumericString( siz, buf );
    output.writeBytesOnly( (uint8_t*)buf, siz );
    delete [] buf;
  }
}
#endif

// ----------------------------------------------------------------------------
// End performance comparison of std::vector, std::deque, std::slist, std::rope
// and DataOutput
// ----------------------------------------------------------------------------


// ----------------------------------------------------------------------------
// Performance comparison of different FairQueue implementations.
// ----------------------------------------------------------------------------

template < typename FQ, typename TObj, int32_t MAX_WAIT_SECS = 5 >
class QueueGetPutTask : public ClientTask
{
  public:

    QueueGetPutTask( int32_t sleepMs )
      : m_sleepMs( sleepMs )
    {
      FWKINFO( "PerfTest::QueueGetPutTask() set the sleepMillis to " <<
          sleepMs );
    }

    virtual bool doSetup( int32_t id ) { return true; }

    virtual uint32_t doTask( int32_t id )
    {
      int32_t loop = -1;
      int32_t count = 0;
      if ( m_Iterations > 0 ) {
        loop = m_Iterations;
      }
      while ( m_Run && loop-- ) {
        TObj* obj = s_queue.getUntil( MAX_WAIT_SECS );
        if ( obj == NULL ) {
          FWKEXCEPTION( "PerfTest::QueueGetPutTask() failed in get from queue" );
          break;
        } else {
          // Do some processing here...
          if ( m_sleepMs > 0 ) {
            perf::sleepMillis( m_sleepMs );
          }
          s_queue.put( obj );
        }
        count++;
      }
      return count;
    }

    virtual void doCleanup( int32_t id ) { }

    static void queuePut( TObj* obj )
    {
      s_queue.put( obj );
    }

    static void queueClean( )
    {
      while ( !s_queue.empty( ) ) {
        delete s_queue.getUntil( MAX_WAIT_SECS );
      }
    }

    static int queueSize( )
    {
      return s_queue.size( );
    }

    virtual ~QueueGetPutTask( ) { }


  private:

    int32_t m_sleepMs;

    static FQ s_queue;
};


#ifndef _SOLARIS

#define FairQueue FairQueue1
#include "FairQueue1.hpp"
#undef FairQueue
#define FairQueue FairQueue2
#include "FairQueue2.hpp"
#undef FairQueue

typedef QueueGetPutTask< gemfire::FairQueue1< std::string >, std::string > Queue1GetPutTask;
typedef QueueGetPutTask< gemfire::FairQueue2< std::string >, std::string > Queue2GetPutTask;
#ifdef __GNUC__
template gemfire::FairQueue1< std::string > Queue1GetPutTask::s_queue;
template <> gemfire::FairQueue1< std::string > Queue1GetPutTask::s_queue;
template gemfire::FairQueue2< std::string > Queue2GetPutTask::s_queue;
template <> gemfire::FairQueue2< std::string > Queue2GetPutTask::s_queue;
#else
gemfire::FairQueue1< std::string > Queue1GetPutTask::s_queue;
gemfire::FairQueue2< std::string > Queue2GetPutTask::s_queue;
#endif

#endif

#define FairQueue FairQueueSol1
#include "FairQueueSol1.hpp"
#undef FairQueue
#define FairQueue FairQueueSol2
#include "FairQueueSol2.hpp"
#undef FairQueue

#include <sstream>

typedef QueueGetPutTask< gemfire::FairQueueSol1< std::string >, std::string > Queue3GetPutTask;
typedef QueueGetPutTask< gemfire::FairQueueSol2< std::string >, std::string > Queue4GetPutTask;
// TODO jbarrett - this is completely broken on solaris
//#if defined(__GNUC__)
//template gemfire::FairQueueSol1< std::string > Queue3GetPutTask::s_queue;
template <> gemfire::FairQueueSol1< std::string > Queue3GetPutTask::s_queue;
//template gemfire::FairQueueSol2< std::string > Queue4GetPutTask::s_queue;
template <> gemfire::FairQueueSol2< std::string > Queue4GetPutTask::s_queue;
//#else
//gemfire::FairQueueSol1< std::string > Queue3GetPutTask::s_queue;
//gemfire::FairQueueSol2< std::string > Queue4GetPutTask::s_queue;
//#endif

 
int32_t PerfTest::queuePopulate( )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::queuePopulate()" );

  try {
    int32_t numObjects = getIntValue( "numObjects" );
    FWKINFO( "PerfTest::queuePopulate populating " << numObjects << " strings" );

    std::stringstream ss;
    for ( int32_t objectNum = 0; objectNum < numObjects; objectNum++ ) {
#ifndef _SOLARIS
      ss.str( "" );
      ss << "This is object1 number " << objectNum;
      Queue1GetPutTask::queuePut( new std::string( ss.str( ) ) );
      ss.str( "" );
      ss << "This is object2 number " << objectNum;
      Queue2GetPutTask::queuePut( new std::string( ss.str( ) ) );
#endif
      ss.str( "" );
      ss << "This is object3 number " << objectNum;
      Queue3GetPutTask::queuePut( new std::string( ss.str( ) ) );
      ss.str( "" );
      ss << "This is object4 number " << objectNum;
      Queue4GetPutTask::queuePut( new std::string( ss.str( ) ) );
    }
#ifndef _SOLARIS
    FWKINFO( "Number of objects in queue1: " << Queue1GetPutTask::queueSize( ) );
    FWKINFO( "Number of objects in queue2: " << Queue2GetPutTask::queueSize( ) );
#endif
    FWKINFO( "Number of objects in queue3: " << Queue3GetPutTask::queueSize( ) );
    FWKINFO( "Number of objects in queue4: " << Queue4GetPutTask::queueSize( ) );
  } catch ( const Exception& ex ) {
    FWKSEVERE( "PerfTest::queuePopulate() Caught Exception: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( const FwkException& ex ) {
    FWKSEVERE( "PerfTest::queuePopulate() Caught FwkException: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::queuePopulate() Caught unknown exception." );
    result = FWK_SEVERE;
  }
  FWKINFO( "PerfTest::queuePopulate() complete." );

  return result;
}

void PerfTest::queueGetPutP( ClientTask* task, const char* taskName,
    int32_t numOps, int32_t numThreads )
{
  TestClient* clnt = TestClient::getTestClient( );

  FWKINFO( "PerfTest::queueGetPutP[" << taskName << "] performing " <<
      numOps << " ops with " << numThreads << " threads" );

  m_PerfSuite.setName( taskName );
  m_PerfSuite.setAction( "getsPuts" );
  m_PerfSuite.setNumKeys( 1 );
  m_PerfSuite.setNumClients( 1 );
  m_PerfSuite.setValueSize( 10 );
  m_PerfSuite.setNumThreads( numThreads );

  FWKINFO( "Running timed task for " << m_PerfSuite.asString( ) );
  if ( !clnt->timeIterations( task, numOps, numThreads, 10000 ) ) {
    FWKEXCEPTION( "In queueGetPutP[" << taskName << "]( ) Run timed out." );
  }
  m_PerfSuite.addRecord( task->getIters( ), (uint32_t)clnt->getTotalMicros( ) );
}

int32_t PerfTest::queueGetPut( )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::queueGetPut()" );

  try {
    int32_t sleepMs;
    int32_t numOps;
    int32_t numThreads;
    while ( ( sleepMs = getIntValue( "sleepMillis" ) ) >= 0 ) {
      while ( ( numOps = getIntValue( "numOps" ) ) > 0 ) {
        while ( ( numThreads = getIntValue( "numThreads" ) ) > 0 ) {
          ClientTask* task;

#ifndef _SOLARIS
          task = new Queue1GetPutTask( sleepMs );
          queueGetPutP( task, "FairQueue1", numOps, numThreads );
          delete task;

          task = new Queue2GetPutTask( sleepMs );
          queueGetPutP( task, "FairQueue2", numOps, numThreads );
          delete task;

#endif
          task = new Queue3GetPutTask( sleepMs );
          queueGetPutP( task, "FairQueue3", numOps, numThreads );
          delete task;

          task = new Queue4GetPutTask( sleepMs );
          queueGetPutP( task, "FairQueue4", numOps, numThreads );
          delete task;

          // real work complete for this pass thru the loop
        } // number of threads loop
      } // number of ops loop
    } // sleep time loop
  } catch ( const Exception& ex ) {
    FWKSEVERE( "PerfTest::queueGetPut() Caught Exception: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( const FwkException& ex ) {
    FWKSEVERE( "PerfTest::queueGetPut() Caught FwkException: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::queueGetPut() Caught unknown exception." );
    result = FWK_SEVERE;
  }
  FWKINFO( "PerfTest::queueGetPut() complete." );

  return result;
}

int32_t PerfTest::queueClean( )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::queueClean()" );

  try {
    FWKINFO( "PerfTest::queueClean cleaning the queues" );

#ifndef _SOLARIS
    Queue1GetPutTask::queueClean( );
    Queue2GetPutTask::queueClean( );
#endif
    Queue3GetPutTask::queueClean( );
    Queue4GetPutTask::queueClean( );
  } catch ( const Exception& ex ) {
    FWKSEVERE( "PerfTest::queueClean() Caught Exception: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( const FwkException& ex ) {
    FWKSEVERE( "PerfTest::queueClean() Caught FwkException: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::queueClean() Caught unknown exception." );
    result = FWK_SEVERE;
  }
  FWKINFO( "PerfTest::queueClean() complete." );

  return result;
}

// ----------------------------------------------------------------------------
// End performance comparison of different FairQueue implementations.
// ----------------------------------------------------------------------------


// Test whether atomic increment/decrement works correctly

class AtomicIncDecTask: public ClientTask
{
  public:

    AtomicIncDecTask( int32_t sleepMs, int32_t numThreads )
      : m_sleepMs( sleepMs ), m_numThreads( numThreads ) { }

    virtual bool doSetup( int32_t id ) { return true; }

    virtual uint32_t doTask( int32_t id )
    {
      int32_t loop = -1;
      int32_t count = 0;
      if ( m_Iterations > 0 ) {
        loop = m_Iterations;
      }
      FWKINFO( "AtomicIncDecTask: starting for " << loop <<
          " iterations with " << m_numThreads <<
          " threads and sleepMillis " << m_sleepMs );
      while ( m_Run && loop-- ) {
        HostAsm::atomicAdd( s_value, 1 );
        // Wait for some time here...
        if ( m_sleepMs > 0 ) {
          perf::sleepMillis( m_sleepMs );
        }
        int32_t newValue = HostAsm::atomicAdd( s_value, -1 );
        if ( newValue <= 0 ) {
          FWKEXCEPTION( "AtomicIncDecTask: failing due to value " <<
              newValue << " going below 1." );
        }
        if ( newValue > m_numThreads ) {
          FWKEXCEPTION( "AtomicIncDecTask: failing due to value " <<
              newValue << " going above numThreads " << m_numThreads );
        }
        count++;
      }
      FWKINFO( "AtomicIncDecTask: completed " << count << " iterations." );
      return count;
    }

    virtual void doCleanup( int32_t id ) { }

    virtual ~AtomicIncDecTask( ) { }


  private:

    int32_t m_sleepMs;
    int32_t m_numThreads;

    static int32_t s_value;
};

int32_t AtomicIncDecTask::s_value = 1;


int32_t PerfTest::atomicIncDec( )
{
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In PerfTest::atomicIncDec()" );

  try {
    TestClient* clnt = TestClient::getTestClient( );

    int32_t sleepMs;
    int32_t numThreads;
    int32_t timedInterval = getTimeValue( "timedInterval" );
    while ( ( sleepMs = getIntValue( "sleepMillis" ) ) >= 0 ) {
      while ( ( numThreads = getIntValue( "numThreads" ) ) > 0 ) {

        FWKINFO( "PerfTest::atomicIncDec running for " << timedInterval <<
            " secs with " << numThreads << " threads and sleepMills " <<
            sleepMs );

        m_PerfSuite.setName( "atomicIncDec" );
        m_PerfSuite.setAction( "incrementDecrement" );
        m_PerfSuite.setNumKeys( 1 );
        m_PerfSuite.setNumClients( 1 );
        m_PerfSuite.setValueSize( 10 );
        m_PerfSuite.setNumThreads( numThreads );

        AtomicIncDecTask* task = new AtomicIncDecTask( sleepMs,
            numThreads );
        FWKINFO( "Running timed task for " << m_PerfSuite.asString( ) );
        if ( !clnt->timeInterval( task, timedInterval, numThreads,
              10 * timedInterval ) ) {
          FWKEXCEPTION( "In atomicIncDec() Timed run timed out." );
        }
        m_PerfSuite.addRecord( task->getIters( ),
            (uint32_t)clnt->getTotalMicros( ) );

        delete task;
        // real work complete for this pass thru the loop
      } // thread loop
    } // sleep loop
  } catch ( const Exception& ex ) {
    FWKSEVERE( "PerfTest::atomicIncDec() Caught Exception: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( const FwkException& ex ) {
    FWKSEVERE( "PerfTest::atomicIncDec() Caught FwkException: " <<
        ex.getMessage( ) );
    result = FWK_SEVERE;
  } catch ( ... ) {
    FWKSEVERE( "PerfTest::atomicIncDec() Caught unknown exception." );
    result = FWK_SEVERE;
  }
  FWKINFO( "PerfTest::atomicIncDec() complete." );

  return result;
}


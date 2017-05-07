/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    delta.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __DELTA_TEST_HPP__
#define __DELTA_TEST_HPP__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"

#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Region.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "fwklib/ClientTask.hpp"
#include "fwklib/FwkLog.hpp"
#include "CacheableString.hpp"
#include "testobject/DeltaTestImpl.hpp"

using namespace testobject;
namespace gemfire {
 namespace testframework {
   namespace deltatest {

class PutGetTask: public ClientTask {
protected:
  RegionPtr m_Region;
  uint32_t m_MaxKeys;
  FrameworkTest * m_test;
  AtomicInc m_Cntr;
  ACE_TSS<perf::Counter> m_count;
  ACE_TSS<perf::Counter> m_MyOffset;

  uint32_t m_iters;

public:

  PutGetTask(RegionPtr reg, uint32_t max, FrameworkTest * test) :
    m_Region(reg), m_MaxKeys(max), m_test(test),m_MyOffset(), m_iters(100) {
  }

  PutGetTask(RegionPtr reg) :
    m_Region(reg), m_iters(100) {
  }
  PutGetTask() :
    m_iters(100) {
  }

  virtual bool doSetup(int32_t id) {
    // per thread iteration offset
    double max = m_MaxKeys;
    srand((++m_Cntr * id) + (unsigned int)time(0));
    m_MyOffset->add((int) (((max * rand()) / (RAND_MAX + 1.0))));
    if (m_Iterations > 0)
      m_Loop = m_Iterations;
    else
      m_Loop = -1;
    return true;
  }

  virtual void doCleanup(int32_t id) {
  }
  virtual ~PutGetTask() {
  }
};
class CreateTask: public PutGetTask {
  int32_t m_intVal;
  AtomicInc m_cnt;
  AtomicInc m_create;
public:
    CreateTask(RegionPtr reg, uint32_t keyCnt,FrameworkTest * test,
     int32_t intVal = 0) :
    PutGetTask(reg, keyCnt,test), m_intVal(intVal), m_cnt(0), m_create(0){
  }
  virtual uint32_t doTask(int32_t id) {
    int localcnt = 0;
    char buf[128];
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    while (m_Run && loop--) {
      idx = count % m_MaxKeys;
      sprintf(buf, "%s%d%010d", "AAAAAA", localcnt, idx);
      CacheableKeyPtr key = CacheableKey::create(buf);
      DeltaTestImplPtr obj(new DeltaTestImpl(0, CacheableString::create("delta")));
      m_Region->create(key, obj);
      m_create++;
      count++;
    }
    return (count - m_MyOffset->value());
  }
  void dumpToBB() {
    m_test->bbSet("DeltaBB", "CREATECOUNT", m_create.value() );
  }
};

class PutTask: public PutGetTask {
  AtomicInc m_cnt;
  bool m_isCreate;
  AtomicInc m_update;

public:
  PutTask(RegionPtr reg, uint32_t keyCnt, FrameworkTest * test, bool isCreate = true) :
    PutGetTask(reg, keyCnt,test), m_cnt(0), m_isCreate(isCreate), m_update(0) {
  }
  virtual uint32_t doTask(int32_t id) {
    int localcnt =0;
    char buf[128];
    uint32_t count = m_MyOffset->value();
    uint32_t loop = m_Loop;
    uint32_t idx;
    DeltaTestImplPtr obj=NULLPTR;
    while (m_Run && loop--) {
      idx = count % m_MaxKeys;
      sprintf(buf, "%s%d%010d", "AAAAAA", localcnt, idx);
      CacheableKeyPtr key = CacheableKey::create(buf);
      DeltaTestImplPtr oldVal = dynCast<DeltaTestImplPtr> (m_Region->get(key));
      if (oldVal == NULLPTR) {
        FWKEXCEPTION("oldDelta cannot be null");
      }
      obj = new DeltaTestImpl(oldVal);
      obj->setIntVar(oldVal->getIntVar() + 1);
      m_Region->put(key, obj);
      m_test->bbSet("ToDeltaBB",key->toString()->asChar(),obj->getToDeltaCounter());
      m_update++;

      //FWKINFO("put key done " << key->toString( )->asChar( ) << " old value = " << oldVal->getIntVar());// << "thread id :" << ( uint32_t )( ACE_Thread::self()));
      //FWKINFO("put key  " << m_Keys[idx]->toString( )->asChar( ) << " and value ");
      //        << obj->value( ) << " count is " << count );*/
      count++;
    }
    return (count - m_MyOffset->value());
  }
  void dumpToBB() {
    m_test->bbSet("DeltaBB", "UPDATECOUNT", m_update.value());
  }
};

class EntryTask:public PutGetTask {
  int32_t m_intVal;
  AtomicInc m_cnt;
  FrameworkTest * m_test;
  AtomicInc m_create;
  AtomicInc m_destroy;
  AtomicInc m_invalidate;
  AtomicInc m_update;
  bool m_isCreate;
  bool m_isDestroy;
  bool m_isInvalidate;
  ACE_Recursive_Thread_Mutex m_lock;
public:
  EntryTask(RegionPtr reg, uint32_t keyCnt, FrameworkTest * test,int32_t intVal = 0, bool isCreate = true,bool isDestroy=true,bool isInvalidate=true):
  PutGetTask(reg,keyCnt,test), m_intVal(intVal),m_cnt(0),m_test(test),m_create(0), m_isCreate(isCreate),m_isDestroy(isDestroy),m_isInvalidate(isInvalidate){}

  virtual uint32_t doTask(int32_t id);

  void dumpToBB() {

    int64 createCnt = m_test->bbGet("DeltaBB", "CREATECOUNT");
    int64 updateCnt = m_test->bbGet("DeltaBB", "UPDATECOUNT");
    int64 destroyCnt = m_test->bbGet("DeltaBB", "DESTROYCOUNT");
    int64 invalidateCnt = m_test->bbGet("DeltaBB", "INVALIDATECOUNT");
    m_test->bbSet("DeltaBB", "CREATECOUNT", createCnt + m_create.value() );
    m_test->bbSet("DeltaBB", "UPDATECOUNT", updateCnt + m_update.value());
    m_test->bbSet("DeltaBB", "DESTROYCOUNT", destroyCnt + m_destroy.value());
    m_test->bbSet("DeltaBB", "INVALIDATECOUNT", invalidateCnt + m_invalidate.value());
  }
};

class DeltaClientValidationListener: public CacheListener {
  HashMapOfCacheable m_latestValues;
  HashMapOfCacheable m_ValidateMap;
  long m_numAfterCreate;
  long m_numAfterUpdate;
  long m_numAfterInvalidate;
  long m_numAfterDestroy;
  const FrameworkTest * m_test;
  void dumpToBB(const RegionPtr&);
public:
  DeltaClientValidationListener(const FrameworkTest * test);
  virtual ~DeltaClientValidationListener() {
  }
  void afterCreate(const EntryEvent& event);

  void afterUpdate(const EntryEvent& event);

  void afterDestroy(const EntryEvent& event);

  void afterInvalidate(const EntryEvent& event);

  HashMapOfCacheable getMap();
  void validateIncrementByOne(CacheableKeyPtr key, DeltaTestImplPtr newValue);
  ACE_Recursive_Thread_Mutex m_lock;
  virtual void afterRegionDestroy( const RegionEvent& event )
  {
    dumpToBB(event.getRegion());
  }
};
typedef SharedPtr< DeltaClientValidationListener > DeltaClientValidationListenerPtr;
class DeltaTest: public FrameworkTest {
public:
  DeltaTest(const char * initArgs) :
    FrameworkTest(initArgs),
    m_KeysA( NULL ),
    m_MaxKeys( 0 ),
    m_KeyIndexBegin(0),
    m_MaxValues( 0 ),
    m_CValue( NULL ),
    m_isObjectRegistered(false){}

  virtual ~DeltaTest(void) {
  }
  int32_t createRegion();
  int32_t puts();
  int32_t populateRegion();
  int32_t createPools();
  int32_t registerAllKeys();
  void checkTest(const char * taskId,bool ispool=false);
  void getClientSecurityParams(PropertiesPtr prop, std::string credentials);
  int32_t validateDeltaTest();
  int32_t doEntryOperation();
private:
  int32_t initKeys(bool useDefault = true, bool useAllClientID = false);
  void clearKeys();
  void initStrKeys(int32_t low, int32_t high, const std::string & keyBase,
      uint32_t clientId, bool useAllClientID = false);
  void initIntKeys(int32_t low, int32_t high);
  RegionPtr getRegionPtr(const char * reg = NULL);
  bool checkReady(int32_t numClients);
  CacheableKeyPtr * m_KeysA;
  int32_t m_MaxKeys;
  int32_t m_KeyIndexBegin;
  int32_t m_MaxValues;

  CacheableBytesPtr * m_CValue;
  bool m_isObjectRegistered;
};

 } //   namespace deltatest
 } // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __DELTA_TEST_HPP__



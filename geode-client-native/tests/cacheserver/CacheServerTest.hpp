/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    CacheServerTest.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __CacheServerTest_hpp__
#define __CacheServerTest_hpp__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/QueryHelper.hpp"
#include "CqAttributesFactory.hpp"
#include "CqAttributes.hpp"
#include "CqListener.hpp"
#include "CqQuery.hpp"
#include "ExpectedRegionContents.hpp"
#include "testobject/PdxClassV1.hpp"
#include "testobject/PdxClassV2.hpp"
#include "testobject/PdxType.hpp"
#include "testobject/PdxVersioned1.hpp"
#include "testobject/PdxVersioned2.hpp"
#include "testobject/VariousPdxTypes.hpp"
#include "testobject/NestedPdxObject.hpp"
#include "pdxautoserializerclass/AutoPdxVersioned1.hpp"
#include "pdxautoserializerclass/AutoPdxVersioned2.hpp"

#include <vector>
using namespace AutoPdxTests;
namespace gemfire {
namespace testframework {
//----------------------------------------------------------------------------
static const int INVALIDATE             = 1;
static const int LOCAL_INVALIDATE       = 2;
static const int DESTROY                = 3;
static const int LOCAL_DESTROY          = 4;
static const int UPDATE_EXISTING_KEY    = 5;
static const int GET                    = 6;
static const int ADD_NEW_KEY            = 7;
static const int PUTALL_NEW_KEY         = 8;
static const int NUM_EXTRA_KEYS = 100;
static const int operations[] = {INVALIDATE, LOCAL_INVALIDATE, DESTROY, LOCAL_DESTROY, UPDATE_EXISTING_KEY, GET, ADD_NEW_KEY, PUTALL_NEW_KEY};

class DoOpsTask: public ClientTask {
  protected:
    RegionPtr regionPtr;
    FrameworkTest * m_test;
    AtomicInc m_Cntr;
    ACE_TSS<perf::Counter> m_count;
    ACE_TSS<perf::Counter> m_MyOffset;
    uint32_t m_iters;
    ACE_Recursive_Thread_Mutex m_lock;
    bool addNewKey(RegionPtr regionPtr);
    bool putAllNewKey(RegionPtr regionPtr);
    bool updateExistingKey(RegionPtr regionPtr);
    bool invalidate(RegionPtr regionPtr);
    bool destroy(RegionPtr regionPtr);
    bool localInvalidate(RegionPtr regionPtr);
    bool localDestroy(RegionPtr regionPtr);
    bool get(RegionPtr regionPtr);
    CacheablePtr GetValue(int32_t value,char* update = NULL);
    //std::vector<int> availableOps;
  public:

    DoOpsTask(RegionPtr reg, FrameworkTest * test) :
    	regionPtr(reg), m_test(test),m_MyOffset(), m_iters(100) {
    	//availableOps = new vector<int>(operations, operations + sizeof(operations) / sizeof(operations[0]) );
   }

    virtual bool doSetup(int32_t id) {
         return true;
    }

    virtual void doCleanup(int32_t id) {
    }
    virtual ~DoOpsTask() {
    }
    virtual uint32_t doTask(int32_t id) {
      char logmsg[2048];
      int opcode;
    //  bool isCqTest=m_test->getBoolValue("cqtest");
      std::vector<int> availableOps (operations, operations + sizeof(operations) / sizeof(operations[0]) );
      if(m_test->gettxManager() != NULLPTR) {
    	  availableOps.erase(availableOps.begin() + LOCAL_DESTROY -1);
    	  availableOps.erase(availableOps.begin() + LOCAL_INVALIDATE -1);
      }
      while(m_Run && (availableOps.size() != 0)){
    	ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
        bool doneWithOps = false;
		//bool rolledback = false;
        uint32_t i = GsRandom::random((uint32_t)0,( uint32_t)availableOps.size());
         opcode = availableOps[i];//getStringValue( "entryOps" );
         if (m_test->gettxManager() != NULLPTR) {
        	 m_test->gettxManager()->begin();
         }
         switch (opcode) {
                  case ADD_NEW_KEY:
                     doneWithOps = addNewKey(regionPtr);
                     break;
                  case PUTALL_NEW_KEY:
                      doneWithOps = putAllNewKey(regionPtr);
                      break;
                  case INVALIDATE:
                     doneWithOps = invalidate(regionPtr);
                     break;
                  case DESTROY:
                     doneWithOps = destroy(regionPtr);
                     break;
                  case UPDATE_EXISTING_KEY:
                     doneWithOps = updateExistingKey(regionPtr);
                     break;
                  case GET:
                     doneWithOps = get(regionPtr);
                     break;
                  case LOCAL_INVALIDATE:
                     doneWithOps = localInvalidate(regionPtr);
                     break;
                  case LOCAL_DESTROY:
                     doneWithOps = localDestroy(regionPtr);
                     break;
                  default: {
                	  sprintf( logmsg, "Invalid operation specified: %d.\n", opcode );
                	  throw new FwkException( logmsg );
                  }
               }
           if (m_test->gettxManager() != NULLPTR) {
                m_test->gettxManager()->commit();
	   }
          if(doneWithOps) {
            FWKINFO("Done with operation "<< opcode);
           availableOps.erase(availableOps.begin() + i);
          }
       //count++;
       }
       return 0;
   }
  private:
    void checkContainsValueForKey(CacheablePtr key, bool expected, std::string logStr);

  };
// ----------------------------------------------------------------------------

class CSCacheListener : public CacheListener
{
public:
  CSCacheListener( FrameworkTest * test, const char * pref );
  virtual ~CSCacheListener() {}

  virtual void afterCreate( const EntryEvent& event ) {
    m_test->bbIncrement( m_bb, m_acTag );
  }
  std::string afterCreateTag() const {
    return m_acTag;
  }
  virtual void afterUpdate( const EntryEvent& event ) {
    m_test->bbIncrement( m_bb, m_auTag );
  }
  virtual void afterInvalidate( const EntryEvent& event ) {
    m_test->bbIncrement( m_bb, m_aiTag );
  }

  virtual void afterDestroy( const EntryEvent& event ) {
    m_test->bbIncrement( m_bb, m_adTag );
  }

  virtual void afterRegionInvalidate( const RegionEvent& event ) {
    m_test->bbIncrement( m_bb, m_ariTag );
  }

  virtual void afterRegionDestroy( const RegionEvent& event ) {
    m_test->bbIncrement( m_bb, m_ardTag );
  }

  virtual void close( const RegionPtr& region ){
    m_test->bbIncrement( m_bb, m_cTag );
  }

  static std::string & getBB() { return m_bb; }

protected:
  /** @brief needed for BB access */
  FrameworkTest * m_test;

  static std::string m_bb;
  std::string m_acTag;
  std::string m_auTag;
  std::string m_aiTag;
  std::string m_adTag;
  std::string m_ariTag;
  std::string m_ardTag;
  std::string m_cTag;
};

typedef SharedPtr< CSCacheListener > CSCacheListenerPtr;

class CountingCacheListener : public CSCacheListener
{
public:
  CountingCacheListener( FrameworkTest * test, const char * pref )
    : CSCacheListener( test, pref ) {}
    virtual void afterCreate( const EntryEvent& event ) {}
};

class ValidationCacheListener : public CSCacheListener
{
public:
  ValidationCacheListener( FrameworkTest * test, const char * pref )
    : CSCacheListener( test, pref ) { }
  virtual void afterCreate( const EntryEvent& event );
  virtual void afterUpdate( const EntryEvent& event );
  virtual ~ValidationCacheListener();
  void validate( const EntryEvent& event, int32_t & eventCount, int32_t & latencyMin ,
    int32_t & latencyMax , int32_t & latencyAvg, int64_t & latencyTotal, const std::string &tag );

};

class MyCqListener : public CqListener {
  uint32_t m_numInserts;
  uint32_t m_numUpdates;
  uint32_t m_numDeletes;
  uint32_t m_numEvents;
  public:
  MyCqListener():
    m_numInserts(0),
    m_numUpdates(0),
    m_numDeletes(0),
    m_numEvents(0)
  {
  }
  inline void updateCount(const CqEvent& cqEvent)
  {
    m_numEvents++;
    switch (cqEvent.getQueryOperation())
    {
      case CqOperation::OP_TYPE_CREATE:
           m_numInserts++;
           break;
      case CqOperation::OP_TYPE_UPDATE:
           m_numUpdates++;
           break;
      case CqOperation::OP_TYPE_DESTROY:
           m_numDeletes++;
           break;
      default:
           break;
       }
  }

  void onEvent(const CqEvent& cqe){
   updateCount(cqe);
  }
  void onError(const CqEvent& cqe){
   updateCount(cqe);
  }
  void close(){
  }
};

class SilenceListener : public CacheListener
{
public:
 
  SilenceListener( FrameworkTest * test ):
	  m_test( test )
  {
  }
  int64_t currentTimeInMillies()
  {
	  ACE_Time_Value startTime = ACE_OS::gettimeofday();
	  return startTime.msec();
  }
  virtual void afterCreate( const EntryEvent& event ) {
	  m_test->bbSetIfGreater("ListenerBB","lastEventTime",currentTimeInMillies());
  }

  virtual void afterUpdate( const EntryEvent& event ) {
	  m_test->bbSetIfGreater("ListenerBB","lastEventTime",currentTimeInMillies());

  }

  virtual void afterInvalidate( const EntryEvent& event ) {
	  m_test->bbSetIfGreater("ListenerBB","lastEventTime",currentTimeInMillies());

  }

  virtual void afterDestroy( const EntryEvent& event ) {
	  m_test->bbSetIfGreater("ListenerBB","lastEventTime",currentTimeInMillies());
	  
  }

  virtual void afterRegionInvalidate( const RegionEvent& event ){

  }

  virtual void afterRegionDestroy( const RegionEvent& event ) {

  }

  virtual void close( const RegionPtr& region ){}

  virtual ~SilenceListener() {
   
  }

  

private:
  
  FrameworkTest * m_test;
};
// ----------------------------------------------------------------------------

class CacheServerTest : public FrameworkTest
{
public:

  CacheServerTest( const char * initArgs ) :
    FrameworkTest( initArgs ),
    m_bb( "CacheServerBB" ),
    m_cacheType( "PEER" ),
   isGetInitialValues(false)
    {}


    virtual ~CacheServerTest() {}

  RegionPtr getRegion( const char * name = NULL );

  std::string cacheType() { return m_cacheType; }

  void createRegion(const std::string& cacheType, CacheListenerPtr cl = NULLPTR,
      const char* lib = NULL, const char* func = NULL);
  int32_t doFeedPuts();
  int32_t doFeedIntPuts();
  void runQuery(int32_t& queryCnt);
  void remoteQuery(QueryStrings currentQuery,bool islargeSetQuery ,
      bool isUnsupportedPRQuery, int32_t queryIndex, bool isparam, bool isStructSet);
  void doTwinkleRegion();
  int32_t doEntryOperations();
  int32_t doOps();
  int32_t doRROps();
  int32_t doEntryOperationsForSecurity();
  void reInitCache();
  void destroyRegion();
  int32_t doDepartSystem();
  int32_t doVerifyClientCount();
  int32_t validateEntryOperationsForSecurity();
  void add( int32_t key, char * valBuf);
  int32_t verifyCount();
  int32_t verifyKeyCount();
  void putAllOps(bool isWithCallBck=false);
  void getAllOps(bool isWithCallBck=false);
  void removeAllOps(bool isWithCallBck=false);
  int32_t verifyRegionContentsBeforeOps();
  int32_t verifyRegionContentsAfterOpsRI();
  int32_t verifyRegionContentsDynamic();
  int32_t verifyRegionContentsAfterLateOps();
  int32_t initInstance();
  int32_t registerInterestSingle();
  int32_t registerInterestList();
  int32_t registerAllKeys();
  int32_t waitForSilenceListenerComplete(int64_t desiredSilenceSec, int64_t sleepMS);
  int32_t ResetImageBB();
  void checkTest( const char * taskId );
  int32_t verifyQueryResult();

private:
  void SetFisrtAndLastKeyOnBB(int32_t entryCount);
  CacheableStringPtr getKey( int32_t max );
  void updateOperationMap(std::string operation);
  void updateExceptionMap(std::string operation);
  CacheablePtr getUserObject(const std::string & objType);
  bool allowQuery(queryCategory category, bool haveLargeResultset,bool islargeSetQuery, bool isUnsupportedPRQuery);

  std::string printKeyIntervalsBBData();
  void checkContainsValueForKey(CacheablePtr key, bool expected, std::string logStr);
  void checkContainsKey(CacheablePtr key, bool expected, std::string logStr);
  void verifyEntry(CacheableKeyPtr key,ExpectedRegionContents *expected);
  void checkValue(CacheableKeyPtr key, CacheablePtr value) ;
  void checkUpdatedValue(CacheableKeyPtr key, CacheablePtr value);
  CacheableHashSetPtr verifyRegionSize(ExpectedRegionContents *expected);
  CacheablePtr GetValue(int32_t value,char* update = NULL);
  void initKeys();
  std::string m_bb;
  std::string m_cacheType;
  //VectorOfCacheableKeyPtr * m_KeysA;
  CacheableKeyPtr * m_KeysA;
  bool isGetInitialValues;

};

} // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __CacheServerTest_hpp__


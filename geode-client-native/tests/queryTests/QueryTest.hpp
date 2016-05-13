/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    QueryTest.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __QUERY_TEST_HPP__
#define __QUERY_TEST_HPP__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"

#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Region.hpp"
#include "Query.hpp"
#include "QueryService.hpp"
#include "SelectResults.hpp"
#include "ResultSet.hpp"
#include "StructSet.hpp"
#include "Struct.hpp"
#include "SelectResultsIterator.hpp"
#include "CqAttributesFactory.hpp"
#include "CqAttributes.hpp"
#include "CqListener.hpp"
#include "CqQuery.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"

#include <stdlib.h>

#ifdef WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#endif

namespace gemfire {
 namespace testframework {
   namespace query {
class MyCqListener : public CqListener {
  uint32_t m_numInserts;
  uint32_t m_numUpdates;
  uint32_t m_numDeletes;
  uint32_t m_numInvalidates;
  uint32_t m_numEvents;
  public:
  uint32_t getNumInserts()
  {
    return m_numInserts;
  }
  uint32_t getNumUpdates()
  {
    return m_numUpdates;
  }
  uint32_t getNumDeletes()
  {
    return m_numDeletes;
  }
  uint32_t getNumEvents()
  {
    return m_numEvents;
  }
  uint32_t getNumInvalidates()
  {
    return m_numInvalidates;
  }
  MyCqListener():
    m_numInserts(0),
    m_numUpdates(0),
    m_numDeletes(0),
    m_numInvalidates(0),
    m_numEvents(0)
  {
  }
  inline void updateCount(const CqEvent& cqEvent)
  {
    m_numEvents++;
    CacheableKeyPtr key=cqEvent.getKey();
    switch (cqEvent.getQueryOperation())
    {
      case CqOperation::OP_TYPE_CREATE:
           m_numInserts++;
           FWKINFO("ML:insert");
           break;
      case CqOperation::OP_TYPE_UPDATE:
           m_numUpdates++;
           FWKINFO("ML:update");
           break;
      case CqOperation::OP_TYPE_DESTROY:
           m_numDeletes++;
           FWKINFO("ML:destroy");
           break;
      case CqOperation::OP_TYPE_INVALIDATE:
           m_numInvalidates++;
           FWKINFO("ML:invalidate");
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

class QueryTest : public FrameworkTest
{
public:
  QueryTest( const char * initArgs ) :
    FrameworkTest( initArgs ),
    m_isObjectRegistered(false)
    {}

  virtual ~QueryTest( void ) {
  }


  int32_t createUserDefineRegion();
  int32_t getObject();
  int32_t runQuery();
  int32_t populateUserObject();
  int32_t populateRangePositionObjects();
  int32_t getAndComparePositionObjects();
  int32_t updateRangePositions();
  int32_t verifyAllPositionObjects();
  int32_t destroyUserObject();
  int32_t invalidateUserObject();
  int32_t addRootAndSubRegion();
  int32_t doRunQueryWithPayloadAndEntries();
  int32_t populateRangePosition();
  int32_t cqOperations();
  int32_t registerCQ();
  int32_t verifyCQListenerInvoked();
  int32_t validateCq();
  int32_t cqState();
  int32_t registerCQForConc();
  int32_t registerAllKeys();
  int32_t restartClientAndRegInt(const char * taskId);
  int32_t closeNormalAndRestart(const char * taskId);
  int32_t verifyCqDestroyed();
  int32_t populatePortfolioObject();
  void checkTest( const char * taskId );
  int32_t validateEvents();

private:
  RegionPtr getRegionPtr( const char * reg = NULL );
  bool checkReady(int32_t numClients);
  bool verifyResultSet(int distinctKeys = 0);
  bool verifyStructSet(int distinctKeys = 0);
  bool readQueryStringfromXml(std::string &queryString);
  SelectResultsPtr remoteQuery(const QueryServicePtr qs,const char * querystr);
  SelectResultsPtr continuousQuery(const QueryServicePtr qs,const char * querystr,int cqNum);
  std::string getNextRegionName(RegionPtr& regionPtr);
  void stopCQ(const CqQueryPtr cq);
  void closeCQ(const CqQueryPtr cq);
  void executeCQ(const CqQueryPtr cq);
  void executeCQWithIR(const CqQueryPtr cq);
  void reRegisterCQ(const CqQueryPtr cq);
  bool m_isObjectRegistered;
};

   } //   namespace query
 } // namespace testframework
} // namespace gemfire


#endif // __QUERY_TEST_HPP__

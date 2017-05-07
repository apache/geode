/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    MultiUserSecurity.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */


#ifndef _MULTIUSERSECURITY_HPP_
#define _MULTIUSERSECURITY_HPP_

#include "GemfireCppCache.hpp"

#include "CacheableString.hpp"
#include "Cache.hpp"
#include "Region.hpp"

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "fwklib/ClientTask.hpp"
#include "fwklib/FwkLog.hpp"

#include <stdlib.h>
#include <map>

namespace gemfire {
 namespace testframework {
   namespace multiusersecurity {
   std::string DURABLEBB( "DURABLEBB" );
class MyCqListener : public CqListener {
  uint32_t m_numInserts;
  uint32_t m_numUpdates;
  uint32_t m_numDeletes;
  uint32_t m_numEvents;
  uint8_t m_id;
  public:
  uint8_t getId()
  {
    return m_id;
  }
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
  MyCqListener(uint8_t id):
  m_id(id),
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
class MultiUser : public FrameworkTest
{
public:
 MultiUser( const char * initArgs ) :
    FrameworkTest( initArgs )
 {}

  virtual ~MultiUser( void ) {}

  int32_t createRegion();
  int32_t createPools();

  void checkTest( const char * taskId );
  void getClientSecurityParams(PropertiesPtr prop, std::string credentials);
  void createMultiUserCacheAndRegion(PoolPtr pool,RegionPtr region);
  void insertKeyStorePath(const char *username,PropertiesPtr userProps);
  int32_t doFeed();
  int32_t entryOperationsForMU();
  int32_t cqForMU();
  int32_t closeCacheAndReInitialize(const char * taskId);
  int32_t validateEntryOperationsForPerUser();
  int32_t validateCqOperationsForPerUser();
  void updateOperationMap(std::string opcode, std::string user);
  void updateExceptionMap(std::string opcode, std::string user);
  void setAdminRole(std::string userName);
  void setReaderRole(std::string userName);
  void setWriterRole(std::string userName);
  void setQueryRole(std::string userName);


private:
  ACE_Recursive_Thread_Mutex m_lock;

};
   };
    };
     };
#endif /*_MULTIUSERSECURITY_HPP_*/

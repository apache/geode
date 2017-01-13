#ifndef __GEMFIRE_CQ_SERVICE_H__
#define __GEMFIRE_CQ_SERVICE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include "TcrMessage.hpp"
#include "Queue.hpp"
#include <ace/ACE.h>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Semaphore.h>
#include <gfcpp/CacheableKey.hpp>
#include <gfcpp/CqOperation.hpp>
#include <gfcpp/CqQuery.hpp>
#include "MapWithLock.hpp"
#include <gfcpp/DistributedSystem.hpp>
#include <map>
#include <string>
#include "Queue.hpp"
#include <ace/Task.h>
#include "ThinClientBaseDM.hpp"
#include "CqServiceVsdStats.hpp"

#include "NonCopyable.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class CqService CqService.hpp
 *
 * Implements the CqService functionality.
 *
 */

/* adongre
 * CID 28727: Other violation (MISSING_COPY)
 * Class "gemfire::CqService" owns resources that are managed in its
 * constructor and destructor but has no user-written copy constructor.
 *
 * CID 28713: Other violation (MISSING_ASSIGN)
 * Class "gemfire::CqService" owns resources that are managed in its constructor
 * and destructor but has no user-written assignment operator.
 *
 * FIX : Make the class NonCopyable
 */
class CPPCACHE_EXPORT CqService : public SharedBase,
                                  private NonCopyable,
                                  private NonAssignable {
 private:
  ThinClientBaseDM* m_tccdm;
  ACE_Recursive_Thread_Mutex m_mutex;
  std::string m_queryString;
  ACE_Semaphore m_notificationSema;

  bool m_running;
  MapOfCqQueryWithLock* m_cqQueryMap;

  CqServiceStatisticsPtr m_stats;

  inline bool noCq() const {
    MapOfRegionGuard guard(m_cqQueryMap->mutex());
    return (0 == m_cqQueryMap->current_size());
  }

 public:
  /**
   * Constructor.
   */
  CqService(ThinClientBaseDM* tccdm);
  ThinClientBaseDM* getDM() { return m_tccdm; }

  void receiveNotification(TcrMessage* msg);
  ~CqService();

  /**
   * Returns the state of the cqService.
   */
  bool checkAndAcquireLock();

  void updateStats();

  CqServiceVsdStats& getCqServiceVsdStats() {
    return *dynamic_cast<CqServiceVsdStats*>(m_stats.ptr());
  }

  /**
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed, however, until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable
   * @return the newly created CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   * client
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating
   *         managing cq metadata info.
   * @throws CqInvalidException if the query doesnot meet the CQ constraints.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a
   * region path.
   *         Bind parameters in the query are not supported for the initial
   * release.
   *
   */
  CqQueryPtr newCq(std::string& cqName, std::string& queryString,
                   CqAttributesPtr& cqAttributes, bool isDurable = false);

  /**
   * Adds the given CQ and cqQuery object into the CQ map.
   */
  void addCq(std::string& cqName, CqQueryPtr& cq);

  /**
   * Removes given CQ from the cqMap..
   */
  void removeCq(std::string& cqName);
  /**
   * Retrieve a CqQuery by name.
   * @return the CqQuery or null if not found
   */
  CqQueryPtr getCq(std::string& cqName);

  /**
   * Clears the CQ Query Map.
   */
  void clearCqQueryMap();
  /**
   * Retrieve  all registered CQs
   */
  void getAllCqs(VectorOfCqQuery& vec);
  /**
   * Executes all the cqs on this client.
   */
  void executeAllClientCqs(bool afterFailover = false);

  /**
   * Executes all CQs on the specified endpoint after failover.
   */
  GfErrType executeAllClientCqs(TcrEndpoint* endpoint);

  /**
   * Executes all the given cqs.
   */
  void executeCqs(VectorOfCqQuery& cqs, bool afterFailover = false);

  /**
   * Executes all the given cqs on the specified endpoint after failover.
   */
  GfErrType executeCqs(VectorOfCqQuery& cqs, TcrEndpoint* endpoint);

  /**
   * Stops all the cqs
   */
  void stopAllClientCqs();

  /**
   * Stops all the specified cqs.
   */
  void stopCqs(VectorOfCqQuery& cqs);

  /**
   * Close all CQs executing in this client, and release resources
   * associated with executing CQs.
   * CqQuerys created by other client are unaffected.
   */
  void closeAllCqs();

  /**
   * Get statistics information for all CQs
   * @return the CqServiceStatistics
   */
  CqServiceStatisticsPtr getCqServiceStatistics();

  /**
   * Close the CQ Service after cleanup if any.
   *
   */
  void closeCqService();

  /**
   * Cleans up the CqService.
   */
  void cleanup();

  /*
   * Checks if CQ with the given name already exists.
   * @param cqName name of the CQ.
   * @return true if exists else false.
   */
  bool isCqExists(std::string& cqName);
  /**
   * Invokes the CqListeners for the given CQs.
   * @param cqs list of cqs with the cq operation from the Server.
   * @param messageType base operation
   * @param key
   * @param value
   */
  void invokeCqListeners(const std::map<std::string, int>* cqs,
                         uint32_t messageType, CacheableKeyPtr key,
                         CacheablePtr value, CacheableBytesPtr deltaValue,
                         EventIdPtr eventId);
  /**
   * Returns the Operation for the given EnumListenerEvent type.
   * @param eventType
   * @return Operation
   */
  CqOperation::CqOperationType getOperation(int eventType);

  void closeCqs(VectorOfCqQuery& cqs);

  /**
   * Gets all the durable CQs registered by this client.
   *
   * @return List of names of registered durable CQs, empty list if no durable
   * cqs.
   */
  CacheableArrayListPtr getAllDurableCqsFromServer();

  void invokeCqConnectedListeners(std::string poolName, bool connected);
};

typedef SharedPtr<CqService> CqServicePtr;

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_SERVICE_H__

#ifndef __GEMFIRE_CQ_QUERY_IMPL_H__
#define __GEMFIRE_CQ_QUERY_IMPL_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>

#include <gfcpp/CqResults.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/CqState.hpp>
#include "CqQueryVsdStats.hpp"
#include "CqService.hpp"
#include <gfcpp/CqOperation.hpp>
#include <gfcpp/CqAttributes.hpp>
#include <gfcpp/Region.hpp>
#include "MapWithLock.hpp"
#include <string>
#include <ace/ACE.h>
#include <ace/Condition_Recursive_Thread_Mutex.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include "ProxyCache.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class CqQueryImpl CqQueryImpl.hpp
 *
 * Represents the CqQuery object. Implements CqQuery API and CqAttributeMutator.
 *
 */
class CqQueryImpl : public CqQuery {
 protected:
  std::string m_cqName;
  std::string m_queryString;
  SelectResultsPtr m_cqResults;

 private:
  QueryPtr m_query;
  CqAttributesPtr m_cqAttributes;
  CqAttributesMutatorPtr m_cqAttributesMutator;
  CqServicePtr m_cqService;
  std::string m_serverCqName;
  bool m_isDurable;

  // Stats counters
  CqStatisticsPtr m_stats;
  CqState::StateType m_cqState;
  CqOperation::CqOperationType m_cqOperation;

  /* CQ Request Type - Start */
  //  unused
  /*
  enum {
     EXECUTE_REQUEST = 0,
     EXECUTE_INITIAL_RESULTS_REQUEST = 1,
     STOP_REQUEST = 2,
     CLOSE_REQUEST = 3,
     REDUNDANT_EXECUTE_REQUEST = 4
  } CqRequestType;
  */

  /* CQ Request type - End */

  /**
   * Constructor.
   */
 public:
  CqQueryImpl(CqServicePtr& cqService, std::string& cqName,
              std::string& queryString, CqAttributesPtr& cqAttributes,
              bool isDurable = false,
              UserAttributesPtr userAttributesPtr = NULLPTR);

  ~CqQueryImpl();

  /**
   * returns CQ name
   */
  const char* getName() const;

  /**
   * sets the CqName.
   */
  void setName(std::string& cqName);

  /**
   * Initializes the CqQuery.
   * creates Query object, if its valid adds into repository.
   */
  void initCq();

  /**
   * Closes the Query.
   *        On Client side, sends the cq close request to server.
   *        On Server side, takes care of repository cleanup.
   * @throws CqException
   */
  void close();

  /**
   * Closes the Query.
   *        On Client side, sends the cq close request to server.
   *        On Server side, takes care of repository cleanup.
   * @param sendRequestToServer true to send the request to server.
   * @throws CqException
   */
  void close(bool sendRequestToServer);

  /**
   * Store this CQ in the cqService's cqMap.
   * @throws CqException
   */
  void addToCqMap();

  /**
   * Removes the CQ from CQ repository.
   * @throws CqException
   */
  void removeFromCqMap();

  /**
   * Returns the QueryString of this CQ.
   */
  const char* getQueryString() const;

  /**
   * Return the query after replacing region names with parameters
   * @return the Query for the query string
   */
  QueryPtr getQuery() const;

  /**
   * @see org.apache.geode.cache.query.CqQuery#getStatistics()
   */
  const CqStatisticsPtr getStatistics() const;

  CqQueryVsdStats& getVsdStats() {
    return *dynamic_cast<CqQueryVsdStats*>(m_stats.ptr());
  }

  const CqAttributesPtr getCqAttributes() const;

  RegionPtr getCqBaseRegion();

  /**
   * Clears the resource used by CQ.
   * @throws CqException
   */
  void cleanup();

  /**
   * @return Returns the cqListeners.
   */
  void getCqListeners(VectorOfCqListener& cqListener);

  /**
   * Start or resume executing the query.
   */
  void execute();

  void executeAfterFailover();

  /**
   * Execute CQ on endpoint after failover
   */
  GfErrType execute(TcrEndpoint* endpoint);

  /**
   * Start or resume executing the query.
   * Gets or updates the CQ results and returns them.
   */
  CqResultsPtr executeWithInitialResults(uint32_t timeout);

  /**
   * This is called when the new server comes-up.
   * Executes the CQ on the given endpoint.
   * @param endpoint
   */
  bool executeCq(TcrMessage::MsgType requestType);

  /**
   * Stop or pause executing the query.
   */
  void stop();

  /**
   * Return the state of this query.
   * @return STOPPED RUNNING or CLOSED
   */
  CqState::StateType getState();

  /**
   * Sets the state of the cq.
   * Server side method. Called during cq registration time.
   */
  void setCqState(CqState::StateType state);

  const CqAttributesMutatorPtr getCqAttributesMutator() const;

  /**
   * @return Returns the cqOperation.
   */
  CqOperation::CqOperationType getCqOperation();

  /**
   * @param cqOperation The cqOperation to set.
   */
  void setCqOperation(CqOperation::CqOperationType cqOperation);

  /**
   * Update CQ stats
   * @param cqEvent object
   */
  void updateStats(CqEvent& cqEvent);

  /**
   * Return true if the CQ is in running state
   * @return true if running, false otherwise
   */
  bool isRunning();

  /**
   * Return true if the CQ is in Sstopped state
   * @return true if stopped, false otherwise
   */
  bool isStopped();

  /**
   * Return true if the CQ is closed
   * @return true if closed, false otherwise
   */
  bool isClosed();

  /**
   * Return true if the CQ is durable
   * @return true if durable, false otherwise
   */
  bool isDurable();

  inline ThinClientBaseDM* getDM() { return m_tccdm; }

 private:
  void updateStats();
  ACE_Recursive_Thread_Mutex m_mutex;
  void sendStopOrClose(TcrMessage::MsgType requestType);
  ThinClientBaseDM* m_tccdm;
  ProxyCachePtr m_proxyCache;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CQ_QUERY_IMPL_H__

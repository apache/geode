/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/VectorT.hpp>
#include "CqQueryImpl.hpp"
#include <gfcpp/CqAttributesFactory.hpp>
#include "CqAttributesMutatorImpl.hpp"
#include <gfcpp/Log.hpp>
#include "ResultSetImpl.hpp"
#include "StructSetImpl.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "ThinClientRegion.hpp"
#include "ReadWriteLock.hpp"
#include "ThinClientRegion.hpp"
using namespace gemfire;

CqQueryImpl::CqQueryImpl(CqServicePtr& cqService, std::string& cqName,
                         std::string& queryString,
                         CqAttributesPtr& cqAttributes, bool isDurable,
                         UserAttributesPtr userAttributesPtr)
    : m_cqName(cqName),
      m_queryString(queryString),
      m_cqService(cqService),
      m_serverCqName(
          cqName),  // On Client Side serverCqName and cqName will be same.
      m_isDurable(isDurable),
      m_stats(new CqQueryVsdStats(m_cqName.c_str())),
      m_cqState(CqState::STOPPED),  // Initial state is stopped
      /* adongre
       * CID 28930: Uninitialized scalar field (UNINIT_CTOR)
       */
      m_cqOperation(CqOperation::OP_TYPE_INVALID),
      m_tccdm(m_cqService->getDM()) {
  CqAttributesFactory cqAf(cqAttributes);
  m_cqAttributes = cqAf.create();
  m_cqAttributesMutator =
      new CqAttributesMutatorImpl(CqAttributesPtr(m_cqAttributes));
  if (userAttributesPtr != NULLPTR) {
    m_proxyCache = userAttributesPtr->getProxyCache();
  } else {
    m_proxyCache = NULLPTR;
  }
}

CqQueryImpl::~CqQueryImpl() {}
/**
 * returns CQ name
 */
void CqQueryImpl::updateStats() { m_cqService->updateStats(); }

const char* CqQueryImpl::getName() const { return m_cqName.c_str(); }

/**
 * sets the CqName.
 */
void CqQueryImpl::setName(std::string& cqName) {
  m_cqName = m_serverCqName = cqName;
}

/**
 * Initializes the CqQuery.
 * creates Query object, if its valid adds into repository.
 */
void CqQueryImpl::initCq() {
  addToCqMap();

  // Initialize the VSD statistics

  // Update statistics with CQ creation.
  CqServiceVsdStats& stats = m_cqService->getCqServiceVsdStats();
  // stats.incNumCqsStopped();
  stats.incNumCqsCreated();
  // stats.incNumCqsOnClient();
  updateStats();
}

/**
 * Closes the Query.
 *        On Client side, sends the cq close request to server.
 *        On Server side, takes care of repository cleanup.
 * @throws CqException
 */
void CqQueryImpl::close() { close(true); }

/**
 * Closes the Query.
 *        On Client side, sends the cq close request to server.
 * @param sendRequestToServer true to send the request to server.
 * @throws CqException
 */
void CqQueryImpl::close(bool sendRequestToServer) {
  // Check if the cq is already closed.
  if (isClosed()) {
    // throw CqClosedException("CQ is already closed, CqName : " + this.cqName);
    LOGFINE("CQ is already closed, CqName : %s", m_cqName.c_str());
    return;
  }

  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }
  LOGFINE("Started closing CQ CqName : %s", m_cqName.c_str());

  // bool isClosed = false;

  // Stat update.
  CqServiceVsdStats& stats = m_cqService->getCqServiceVsdStats();
  /*
  if (isRunning()) {
      stats.decNumCqsActive();
  }
  else if (isStopped()) {
      stats.decNumCqsStopped();
  }
  */
  setCqState(CqState::CLOSING);
  if (sendRequestToServer == true) {
    try {
      sendStopOrClose(TcrMessage::CLOSECQ_MSG_TYPE);
    } catch (...) {
      // ignore and move on
    }
  }

  // Set the state to close, and update stats
  setCqState(CqState::CLOSED);
  stats.incNumCqsClosed();

  // Invoke close on Listeners if any.
  if (m_cqAttributes != NULLPTR) {
    VectorOfCqListener cqListeners;
    m_cqAttributes->getCqListeners(cqListeners);

    if (!cqListeners.empty()) {
      LOGFINE(
          "Invoking CqListeners close() api for the CQ, CqName : %s  Number of "
          "CqListeners : %d",
          m_cqName.c_str(), cqListeners.length());

      for (int32_t lCnt = 0; lCnt < cqListeners.length(); lCnt++) {
        try {
          cqListeners[lCnt]->close();
          // Handle client side exceptions.
        } catch (Exception& ex) {
          LOGWARN(
              "Exception occoured in the CqListener of the CQ, CqName : "
              "%sError : %s",
              m_cqName.c_str(), ex.getMessage());
        }
      }
    }
  }

  removeFromCqMap();
  updateStats();
  LOGFINE("Successfully closed the CQ. %s", m_cqName.c_str());
}

/**
 * Store this CQ in the cqService's cqMap.
 * @throws CqException
 */
void CqQueryImpl::addToCqMap() {
  // Add CQ to the CQ repository
  try {
    LOGFINE("Adding to CQ Repository. CqName : %s Server CqName : %s",
            m_cqName.c_str(), m_serverCqName.c_str());
    CqQueryPtr cq(this);
    m_cqService->addCq(m_cqName, cq);
  } catch (Exception& ex) {
    std::string errMsg =
        "Failed to store Continuous Query in the repository. CqName: " +
        m_cqName + ex.getMessage();
    LOGERROR(errMsg.c_str());
    throw CqException(errMsg.c_str());
  }
  LOGFINE("Stored CQ in the CQ repository. %s", m_cqName.c_str());
}

/**
 * Removes the CQ from CQ repository.
 * @throws CqException
 */
void CqQueryImpl::removeFromCqMap() {
  try {
    m_cqService->removeCq(m_cqName);
  } catch (Exception& ex) {
    std::string errMsg =
        "Failed to remove Continuous Query From the repository. CqName: " +
        m_cqName + " Error : " + ex.getMessage();
    LOGERROR(errMsg.c_str());
    throw CqException(errMsg.c_str());
  }
  LOGFINE("Removed CQ from the CQ repository. CQ Name: %s", m_cqName.c_str());
}

/**
 * Returns the QueryString of this CQ.
 */
const char* CqQueryImpl::getQueryString() const {
  return m_queryString.c_str();
}

/**
 * Return the query
 * @return the Query for the query string
 */
QueryPtr CqQueryImpl::getQuery() const { return m_query; }

/**
 * @see org.apache.geode.cache.query.CqQuery#getStatistics()
 */
const CqStatisticsPtr CqQueryImpl::getStatistics() const { return m_stats; }

const CqAttributesPtr CqQueryImpl::getCqAttributes() const {
  return m_cqAttributes;
}

/**
 * Clears the resource used by CQ.
 * @throws CqException
 */
void CqQueryImpl::cleanup() { removeFromCqMap(); }

/**
 * @return Returns the cqListeners.
 */
void CqQueryImpl::getCqListeners(VectorOfCqListener& cqListener) {
  m_cqAttributes->getCqListeners(cqListener);
}

GfErrType CqQueryImpl::execute(TcrEndpoint* endpoint) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  if (m_cqState != CqState::RUNNING) {
    return GF_NOERR;
  }

  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }

  LOGFINE("Executing CQ [%s]", m_cqName.c_str());

  TcrMessageExecuteCq request(m_cqName, m_queryString, CqState::RUNNING,
                              isDurable(), m_tccdm);
  TcrMessageReply reply(true, m_tccdm);

  GfErrType err = GF_NOERR;

  err = m_tccdm->sendRequestToEP(request, reply, endpoint);

  if (err != GF_NOERR) {
    // GfErrTypeToException("CqQuery::execute(endpoint)", err);
    return err;
  }

  if (reply.getMessageType() == TcrMessage::EXCEPTION ||
      reply.getMessageType() == TcrMessage::CQDATAERROR_MSG_TYPE ||
      reply.getMessageType() == TcrMessage::CQ_EXCEPTION_TYPE) {
    err = ThinClientRegion::handleServerException("CqQuery::execute(endpoint)",
                                                  reply.getException());
    /*
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw CqQueryException("CqQuery::execute(endpoint): exception at the
    server side: ",
              reply.getException());
    }
    else {
      GfErrTypeToException("CqQuery::execute(endpoint)", err);
    }
    */
  }

  return err;
}

void CqQueryImpl::executeAfterFailover() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  if (m_cqState != CqState::RUNNING) {
    return;
  }
  executeCq(TcrMessage::EXECUTECQ_MSG_TYPE);
}

void CqQueryImpl::execute() {
  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }

  ACE_Guard<ACE_Recursive_Thread_Mutex> guardRedundancy(
      *(m_tccdm->getRedundancyLock()));

  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  if (m_cqState == CqState::RUNNING) {
    throw IllegalStateException("CqQuery::execute: cq is already running");
  }
  executeCq(TcrMessage::EXECUTECQ_MSG_TYPE);
}
// for          EXECUTE_REQUEST or REDUNDANT_EXECUTE_REQUEST
bool CqQueryImpl::executeCq(TcrMessage::MsgType requestType) {
  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }

  LOGDEBUG("CqQueryImpl::executeCq");
  TcrMessageExecuteCq msg(m_cqName, m_queryString, CqState::RUNNING,
                          isDurable(), m_tccdm);
  TcrMessageReply reply(true, m_tccdm);

  GfErrType err = GF_NOERR;
  err = m_tccdm->sendSyncRequest(msg, reply);
  if (err != GF_NOERR) {
    GfErrTypeToException("CqQuery::executeCq:", err);
  }
  if (reply.getMessageType() == TcrMessage::EXCEPTION ||
      reply.getMessageType() == TcrMessage::CQDATAERROR_MSG_TYPE ||
      reply.getMessageType() == TcrMessage::CQ_EXCEPTION_TYPE) {
    err = ThinClientRegion::handleServerException("CqQuery::executeCq",
                                                  reply.getException());
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw CqQueryException(
          "CqQuery::executeCq: exception at the server side: ",
          reply.getException());
    } else {
      GfErrTypeToException("CqQuery::executeCq", err);
    }
  }
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  m_cqState = CqState::RUNNING;
  updateStats();
  return true;
}

// for EXECUTE_INITIAL_RESULTS_REQUEST :
CqResultsPtr CqQueryImpl::executeWithInitialResults(uint32_t timeout) {
  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }

  ACE_Guard<ACE_Recursive_Thread_Mutex> guardRedundancy(
      *(m_tccdm->getRedundancyLock()));

  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  if (m_cqState == CqState::RUNNING) {
    throw IllegalStateException(
        "CqQuery::executeWithInitialResults: cq is already running");
  }
  // QueryResult values;
  TcrMessageExecuteCqWithIr msg(m_cqName, m_queryString, CqState::RUNNING,
                                isDurable(), m_tccdm);
  TcrMessageReply reply(true, m_tccdm);
  ChunkedQueryResponse* resultCollector = (new ChunkedQueryResponse(reply));
  reply.setChunkedResultHandler(
      static_cast<TcrChunkedResult*>(resultCollector));
  reply.setTimeout(timeout);

  GfErrType err = GF_NOERR;
  err = m_tccdm->sendSyncRequest(msg, reply);
  if (err != GF_NOERR) {
    LOGDEBUG("CqQueryImpl::executeCqWithInitialResults errorred!!!!");
    GfErrTypeToException("CqQuery::executeCqWithInitialResults:", err);
  }
  if (reply.getMessageType() == TcrMessage::EXCEPTION ||
      reply.getMessageType() == TcrMessage::CQDATAERROR_MSG_TYPE ||
      reply.getMessageType() == TcrMessage::CQ_EXCEPTION_TYPE) {
    err = ThinClientRegion::handleServerException(
        "CqQuery::executeCqWithInitialResults", reply.getException());
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw CqQueryException(
          "CqQuery::executeWithInitialResults: exception "
          "at the server side: ",
          reply.getException());
    } else {
      GfErrTypeToException("CqQuery::executeWithInitialResults", err);
    }
  }
  m_cqState = CqState::RUNNING;
  updateStats();
  CqResultsPtr sr;
  CacheableVectorPtr values = resultCollector->getQueryResults();
  const std::vector<CacheableStringPtr>& fieldNameVec =
      resultCollector->getStructFieldNames();
  int32_t sizeOfFieldNamesVec = static_cast<int32_t>(fieldNameVec.size());
  if (sizeOfFieldNamesVec == 0) {
    LOGFINEST("Query::execute: creating ResultSet for query: %s",
              m_queryString.c_str());
    sr = new ResultSetImpl(values);
  } else {
    if (values->size() % fieldNameVec.size() != 0) {
      throw MessageException(
          "Query::execute: Number of values coming "
          "from server has to be exactly divisible by field count");
    } else {
      LOGFINEST("Query::execute: creating StructSet for query: %s",
                m_queryString.c_str());
      sr = new StructSetImpl(values, fieldNameVec);
    }
  }
  delete resultCollector;
  return sr;
}

/**
 * Stop or pause executing the query.
 */
void CqQueryImpl::stop() {
  if (isClosed()) {
    throw CqClosedException(("CQ is closed, CqName : " + m_cqName).c_str());
  }

  GuardUserAttribures gua;
  if (m_proxyCache != NULLPTR) {
    gua.setProxyCache(m_proxyCache);
  }

  if (!(isRunning())) {
    throw IllegalStateException(
        ("CQ is not in running state, stop CQ does not apply, CqName : " +
         m_cqName)
            .c_str());
  }

  sendStopOrClose(TcrMessage::STOPCQ_MSG_TYPE);
  /*
  CqServiceVsdStats & stats = m_cqService->getCqServiceVsdStats();
  stats.decNumCqsActive();
  */
  setCqState(CqState::STOPPED);
  // stats.incNumCqsStopped();
  updateStats();
}
void CqQueryImpl::sendStopOrClose(TcrMessage::MsgType requestType) {
  GfErrType err = GF_NOERR;
  TcrMessageReply reply(true, m_tccdm);

  if (requestType == TcrMessage::STOPCQ_MSG_TYPE) {
    TcrMessageStopCQ msg(m_cqName, -1, m_tccdm);
    err = m_tccdm->sendSyncRequest(msg, reply);
  } else if (requestType == TcrMessage::CLOSECQ_MSG_TYPE) {
    TcrMessageCloseCQ msg(m_cqName, -1, m_tccdm);
    err = m_tccdm->sendSyncRequest(msg, reply);
  }

  if (err != GF_NOERR) {
    GfErrTypeToException("CqQuery::stop/close:", err);
  }
  if (reply.getMessageType() == TcrMessage::EXCEPTION ||
      reply.getMessageType() == TcrMessage::CQDATAERROR_MSG_TYPE ||
      reply.getMessageType() == TcrMessage::CQ_EXCEPTION_TYPE) {
    err = ThinClientRegion::handleServerException("CqQuery::stop/close",
                                                  reply.getException());
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw CqQueryException(
          "CqQuery::stop/close: exception at the server side: ",
          reply.getException());
    } else {
      GfErrTypeToException("CqQuery::stop/close", err);
    }
  }
}

/**
 * Return the state of this query.
 * @return STOPPED RUNNING or CLOSED
 */
CqState::StateType CqQueryImpl::getState() { return m_cqState; }

/**
 * Sets the state of the cq.
 * Server side method. Called during cq registration time.
 */
void CqQueryImpl::setCqState(CqState::StateType state) {
  if (isClosed()) {
    throw CqClosedException(("CQ is closed, CqName : " + m_cqName).c_str());
  }
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  m_cqState = state;
}

const CqAttributesMutatorPtr CqQueryImpl::getCqAttributesMutator() const {
  return m_cqAttributesMutator;
}
/**
 * @return Returns the cqOperation.
 */
CqOperation::CqOperationType CqQueryImpl::getCqOperation() {
  return m_cqOperation;
}

/**
 * @param cqOperation The cqOperation to set.
 */
void CqQueryImpl::setCqOperation(CqOperation::CqOperationType cqOperation) {
  m_cqOperation = cqOperation;
}

/**
 * Update CQ stats
 * @param cqEvent object
 */
void CqQueryImpl::updateStats(CqEvent& cqEvent) {
  CqQueryVsdStats* stats = dynamic_cast<CqQueryVsdStats*>(m_stats.ptr());
  stats->incNumEvents();
  switch (cqEvent.getQueryOperation()) {
    case CqOperation::OP_TYPE_CREATE:
      stats->incNumInserts();
      break;
    case CqOperation::OP_TYPE_UPDATE:
      stats->incNumUpdates();
      break;
    case CqOperation::OP_TYPE_DESTROY:
      stats->incNumDeletes();
      break;
    default:
      break;
  }
}

/**
 * Return true if the CQ is in running state
 * @return true if running, false otherwise
 */
bool CqQueryImpl::isRunning() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  return m_cqState == CqState::RUNNING;
}

/**
 * Return true if the CQ is in Sstopped state
 * @return true if stopped, false otherwise
 */
bool CqQueryImpl::isStopped() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  return m_cqState == CqState::STOPPED ||
         (m_proxyCache != NULLPTR && m_proxyCache->isClosed());
}

/**
 * Return true if the CQ is closed
 * @return true if closed, false otherwise
 */
bool CqQueryImpl::isClosed() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  return m_cqState == CqState::CLOSED ||
         (m_proxyCache != NULLPTR && m_proxyCache->isClosed());
}

/**
 * Return true if the CQ is durable
 * @return true if durable, false otherwise
 */
bool CqQueryImpl::isDurable() { return m_isDurable; }

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CqService.hpp"
#include "ReadWriteLock.hpp"
#include <gfcpp/DistributedSystem.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "CqQueryImpl.hpp"
#include "CqEventImpl.hpp"
#include <gfcpp/CqServiceStatistics.hpp>
#include "ThinClientPoolDM.hpp"
#include <gfcpp/CqStatusListener.hpp>
using namespace gemfire;

CqService::CqService(ThinClientBaseDM* tccdm)
    : m_tccdm(tccdm), m_notificationSema(1), m_stats(new CqServiceVsdStats()) {
  m_cqQueryMap = new MapOfCqQueryWithLock();
  m_running = true;
  LOGDEBUG("CqService Started");
}
CqService::~CqService() {
  if (m_cqQueryMap != NULL) delete m_cqQueryMap;
  LOGDEBUG("CqService Destroyed");
}

void CqService::updateStats() {
  CqServiceVsdStats* stats = dynamic_cast<CqServiceVsdStats*>(m_stats.ptr());

  stats->setNumCqsActive(0);
  stats->setNumCqsStopped(0);

  MapOfRegionGuard guard(m_cqQueryMap->mutex());

  stats->setNumCqsOnClient(static_cast<uint32_t>(m_cqQueryMap->current_size()));

  if (m_cqQueryMap->current_size() == 0) return;

  for (MapOfCqQueryWithLock::iterator q = m_cqQueryMap->begin();
       q != m_cqQueryMap->end(); ++q) {
    CqQueryPtr cquery = ((*q).int_id_);
    switch (cquery->getState()) {
      case CqState::RUNNING:
        stats->incNumCqsActive();
        break;
      case CqState::STOPPED:
        stats->incNumCqsStopped();
        break;
      default:
        break;
    }
  }
}

bool CqService::checkAndAcquireLock() {
  if (m_running) {
    m_notificationSema.acquire();
    if (m_running == false) {
      m_notificationSema.release();
      return false;
    }
    return true;
  } else {
    return false;
  }
}

CqQueryPtr CqService::newCq(std::string& cqName, std::string& queryString,
                            CqAttributesPtr& cqAttributes, bool isDurable) {
  if (queryString.empty()) {
    throw IllegalArgumentException("Null queryString is passed. ");
  } else if (cqAttributes == NULLPTR) {
    throw IllegalArgumentException("Null cqAttribute is passed. ");
  }

  // Check if the subscription is enabled on the pool
  ThinClientPoolDM* pool = dynamic_cast<ThinClientPoolDM*>(m_tccdm);
  if (pool != NULL && !pool->getSubscriptionEnabled()) {
    LOGERROR(
        "Cannot create CQ because subscription is not enabled on the pool.");
    throw IllegalStateException(
        "Cannot create CQ because subscription is not enabled on the pool.");
  }

  // check for durable client
  if (isDurable) {
    SystemProperties* sysProps = DistributedSystem::getSystemProperties();
    const char* durableID =
        (sysProps != NULL) ? sysProps->durableClientId() : NULL;
    if (durableID == NULL || strlen(durableID) == 0) {
      LOGERROR("Cannot create durable CQ because client is not durable.");
      throw IllegalStateException(
          "Cannot create durable CQ because client is not durable.");
    }
  }

  // Check if the given cq already exists.
  if (!cqName.empty() && isCqExists(cqName)) {
    throw CqExistsException(
        ("CQ with the given name already exists. CqName : " + cqName).c_str());
  }

  UserAttributesPtr ua;
  ua = NULLPTR;
  if (m_tccdm != NULL && m_tccdm->isMultiUserMode()) {
    ua = TSSUserAttributesWrapper::s_gemfireTSSUserAttributes
             ->getUserAttributes();
  }

  CqServicePtr cqs(this);
  CqQueryImpl* cQuery =
      new CqQueryImpl(cqs, cqName, queryString, cqAttributes, isDurable, ua);
  cQuery->initCq();
  CqQueryPtr ptr(cQuery);
  return ptr;
}

/**
 * Adds the given CQ and cqQuery object into the CQ map.
 */
void CqService::addCq(std::string& cqName, CqQueryPtr& cq) {
  try {
    MapOfRegionGuard guard(m_cqQueryMap->mutex());
    CqQueryPtr tmp;
    if (0 == m_cqQueryMap->find(cqName, tmp)) {
      throw CqExistsException("CQ with given name already exists. ");
    }
    m_cqQueryMap->bind(cqName, cq);
  } catch (Exception& e) {
    throw e;
  }
}

/**
 * Removes given CQ from the cqMap..
 */
void CqService::removeCq(std::string& cqName) {
  try {
    MapOfRegionGuard guard(m_cqQueryMap->mutex());
    m_cqQueryMap->unbind(cqName);
  } catch (Exception& e) {
    throw e;
  }
}

/**
 * Retrieve a CqQuery by name.
 * @return the CqQuery or null if not found
 */
CqQueryPtr CqService::getCq(std::string& cqName) {
  MapOfRegionGuard guard(m_cqQueryMap->mutex());
  CqQueryPtr tmp;
  if (0 != m_cqQueryMap->find(cqName, tmp)) {
    LOGWARN("Failed to get the specified CQ: %s", cqName.c_str());
  } else {
    return tmp;
  }
  return NULLPTR;
}

/**
 * Clears the CQ Query Map.
 */
void CqService::clearCqQueryMap() {
  Log::fine("Cleaning clearCqQueryMap.");
  try {
    MapOfRegionGuard guard(m_cqQueryMap->mutex());
    m_cqQueryMap->unbind_all();
  } catch (Exception& e) {
    throw e;
  }
}

/**
 * Retrieve  all registered CQs
 */
void CqService::getAllCqs(VectorOfCqQuery& cqVec) {
  cqVec.clear();
  MapOfRegionGuard guard(m_cqQueryMap->mutex());
  if (m_cqQueryMap->current_size() == 0) return;
  cqVec.reserve(static_cast<int32_t>(m_cqQueryMap->current_size()));
  for (MapOfCqQueryWithLock::iterator q = m_cqQueryMap->begin();
       q != m_cqQueryMap->end(); ++q) {
    cqVec.push_back((*q).int_id_);
  }
}

/**
 * Executes all the cqs on this client.
 */
void CqService::executeAllClientCqs(bool afterFailover) {
  // ACE_Guard< ACE_Recursive_Thread_Mutex > _guard( m_mutex );
  VectorOfCqQuery cqVec;
  getAllCqs(cqVec);
  // MapOfRegionGuard guard( m_cqQueryMap->mutex() );
  executeCqs(cqVec, afterFailover);
}

/**
 * Executes all CQs on the specified endpoint after failover.
 */
GfErrType CqService::executeAllClientCqs(TcrEndpoint* endpoint) {
  VectorOfCqQuery cqVec;
  getAllCqs(cqVec);
  return executeCqs(cqVec, endpoint);
}

/**
 * Executes all the given cqs on the specified endpoint after failover.
 */
GfErrType CqService::executeCqs(VectorOfCqQuery& cqs, TcrEndpoint* endpoint) {
  if (cqs.empty()) {
    return GF_NOERR;
  }

  GfErrType err = GF_NOERR;
  GfErrType opErr = GF_NOERR;

  for (int32_t cqCnt = 0; cqCnt < cqs.length(); cqCnt++) {
    CqQueryPtr cq = cqs[cqCnt];
    CqQueryImpl* cQueryImpl = static_cast<CqQueryImpl*>(cq.ptr());
    if (!cq->isClosed() && cq->isRunning()) {
      opErr = cQueryImpl->execute(endpoint);
      if (err == GF_NOERR) {
        err = opErr;
      }
    }
  }
  return err;
}

/**
 * Executes all the given cqs.
 */
void CqService::executeCqs(VectorOfCqQuery& cqs, bool afterFailover) {
  if (cqs.empty()) {
    return;
  }
  std::string cqName;
  for (int32_t cqCnt = 0; cqCnt < cqs.length(); cqCnt++) {
    CqQueryPtr cq = cqs[cqCnt];
    CqQueryImpl* cQueryImpl = dynamic_cast<CqQueryImpl*>(cq.ptr());
    if (!cq->isClosed() &&
        (cq->isStopped() || (cq->isRunning() && afterFailover))) {
      try {
        cqName = cq->getName();
        if (afterFailover) {
          cQueryImpl->executeAfterFailover();
        } else {
          cq->execute();
        }
      } catch (QueryException& qe) {
        LOGFINE("%s", ("Failed to execute the CQ, CqName : " + cqName +
                       " Error : " + qe.getMessage())
                          .c_str());
      } catch (CqClosedException& cce) {
        LOGFINE(("Failed to execute the CQ, CqName : " + cqName + " Error : " +
                 cce.getMessage())
                    .c_str());
      }
    }
  }
}

/**
 * Stops all the cqs
 */
void CqService::stopAllClientCqs() {
  VectorOfCqQuery cqVec;
  getAllCqs(cqVec);
  // MapOfRegionGuard guard( m_cqQueryMap->mutex() );
  stopCqs(cqVec);
}

/**
 * Stops all the specified cqs.
 */
void CqService::stopCqs(VectorOfCqQuery& cqs) {
  if (cqs.empty()) {
    return;
  }

  std::string cqName;
  for (int32_t cqCnt = 0; cqCnt < cqs.length(); cqCnt++) {
    CqQueryPtr cq = cqs[cqCnt];
    if (!cq->isClosed() && cq->isRunning()) {
      try {
        cqName = cq->getName();
        cq->stop();
      } catch (QueryException& qe) {
        Log::fine(("Failed to stop the CQ, CqName : " + cqName + " Error : " +
                   qe.getMessage())
                      .c_str());
      } catch (CqClosedException& cce) {
        Log::fine(("Failed to stop the CQ, CqName : " + cqName + " Error : " +
                   cce.getMessage())
                      .c_str());
      }
    }
  }
}

void CqService::closeCqs(VectorOfCqQuery& cqs) {
  LOGDEBUG("closeCqs() TcrMessage::isKeepAlive() = %d ",
           TcrMessage::isKeepAlive());
  if (!cqs.empty()) {
    std::string cqName;
    for (int32_t cqCnt = 0; cqCnt < cqs.length(); cqCnt++) {
      try {
        CqQueryImpl* cq = dynamic_cast<CqQueryImpl*>(cqs[cqCnt].ptr());
        cqName = cq->getName();
        LOGDEBUG("closeCqs() cqname = %s isDurable = %d ", cqName.c_str(),
                 cq->isDurable());
        if (!(cq->isDurable() && TcrMessage::isKeepAlive())) {
          cq->close(true);
        } else {
          cq->close(false);
        }
      } catch (QueryException& qe) {
        Log::fine(("Failed to close the CQ, CqName : " + cqName + " Error : " +
                   qe.getMessage())
                      .c_str());
      } catch (CqClosedException& cce) {
        Log::fine(("Failed to close the CQ, CqName : " + cqName + " Error : " +
                   cce.getMessage())
                      .c_str());
      }
    }
  }
}

/**
 * Get statistics information for all CQs
 * @return the CqServiceStatistics
 */
CqServiceStatisticsPtr CqService::getCqServiceStatistics() { return m_stats; }

/**
 * Close the CQ Service after cleanup if any.
 *
 */
void CqService::closeCqService() {
  if (m_running) {
    m_running = false;
    m_notificationSema.acquire();
    cleanup();
    m_notificationSema.release();
  }
}
void CqService::closeAllCqs() {
  Log::fine("closeAllCqs()");
  VectorOfCqQuery cqVec;
  getAllCqs(cqVec);
  Log::fine("closeAllCqs() 1");
  MapOfRegionGuard guard(m_cqQueryMap->mutex());
  Log::fine("closeAllCqs() 2");
  closeCqs(cqVec);
}

/**
 * Cleans up the CqService.
 */
void CqService::cleanup() {
  Log::fine("Cleaning up CqService.");

  // Close All the CQs.
  // Need to take care when Clients are still connected...
  closeAllCqs();

  // Clear cqQueryMap.
  clearCqQueryMap();
}

/*
 * Checks if CQ with the given name already exists.
 * @param cqName name of the CQ.
 * @return true if exists else false.
 */
bool CqService::isCqExists(std::string& cqName) {
  bool status = false;
  try {
    MapOfRegionGuard guard(m_cqQueryMap->mutex());
    CqQueryPtr tmp;
    status = (0 == m_cqQueryMap->find(cqName, tmp));
  } catch (Exception& ex) {
    LOGFINE("Exception (%s) in isCQExists, ignored ",
            ex.getMessage());  // Ignore.
  }
  return status;
}
void CqService::receiveNotification(TcrMessage* msg) {
  invokeCqListeners(msg->getCqs(), msg->getMessageTypeForCq(), msg->getKey(),
                    msg->getValue(), msg->getDeltaBytes(), msg->getEventId());
  GF_SAFE_DELETE(msg);
  m_notificationSema.release();
}

/**
 * Invokes the CqListeners for the given CQs.
 * @param cqs list of cqs with the cq operation from the Server.
 * @param messageType base operation
 * @param key
 * @param value
 */
void CqService::invokeCqListeners(const std::map<std::string, int>* cqs,
                                  uint32_t messageType, CacheableKeyPtr key,
                                  CacheablePtr value,
                                  CacheableBytesPtr deltaValue,
                                  EventIdPtr eventId) {
  LOGDEBUG("CqService::invokeCqListeners");
  CqQueryPtr cQuery;
  CqQueryImpl* cQueryImpl;
  for (std::map<std::string, int>::const_iterator iter = cqs->begin();
       iter != cqs->end(); ++iter) {
    std::string cqName = iter->first;
    cQuery = getCq(cqName);
    cQueryImpl = dynamic_cast<CqQueryImpl*>(cQuery.ptr());
    if (cQueryImpl == NULL || !cQueryImpl->isRunning()) {
      LOGFINE("Unable to invoke CqListener, %s, CqName: %s",
              (cQueryImpl == NULL) ? "CQ not found" : "CQ is Not running",
              cqName.c_str());
      continue;
    }

    int cqOp = iter->second;

    // If Region destroy event, close the cq.
    if (cqOp == TcrMessage::DESTROY_REGION) {
      // The close will also invoke the listeners close().
      try {
        cQueryImpl->close(false);
      } catch (Exception& ex) {
        // handle?
        LOGFINE("Exception while invoking CQ listeners: %s", ex.getMessage());
      }
      continue;
    }

    // Construct CqEvent.
    CqEventImpl* cqEvent =
        new CqEventImpl(cQuery, getOperation(messageType), getOperation(cqOp),
                        key, value, m_tccdm, deltaValue, eventId);

    // Update statistics
    cQueryImpl->updateStats(*cqEvent);

    // invoke CQ Listeners.
    VectorOfCqListener cqListeners;
    cQueryImpl->getCqAttributes()->getCqListeners(cqListeners);
    /*
    Log::fine(("Invoking CqListeners for the CQ, CqName : " + cqName +
        " , Number of Listeners : " + cqListeners.length() + " cqEvent : " +
    cqEvent);
        */

    for (int32_t lCnt = 0; lCnt < cqListeners.length(); lCnt++) {
      try {
        // Check if the listener is not null, it could have been changed/reset
        // by the CqAttributeMutator.
        if (cqListeners[lCnt] != NULLPTR) {
          if (cqEvent->getError() == true) {
            cqListeners[lCnt]->onError(*cqEvent);
          } else {
            cqListeners[lCnt]->onEvent(*cqEvent);
          }
        }
        // Handle client side exceptions.
      } catch (Exception& ex) {
        LOGWARN(("Exception in the CqListener of the CQ named " + cqName +
                 ", error: " + ex.getMessage())
                    .c_str());
      }
    }
    delete cqEvent;
  }
}

void CqService::invokeCqConnectedListeners(std::string poolName,
                                           bool connected) {
  CqQueryPtr cQuery;
  CqQueryImpl* cQueryImpl;
  VectorOfCqQuery vec;
  getAllCqs(vec);
  for (int32_t i = 0; i < vec.size(); i++) {
    std::string cqName = vec.at(i)->getName();
    cQuery = getCq(cqName);
    cQueryImpl = dynamic_cast<CqQueryImpl*>(cQuery.ptr());
    if (cQueryImpl == NULL || !cQueryImpl->isRunning()) {
      LOGFINE("Unable to invoke CqStatusListener, %s, CqName: %s",
              (cQueryImpl == NULL) ? "CQ not found" : "CQ is Not running",
              cqName.c_str());
      continue;
    }

    // Check cq pool to determine if the pool matches, if not continue.
    ThinClientPoolDM* poolDM =
        dynamic_cast<ThinClientPoolDM*>(cQueryImpl->getDM());
    if (poolDM != NULL) {
      std::string pName = poolDM->getName();
      if (pName.compare(poolName) != 0) {
        continue;
      }
    }

    // invoke CQ Listeners.
    VectorOfCqListener cqListeners;
    cQueryImpl->getCqAttributes()->getCqListeners(cqListeners);
    for (int32_t lCnt = 0; lCnt < cqListeners.length(); lCnt++) {
      try {
        // Check if the listener is not null, it could have been changed/reset
        // by the CqAttributeMutator.
        CqStatusListenerPtr statusLstr = NULLPTR;
        try {
          statusLstr = dynCast<CqStatusListenerPtr>(cqListeners[lCnt]);
        } catch (const ClassCastException&) {
          // ignore
        }
        if (statusLstr != NULLPTR) {
          if (connected) {
            statusLstr->onCqConnected();
          } else {
            statusLstr->onCqDisconnected();
          }
        }
        // Handle client side exceptions.
      } catch (Exception& ex) {
        LOGWARN(("Exception in the CqStatusListener of the CQ named " + cqName +
                 ", error: " + ex.getMessage())
                    .c_str());
      }
    }
  }
}

/**
 * Returns the Operation for the given EnumListenerEvent type.
 * @param eventType
 * @return Operation
 */
CqOperation::CqOperationType CqService::getOperation(int eventType) {
  CqOperation::CqOperationType op = CqOperation::OP_TYPE_INVALID;
  switch (eventType) {
    case TcrMessage::LOCAL_CREATE:
      op = CqOperation::OP_TYPE_CREATE;
      break;

    case TcrMessage::LOCAL_UPDATE:
      op = CqOperation::OP_TYPE_UPDATE;
      break;

    case TcrMessage::LOCAL_DESTROY:
      op = CqOperation::OP_TYPE_DESTROY;
      break;

    case TcrMessage::LOCAL_INVALIDATE:
      op = CqOperation::OP_TYPE_INVALIDATE;
      break;

    case TcrMessage::CLEAR_REGION:
      op = CqOperation::OP_TYPE_REGION_CLEAR;
      break;

      //      case TcrMessage::INVALIDATE_REGION :
      //        op = CqOperation::OP_TYPE_REGION_INVALIDATE;
      //        break;
  }
  return op;
}

/**
  * Gets all the durable CQs registered by this client.
  *
  * @return List of names of registered durable CQs, empty list if no durable
 * cqs.
  */
CacheableArrayListPtr CqService::getAllDurableCqsFromServer() {
  TcrMessageGetDurableCqs msg(m_tccdm);
  TcrMessageReply reply(true, m_tccdm);

  // intialize the chunked response hadler for durable cqs list
  ChunkedDurableCQListResponse* resultCollector =
      new ChunkedDurableCQListResponse(reply);
  reply.setChunkedResultHandler(
      static_cast<TcrChunkedResult*>(resultCollector));
  reply.setTimeout(DEFAULT_QUERY_RESPONSE_TIMEOUT);

  GfErrType err = GF_NOERR;
  err = m_tccdm->sendSyncRequest(msg, reply);
  if (err != GF_NOERR) {
    LOGDEBUG("CqService::getAllDurableCqsFromServer!!!!");
    GfErrTypeToException("CqService::getAllDurableCqsFromServer:", err);
  }
  if (reply.getMessageType() == TcrMessage::EXCEPTION ||
      reply.getMessageType() == TcrMessage::GET_DURABLE_CQS_DATA_ERROR) {
    err = ThinClientRegion::handleServerException(
        "CqService::getAllDurableCqsFromServer", reply.getException());
    if (err == GF_CACHESERVER_EXCEPTION) {
      throw CqQueryException(
          "CqService::getAllDurableCqsFromServer: exception "
          "at the server side: ",
          reply.getException());
    } else {
      GfErrTypeToException("CqService::getAllDurableCqsFromServer", err);
    }
  }

  CacheableArrayListPtr tmpRes = resultCollector->getResults();
  delete resultCollector;
  return tmpRes;
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CqEventImpl.hpp"
#include <gfcpp/CacheableString.hpp>
#include "ThinClientPoolHADM.hpp"
#include "ThinClientCacheDistributionManager.hpp"
#include "TcrMessage.hpp"

using namespace gemfire;
CqEventImpl::CqEventImpl(CqQueryPtr& cQuery,
                         CqOperation::CqOperationType baseOp,
                         CqOperation::CqOperationType cqOp,
                         CacheableKeyPtr& key, CacheablePtr& value,
                         ThinClientBaseDM* tcrdm, CacheableBytesPtr deltaBytes,
                         EventIdPtr eventId)
    : m_error(false) {
  m_cQuery = cQuery;
  m_queryOp = cqOp;
  m_baseOp = baseOp;
  m_key = key;
  m_newValue = value;
  if (m_queryOp == CqOperation::OP_TYPE_INVALID) m_error = true;
  m_tcrdm = tcrdm;
  m_deltaValue = deltaBytes;
  m_eventId = eventId;
}

CqQueryPtr CqEventImpl::getCq() const { return m_cQuery; }

CqOperation::CqOperationType CqEventImpl::getBaseOperation() const {
  return m_baseOp;
}

/**
 * Get the the operation on the query results. Supported operations include
 * update, create, and destroy.
 */
CqOperation::CqOperationType CqEventImpl::getQueryOperation() const {
  return m_queryOp;
}

/**
 * Get the key relating to the event.
 * @return Object key.
 */
CacheableKeyPtr CqEventImpl::getKey() const { return m_key; }
/**
 * Get the new value of the modification.
 *  If there is no new value because this is a delete, then
 *  return null.
 */
CacheablePtr CqEventImpl::getNewValue() const {
  if (m_deltaValue == NULLPTR) {
    return m_newValue;
  } else {
    // Get full object for delta
    TcrMessageRequestEventValue fullObjectMsg(m_eventId);
    TcrMessageReply reply(true, NULL);
    ThinClientPoolHADM* poolHADM = dynamic_cast<ThinClientPoolHADM*>(m_tcrdm);
    GfErrType err = GF_NOTCON;
    if (poolHADM) {
      err = poolHADM->sendRequestToPrimary(fullObjectMsg, reply);
    } else {
      err = static_cast<ThinClientCacheDistributionManager*>(m_tcrdm)
                ->sendRequestToPrimary(fullObjectMsg, reply);
    }
    CacheablePtr fullObject = NULLPTR;
    if (err == GF_NOERR) {
      fullObject = reply.getValue();
    }
    return fullObject;
  }
}

bool CqEventImpl::getError() { return m_error; }

std::string CqEventImpl::toString() {
  char buffer[1024];
  ACE_OS::snprintf(
      buffer, 1024,
      "CqEvent CqName=%s; base operation=%d; cq operation= %d;key=%s;value=%s",
      m_cQuery->getName(), m_baseOp, m_queryOp, m_key->toString()->asChar(),
      m_newValue->toString()->asChar());
  return buffer;
}

CacheableBytesPtr CqEventImpl::getDeltaValue() const { return m_deltaValue; }

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CqAttributesImpl.hpp"
#include <gfcpp/ExceptionTypes.hpp>
using namespace gemfire;
void CqAttributesImpl::getCqListeners(VectorOfCqListener& vl) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  vl.clear();
  //        vl.reserve(m_cqListeners.size());
  vl = m_cqListeners;
  //	for(size_t i=0; i < m_cqListeners.size(); i++)
  //	  vl[i]  = m_cqListeners[i];
}

void CqAttributesImpl::addCqListener(CqListenerPtr& cql) {
  if (cql == NULLPTR) {
    throw IllegalArgumentException("addCqListener parameter was null");
  }
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  m_cqListeners.push_back(cql);
}

CqAttributesImpl* CqAttributesImpl::clone() {
  CqAttributesImpl* ptr = new CqAttributesImpl();
  ptr->setCqListeners(m_cqListeners);
  return ptr;
}

void CqAttributesImpl::setCqListeners(VectorOfCqListener& addedListeners) {
  if (addedListeners.empty() == true) {
    LOGWARN("setCqListeners parameter had a null element, nothing to be set");
    return;
  }
  VectorOfCqListener oldListeners(m_cqListeners);
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
    m_cqListeners = addedListeners;
  }
  if (!oldListeners.empty()) {
    for (int32_t i = 0; i < oldListeners.length(); i++) {
      try {
        oldListeners[i]->close();
        // Handle client side exceptions.
      } catch (Exception& ex) {
        LOGWARN("Exception occured while closing CQ Listener %s Error",
                ex.getMessage());
      }
    }
    oldListeners.clear();
  }
}

void CqAttributesImpl::removeCqListener(CqListenerPtr& cql) {
  if (cql == NULLPTR) {
    throw IllegalArgumentException("removeCqListener parameter was null");
  }
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_mutex);
  if (!m_cqListeners.empty()) {
    for (int32_t i = 0; i < m_cqListeners.size(); i++) {
      if (m_cqListeners.at(i) == cql) {
        m_cqListeners.erase(i);
      }
    }
    try {
      cql->close();
      // Handle client side exceptions.
    } catch (Exception& ex) {
      LOGWARN("Exception closing CQ Listener %s Error ", ex.getMessage());
    }
  }
}

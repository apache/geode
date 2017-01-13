/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CqAttributesMutatorImpl.hpp"
#include "CqAttributesImpl.hpp"
using namespace gemfire;
CqAttributesMutatorImpl::CqAttributesMutatorImpl(const CqAttributesPtr& impl)
    : m_cqAttributes(const_cast<CqAttributes*>(impl.ptr())) {}

void CqAttributesMutatorImpl::addCqListener(const CqListenerPtr& aListener) {
  CqAttributesImpl* cqImpl =
      dynamic_cast<CqAttributesImpl*>(m_cqAttributes.ptr());
  CqListenerPtr listener = dynCast<CqListenerPtr>(aListener);
  cqImpl->addCqListener(listener);
}

void CqAttributesMutatorImpl::removeCqListener(const CqListenerPtr& aListener) {
  CqAttributesImpl* cqImpl =
      dynamic_cast<CqAttributesImpl*>(m_cqAttributes.ptr());
  CqListenerPtr listener = dynCast<CqListenerPtr>(aListener);
  cqImpl->removeCqListener(listener);
}

void CqAttributesMutatorImpl::setCqListeners(VectorOfCqListener& newListeners) {
  CqAttributesImpl* cqImpl =
      dynamic_cast<CqAttributesImpl*>(m_cqAttributes.ptr());
  cqImpl->setCqListeners(newListeners);
}

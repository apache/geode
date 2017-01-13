/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/CqAttributesFactory.hpp>
#include <CqAttributesImpl.hpp>
#include <gfcpp/ExceptionTypes.hpp>

using namespace gemfire;

CqAttributesFactory::CqAttributesFactory() {
  m_cqAttributes = new CqAttributesImpl();
}
CqAttributesFactory::CqAttributesFactory(CqAttributesPtr &cqAttributes) {
  CqAttributesImpl *cqImpl = new CqAttributesImpl();
  m_cqAttributes = cqImpl;
  VectorOfCqListener vl;
  CqAttributesImpl *cqAttr =
      dynamic_cast<CqAttributesImpl *>(cqAttributes.ptr());
  cqAttr->getCqListeners(vl);
  cqImpl->setCqListeners(vl);
}

void CqAttributesFactory::addCqListener(const CqListenerPtr &cqListener) {
  if (cqListener == NULLPTR) {
    throw IllegalArgumentException("addCqListener parameter was null");
  }
  CqAttributesImpl *cqImpl =
      dynamic_cast<CqAttributesImpl *>(m_cqAttributes.ptr());
  CqListenerPtr listener = dynCast<CqListenerPtr>(cqListener);
  cqImpl->addCqListener(listener);
}

void CqAttributesFactory::initCqListeners(VectorOfCqListener &cqListeners) {
  CqAttributesImpl *cqImpl =
      dynamic_cast<CqAttributesImpl *>(m_cqAttributes.ptr());
  cqImpl->setCqListeners(cqListeners);
}

CqAttributesPtr CqAttributesFactory::create() {
  CqAttributesImpl *cqImpl =
      dynamic_cast<CqAttributesImpl *>(m_cqAttributes.ptr());
  CqAttributesPtr ptr(cqImpl->clone());
  return ptr;
}

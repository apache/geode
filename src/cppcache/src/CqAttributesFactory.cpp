/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

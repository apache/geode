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

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "LocatorListRequest.hpp"
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/CacheableString.hpp>
using namespace gemfire;
LocatorListRequest::LocatorListRequest(const std::string& servergroup)
    : m_servergroup(servergroup) {}

void LocatorListRequest::toData(DataOutput& output) const {
  // CacheableStringPtr pxr = CacheableString::create( m_servergroup.c_str());
  // output.writeObject(pxr);// changed
  output.writeNativeString(m_servergroup.c_str());
}
Serializable* LocatorListRequest::fromData(DataInput& input) { return NULL; }
int8_t LocatorListRequest::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::LocatorListRequest);
}
uint32_t LocatorListRequest::objectSize() const { return 0; }

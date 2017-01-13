/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "LocatorListResponse.hpp"
#include <gfcpp/DataInput.hpp>
#include <vector>
using namespace gemfire;
LocatorListResponse* LocatorListResponse::fromData(DataInput& input) {
  readList(input);
  input.readBoolean(&m_isBalanced);
  return this;
}
int8_t LocatorListResponse::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::LocatorListResponse);
}
uint32_t LocatorListResponse::objectSize() const { return 0; }
void LocatorListResponse::readList(DataInput& input) {
  uint32_t size = 0;
  input.readInt(&size);
  for (uint32_t i = 0; i < size; i++) {
    ServerLocation temp;
    temp.fromData(input);
    m_locators.push_back(temp);
  }
}
const std::vector<ServerLocation>& LocatorListResponse::getLocators() const {
  return m_locators;
}
bool LocatorListResponse::isBalanced() const { return m_isBalanced; }

Serializable* LocatorListResponse::create() {
  return new LocatorListResponse();
}

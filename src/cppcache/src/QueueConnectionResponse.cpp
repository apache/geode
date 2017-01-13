/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "QueueConnectionResponse.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/DataInput.hpp>
#include "ServerLocation.hpp"
using namespace gemfire;
QueueConnectionResponse* QueueConnectionResponse::fromData(DataInput& input) {
  input.readBoolean(&m_durableQueueFound);
  readList(input);
  return this;
}
int8_t QueueConnectionResponse::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::QueueConnectionResponse);
}

uint32_t QueueConnectionResponse::objectSize() const {
  return 0;  // Can be implemented later.
}

void QueueConnectionResponse::readList(DataInput& input) {
  uint32_t size = 0;
  input.readInt(&size);
  for (uint32_t i = 0; i < size; i++) {
    ServerLocation temp;
    temp.fromData(input);
    m_list.push_back(temp);
  }
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ClientConnectionResponse.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
using namespace gemfire;
ClientConnectionResponse* ClientConnectionResponse::fromData(DataInput& input) {
  input.readBoolean(&m_serverFound);
  if (m_serverFound) {
    m_server.fromData(input);
  }
  return this;
}
int8_t ClientConnectionResponse::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::ClientConnectionResponse);
}
uint32_t ClientConnectionResponse::objectSize() const {
  return (m_server.objectSize());
}
ServerLocation ClientConnectionResponse::getServerLocation() const {
  return m_server;
}

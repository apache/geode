/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "GetAllServersResponse.hpp"

using namespace gemfire;

void GetAllServersResponse::toData(DataOutput& output) const {
  int32_t length = static_cast<int32_t>(m_servers.size());
  output.writeInt(length);
  for (int32_t i = 0; i < length; i++) {
    SerializablePtr sPtr(&m_servers.at(i));
    output.writeObject(sPtr);
  }
}
Serializable* GetAllServersResponse::fromData(DataInput& input) {
  int length = 0;

  input.readInt(&length);
  LOGFINER("GetAllServersResponse::fromData length = %d ", length);
  for (int i = 0; i < length; i++) {
    ServerLocation sLoc;
    sLoc.fromData(input);
    m_servers.push_back(sLoc);
  }
  return this;
}

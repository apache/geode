/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "QueueConnectionRequest.hpp"
#include "GemfireTypeIdsImpl.hpp"
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
using namespace gemfire;

void QueueConnectionRequest::toData(DataOutput& output) const {
  // CacheableStringPtr abe = CacheableString::create(m_serverGp.c_str());
  // output.writeObject(abe);// changed
  output.writeNativeString(m_serverGp.c_str());
  output.write(static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte));
  output.write(
      static_cast<int8_t>(GemfireTypeIdsImpl::ClientProxyMembershipId));
  uint32_t buffLen;
  const char* buff = m_membershipID.getDSMemberId(buffLen);
  output.writeBytes((uint8_t*)buff, buffLen);
  output.writeInt((int32_t)1);
  output.writeInt(static_cast<int32_t>(m_redundantCopies));
  writeSetOfServerLocation(output);
  output.writeBoolean(m_findDurable);
}
QueueConnectionRequest* QueueConnectionRequest::fromData(
    DataInput& input)  // NOt needed as of now.
{
  return NULL;
}
int8_t QueueConnectionRequest::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::QueueConnectionRequest);
}
uint32_t QueueConnectionRequest::objectSize() const {
  return 0;  // will implement later.
}
std::set<ServerLocation> QueueConnectionRequest::getExcludedServer() const {
  return m_excludedServers;
}
const ClientProxyMembershipID& QueueConnectionRequest::getProxyMemberShipId()
    const {
  return m_membershipID;
}

int QueueConnectionRequest::getRedundentCopies() const {
  return m_redundantCopies;
}
bool QueueConnectionRequest::isFindDurable() const { return m_findDurable; }
void QueueConnectionRequest::writeSetOfServerLocation(
    DataOutput& output) const {
  output.writeInt(static_cast<int32_t>(m_excludedServers.size()));
  std::set<ServerLocation>::const_iterator it = m_excludedServers.begin();
  while (it != m_excludedServers.end()) {
    it->toData(output);
    it++;
  }
}

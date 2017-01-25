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
#include "QueueConnectionRequest.hpp"
#include "GeodeTypeIdsImpl.hpp"
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
using namespace apache::geode::client;

void QueueConnectionRequest::toData(DataOutput& output) const {
  // CacheableStringPtr abe = CacheableString::create(m_serverGp.c_str());
  // output.writeObject(abe);// changed
  output.writeNativeString(m_serverGp.c_str());
  output.write(static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte));
  output.write(
      static_cast<int8_t>(GeodeTypeIdsImpl::ClientProxyMembershipId));
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
  return static_cast<int8_t>(GeodeTypeIdsImpl::QueueConnectionRequest);
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

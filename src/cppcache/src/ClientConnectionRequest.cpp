/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ClientConnectionRequest.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include "GemfireTypeIdsImpl.hpp"
using namespace gemfire;
void ClientConnectionRequest::toData(DataOutput& output) const {
  // output.writeASCII( m_servergroup.c_str() );
  // writeSetOfServerLocation( output );
  // CacheableStringPtr abe = CacheableString::create( m_servergroup.c_str());
  // output.writeObject(abe);// Changed
  output.writeNativeString(m_servergroup.c_str());
  writeSetOfServerLocation(output);
}
Serializable* ClientConnectionRequest::fromData(DataInput& input) {
  return NULL;  // not needed as of now and my guess is  it will never be
                // needed.
}
uint32_t ClientConnectionRequest::objectSize() const { return 0; }
int8_t ClientConnectionRequest::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::ClientConnectionRequest);
}
void ClientConnectionRequest::writeSetOfServerLocation(
    DataOutput& output) const {
  output.writeInt(
      static_cast<int32_t>(m_excludeServergroup_serverLocation.size()));
  std::set<ServerLocation>::const_iterator it =
      m_excludeServergroup_serverLocation.begin();
  while (it != m_excludeServergroup_serverLocation.end()) {
    it->toData(output);
    it++;
  }
}

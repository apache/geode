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

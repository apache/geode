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

#include "ClientReplacementRequest.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include "GemfireTypeIdsImpl.hpp"
using namespace apache::geode::client;
void ClientReplacementRequest::toData(DataOutput& output) const {
  ClientConnectionRequest::toData(output);
  this->m_serverLocation.toData(output);
}
Serializable* ClientReplacementRequest::fromData(DataInput& input) {
  return NULL;  // not needed as of now and my guess is  it will never be
                // needed.
}
int8_t ClientReplacementRequest::typeId() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::ClientReplacementRequest);
}

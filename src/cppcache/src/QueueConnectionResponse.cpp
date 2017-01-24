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

#include "QueueConnectionResponse.hpp"
#include "GeodeTypeIdsImpl.hpp"
#include <gfcpp/DataInput.hpp>
#include "ServerLocation.hpp"
using namespace apache::geode::client;
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

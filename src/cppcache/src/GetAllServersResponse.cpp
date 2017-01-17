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

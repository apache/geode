#pragma once

#ifndef GEODE_GETALLSERVERSRESPONSE_H_
#define GEODE_GETALLSERVERSRESPONSE_H_

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

#include <gfcpp/Serializable.hpp>
#include "ServerLocation.hpp"
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <vector>

namespace apache {
namespace geode {
namespace client {
class GetAllServersResponse : public Serializable {
  std::vector<ServerLocation> m_servers;

 public:
  static Serializable* create() { return new GetAllServersResponse(); }
  GetAllServersResponse() : Serializable() {}
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int32_t classId() const { return 0; }
  virtual int8_t typeId() const {
    return GeodeTypeIdsImpl::GetAllServersResponse;
  }
  virtual int8_t DSFID() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte);
  }
  virtual uint32_t objectSize() const {
    return static_cast<uint32_t>(m_servers.size());
  }
  std::vector<ServerLocation> getServers() { return m_servers; }
  virtual ~GetAllServersResponse() {}
};
typedef SharedPtr<GetAllServersResponse> GetAllServersResponsePtr;
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GETALLSERVERSRESPONSE_H_

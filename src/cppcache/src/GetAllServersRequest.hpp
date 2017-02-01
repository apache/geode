#pragma once

#ifndef GEODE_GETALLSERVERSREQUEST_H_
#define GEODE_GETALLSERVERSREQUEST_H_

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
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/CacheableString.hpp>
#include "GeodeTypeIdsImpl.hpp"
#include <string>

namespace apache {
namespace geode {
namespace client {
class GetAllServersRequest : public Serializable {
  CacheableStringPtr m_serverGroup;

 public:
  GetAllServersRequest(const std::string& serverGroup) : Serializable() {
    m_serverGroup = CacheableString::create(serverGroup.c_str());
  }
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int32_t classId() const { return 0; }
  virtual int8_t typeId() const {
    return GeodeTypeIdsImpl::GetAllServersRequest;
  }
  virtual int8_t DSFID() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte);
  }
  virtual uint32_t objectSize() const { return m_serverGroup->length(); }
  virtual ~GetAllServersRequest() {}
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GETALLSERVERSREQUEST_H_

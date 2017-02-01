#pragma once

#ifndef GEODE_SERVERLOCATIONRESPONSE_H_
#define GEODE_SERVERLOCATIONRESPONSE_H_

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
#include "GeodeTypeIdsImpl.hpp"
namespace apache {
namespace geode {
namespace client {
class ServerLocationResponse : public Serializable {
 public:
  ServerLocationResponse() : Serializable() {}
  virtual void toData(DataOutput& output) const {}  // Not needed as of now
  virtual Serializable* fromData(
      DataInput& input) = 0;  // Has to be implemented by concerte class
  virtual int32_t classId() const { return 0; }
  virtual int8_t typeId() const = 0;  // Has to be implemented by concrete class
  virtual int8_t DSFID() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::FixedIDByte);
  }
  virtual uint32_t objectSize()
      const = 0;  // Has to be implemented by concrete class
  virtual ~ServerLocationResponse() {}  // Virtual destructor
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_SERVERLOCATIONRESPONSE_H_

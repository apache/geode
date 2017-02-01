#pragma once

#ifndef GEODE_GATEWAYEVENTCALLBACKARGUMENT_H_
#define GEODE_GATEWAYEVENTCALLBACKARGUMENT_H_

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
class GatewayEventCallbackArgument : public Serializable {
  SerializablePtr m_callback;

  virtual int32_t classId() const { return 0; }

  virtual int8_t typeId() const {
    return static_cast<int8_t>(GeodeTypeIdsImpl::GatewayEventCallbackArgument);
  }

  virtual int8_t DSFID() const {
    return static_cast<int32_t>(GeodeTypeIdsImpl::FixedIDByte);
  }

  virtual uint32_t objectSize() const { return 0; }

  virtual void toData(DataOutput& output) const {
    throw IllegalStateException(
        "GatewayEventCallbackArgument::toData not implemented");
  }

  virtual Serializable* fromData(DataInput& input) {
    input.readObject(m_callback);
    CacheableStringPtr ignored;
    // input.readObject(ignored);// Changed
    input.readNativeString(ignored);
    int32_t items;
    input.readInt(&items);
    for (int32_t item = 0; item < items; item++) {
      // input.readObject(ignored);// Changed
      input.readNativeString(ignored);
    }
    return m_callback.ptr();
  }

 public:
  inline static Serializable* createDeserializable() {
    return new GatewayEventCallbackArgument();
  }
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GATEWAYEVENTCALLBACKARGUMENT_H_

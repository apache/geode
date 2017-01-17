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
#ifndef __GATEWAYEVENTCALLBACKARGUMENT_HPP__
#define __GATEWAYEVENTCALLBACKARGUMENT_HPP__

#include <gfcpp/Serializable.hpp>
#include "GemfireTypeIdsImpl.hpp"

namespace gemfire {
class GatewayEventCallbackArgument : public Serializable {
  SerializablePtr m_callback;

  virtual int32_t classId() const { return 0; }

  virtual int8_t typeId() const {
    return (int8_t)GemfireTypeIdsImpl::GatewayEventCallbackArgument;
  }

  virtual int8_t DSFID() const {
    return (int32_t)GemfireTypeIdsImpl::FixedIDByte;
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
}

#endif  // __GATEWAYEVENTCALLBACKARGUMENT_HPP__

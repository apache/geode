#pragma once

#ifndef GEODE_GATEWAYSENDEREVENTCALLBACKARGUMENT_H_
#define GEODE_GATEWAYSENDEREVENTCALLBACKARGUMENT_H_

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
class GatewaySenderEventCallbackArgument : public Serializable {
  SerializablePtr m_callback;

  virtual int32_t classId() const {
    return static_cast<int16_t>(
        GeodeTypeIdsImpl::GatewaySenderEventCallbackArgument);
    ;
  }

  virtual int8_t typeId() const {
    throw IllegalStateException(
        "GatewaySenderEventCallbackArgument::typeId not implemented use "
        "classid");
  }

  virtual int8_t DSFID() const {
    return static_cast<int32_t>(GeodeTypeIdsImpl::FixedIDShort);
  }

  virtual uint32_t objectSize() const { return 0; }

  virtual void toData(DataOutput& output) const {
    throw IllegalStateException(
        "GatewaySenderEventCallbackArgument::toData not implemented");
  }

  virtual Serializable* fromData(DataInput& input) {
    input.readObject(m_callback);
    int ignoreInt;
    input.readInt(&ignoreInt);
    int32_t items;
    input.readInt(&items);
    for (int32_t item = 0; item < items; item++) {
      input.readInt(&ignoreInt);
    }
    return m_callback.ptr();
  }

 public:
  inline static Serializable* createDeserializable() {
    return new GatewaySenderEventCallbackArgument();
  }
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GATEWAYSENDEREVENTCALLBACKARGUMENT_H_

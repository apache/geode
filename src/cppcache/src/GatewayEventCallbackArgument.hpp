/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

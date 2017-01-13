#ifndef __GATEWAYSENDEREVENTCALLBACKARGUMENT_HPP__
#define __GATEWAYSENDEREVENTCALLBACKARGUMENT_HPP__

#include <gfcpp/Serializable.hpp>
#include "GemfireTypeIdsImpl.hpp"

namespace gemfire {
class GatewaySenderEventCallbackArgument : public Serializable {
  SerializablePtr m_callback;

  virtual int32_t classId() const {
    return (int16_t)GemfireTypeIdsImpl::GatewaySenderEventCallbackArgument;
    ;
  }

  virtual int8_t typeId() const {
    throw IllegalStateException(
        "GatewaySenderEventCallbackArgument::typeId not implemented use "
        "classid");
  }

  virtual int8_t DSFID() const {
    return (int32_t)GemfireTypeIdsImpl::FixedIDShort;
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
}

#endif  // __GATEWAYSENDEREVENTCALLBACKARGUMENT_HPP__

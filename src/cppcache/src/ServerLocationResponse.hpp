/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __SERVER_LOCATION__RESPONSE__
#define __SERVER_LOCATION__RESPONSE__
#include <gfcpp/Serializable.hpp>
#include "GemfireTypeIdsImpl.hpp"
namespace gemfire {
class ServerLocationResponse : public Serializable {
 public:
  ServerLocationResponse() : Serializable() {}
  virtual void toData(DataOutput& output) const {}  // Not needed as of now
  virtual Serializable* fromData(
      DataInput& input) = 0;  // Has to be implemented by concerte class
  virtual int32_t classId() const { return 0; }
  virtual int8_t typeId() const = 0;  // Has to be implemented by concrete class
  virtual int8_t DSFID() const {
    return (int8_t)GemfireTypeIdsImpl::FixedIDByte;
  }
  virtual uint32_t objectSize()
      const = 0;  // Has to be implemented by concrete class
  virtual ~ServerLocationResponse() {}  // Virtual destructor
};
}
#endif

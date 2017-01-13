/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __SERVER_LOCATION_REQUEST__
#define __SERVER_LOCATION_REQUEST__
#include <gfcpp/Serializable.hpp>
namespace gemfire {
class ServerLocationRequest : public Serializable {
 public:
  ServerLocationRequest() : Serializable() {}
  virtual void toData(DataOutput& output) const = 0;
  virtual Serializable* fromData(DataInput& input) = 0;
  virtual int32_t classId() const;
  virtual int8_t typeId() const = 0;
  virtual int8_t DSFID() const;
  virtual uint32_t objectSize() const = 0;
  virtual ~ServerLocationRequest() {}
};
}
#endif

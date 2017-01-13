/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __LOCATORLISTREQUEST__
#define __LOCATORLISTREQUEST__
#include <string>
#include "ServerLocationRequest.hpp"
#include "GemfireTypeIdsImpl.hpp"
namespace gemfire {
class DataOutput;
class DataInput;
class Serializable;
class LocatorListRequest : public ServerLocationRequest {
 private:
  std::string m_servergroup;

 public:
  LocatorListRequest(const std::string& servergroup = "");
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
};
}
#endif

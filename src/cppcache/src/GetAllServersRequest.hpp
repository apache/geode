/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GET_All_SERVERS_REQUEST_HPP_INCLUDED__
#define __GET_All_SERVERS_REQUEST_HPP_INCLUDED__

#include <gfcpp/Serializable.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/CacheableString.hpp>
#include "GemfireTypeIdsImpl.hpp"
#include <string>

namespace gemfire {
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
    return GemfireTypeIdsImpl::GetAllServersRequest;
  }
  virtual int8_t DSFID() const {
    return (int8_t)GemfireTypeIdsImpl::FixedIDByte;
  }
  virtual uint32_t objectSize() const { return m_serverGroup->length(); }
  virtual ~GetAllServersRequest() {}
};
}

#endif

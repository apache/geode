/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GET_All_SERVERS_RESPONSE_HPP_INCLUDED__
#define __GET_All_SERVERS_RESPONSE_HPP_INCLUDED__

#include <gfcpp/Serializable.hpp>
#include "ServerLocation.hpp"
#include <gfcpp/DataInput.hpp>
#include <gfcpp/DataOutput.hpp>
#include <vector>

namespace gemfire {
class GetAllServersResponse : public Serializable {
  std::vector<ServerLocation> m_servers;

 public:
  static Serializable* create() { return new GetAllServersResponse(); }
  GetAllServersResponse() : Serializable() {}
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int32_t classId() const { return 0; }
  virtual int8_t typeId() const {
    return GemfireTypeIdsImpl::GetAllServersResponse;
  }
  virtual int8_t DSFID() const {
    return (int8_t)GemfireTypeIdsImpl::FixedIDByte;
  }
  virtual uint32_t objectSize() const {
    return static_cast<uint32_t>(m_servers.size());
  }
  std::vector<ServerLocation> getServers() { return m_servers; }
  virtual ~GetAllServersResponse() {}
};
typedef SharedPtr<GetAllServersResponse> GetAllServersResponsePtr;
}

#endif

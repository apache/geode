/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __CLIENT_REPLACEMENT_REQUEST__
#define __CLIENT_REPLACEMENT_REQUEST__
#include "ServerLocationRequest.hpp"
#include "ClientConnectionRequest.hpp"
#include "TcrEndpoint.hpp"
#include <string>
#include <set>
#include "ServerLocation.hpp"

namespace gemfire {
class ClientReplacementRequest : public ClientConnectionRequest {
 public:
  ClientReplacementRequest(const std::string& serverName,
                           const std::set<ServerLocation>& excludeServergroup,
                           std::string servergroup = "")
      : ClientConnectionRequest(excludeServergroup, servergroup),
        m_serverLocation(ServerLocation(serverName)) {}
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual ~ClientReplacementRequest() {}  // Virtual destructor
 private:
  const ServerLocation m_serverLocation;
};
}
#endif

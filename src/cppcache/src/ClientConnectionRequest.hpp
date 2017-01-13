/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __CLIENT_CONNECTION_REQUEST__
#define __CLIENT_CONNECTION_REQUEST__
#include "ServerLocationRequest.hpp"
#include "TcrEndpoint.hpp"
#include <string>
#include <set>
#include "ServerLocation.hpp"
#define _TEST_
namespace gemfire {
class ClientConnectionRequest : public ServerLocationRequest {
 public:
#ifdef _TEST_
  ClientConnectionRequest(const std::set<ServerLocation>& excludeServergroup,
                          std::string servergroup = "")
      : ServerLocationRequest(),
        m_servergroup(servergroup),
        m_excludeServergroup_serverLocation(excludeServergroup) {}
#else
  ClientConnectionRequest(const std::set<TcrEndpoint*>& excludeServergroup,
                          std::string servergroup = "")
      : m_excludeServergroup(excludeServergroup), m_servergroup(servergroup) {}
#endif
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual uint32_t objectSize() const;
  virtual int8_t typeId() const;
  std::string getServerGroup() const { return m_servergroup; }
#ifdef _TEST_
  const std::set<ServerLocation>& getExcludedServerGroup() const {
    return m_excludeServergroup_serverLocation;
  }
#else
  const std::set<TcrEndpoint*>& getExcludedServerGroup() const {
    return m_excludeServergroup;
  }
#endif
  virtual ~ClientConnectionRequest() {}  // Virtual destructor
 private:
  void writeSetOfServerLocation(DataOutput& output) const;
  std::string m_servergroup;
#ifdef _TEST_
  const std::set<ServerLocation>& m_excludeServergroup_serverLocation;
#else
  const std::set<TcrEndpoint*>& m_excludeServergroup;
#endif
};
}
#endif

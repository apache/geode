#pragma once

#ifndef GEODE_CLIENTCONNECTIONREQUEST_H_
#define GEODE_CLIENTCONNECTIONREQUEST_H_

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
#include "ServerLocationRequest.hpp"
#include "TcrEndpoint.hpp"
#include <string>
#include <set>
#include "ServerLocation.hpp"
#define _TEST_
namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CLIENTCONNECTIONREQUEST_H_

#pragma once

#ifndef GEODE_QUEUECONNECTIONREQUEST_H_
#define GEODE_QUEUECONNECTIONREQUEST_H_

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
#include "ServerLocation.hpp"
#include "ClientProxyMembershipID.hpp"
#include <set>
#include <string>
namespace apache {
namespace geode {
namespace client {
class QueueConnectionRequest : public ServerLocationRequest {
 public:
  QueueConnectionRequest(const ClientProxyMembershipID& memId,
                         const std::set<ServerLocation>& excludedServers,
                         int redundantCopies, bool findDurable,
                         std::string serverGp = "")
      : ServerLocationRequest(),
        m_membershipID(memId),
        m_excludedServers(excludedServers),
        m_redundantCopies(redundantCopies),
        m_findDurable(findDurable),
        m_serverGp(serverGp) {}  // No need for default constructor as creating
                                 // request with it does not make sense.
  virtual void toData(DataOutput& output) const;
  virtual QueueConnectionRequest* fromData(DataInput& input);
  virtual int8_t typeId() const;
  virtual uint32_t objectSize() const;
  virtual std::set<ServerLocation> getExcludedServer() const;
  virtual const ClientProxyMembershipID& getProxyMemberShipId() const;
  virtual int getRedundentCopies() const;
  virtual bool isFindDurable() const;
  virtual ~QueueConnectionRequest() {}

 private:
  QueueConnectionRequest(const QueueConnectionRequest&);
  void operator=(const QueueConnectionRequest&);
  void writeSetOfServerLocation(DataOutput& output) const;
  const ClientProxyMembershipID& m_membershipID;
  const std::set<ServerLocation>& m_excludedServers;
  const int m_redundantCopies;
  const bool m_findDurable;
  std::string m_serverGp;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_QUEUECONNECTIONREQUEST_H_

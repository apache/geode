#pragma once

#ifndef GEODE_THINCLIENTLOCATORHELPER_H_
#define GEODE_THINCLIENTLOCATORHELPER_H_

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

#include <string>
#include <gfcpp/gfcpp_globals.hpp>
#include "TcrEndpoint.hpp"
#include "ServerLocation.hpp"
#include <set>
#include <list>
#include "ClientProxyMembershipID.hpp"
#include "GetAllServersRequest.hpp"
#include "GetAllServersResponse.hpp"

namespace apache {
namespace geode {
namespace client {
class TcrEndpoint;
class ThinClientPoolDM;
class ThinClientLocatorHelper {
 public:
  ThinClientLocatorHelper(std::vector<std::string> locHostPort,
                          const ThinClientPoolDM* poolDM);
  GfErrType getEndpointForNewFwdConn(ServerLocation& outEndpoint,
                                     std::string& additionalLoc,
                                     const std::set<ServerLocation>& exclEndPts,
                                     const std::string& serverGrp = "",
                                     const TcrConnection* currentServer = NULL);
  GfErrType getEndpointForNewCallBackConn(
      ClientProxyMembershipID& memId, std::list<ServerLocation>& outEndpoint,
      std::string& additionalLoc, int redundancy,
      const std::set<ServerLocation>& exclEndPts,
      /*const std::set<TcrEndpoint*>& exclEndPts,*/ const std::string&
          serverGrp);
  GfErrType getAllServers(std::vector<ServerLocation>& servers,
                          const std::string& serverGrp);
  int32_t getCurLocatorsNum() {
    return static_cast<int32_t>(m_locHostPort.size());
  }
  GfErrType updateLocators(const std::string& serverGrp = "");

 private:
  Connector* createConnection(Connector*& conn, const char* hostname,
                              int32_t port, uint32_t waitSeconds,
                              int32_t maxBuffSizePool = 0);
  ACE_Thread_Mutex m_locatorLock;
  std::vector<ServerLocation> m_locHostPort;
  const ThinClientPoolDM* m_poolDM;
  ThinClientLocatorHelper(const ThinClientLocatorHelper&);
  ThinClientLocatorHelper& operator=(const ThinClientLocatorHelper&);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_THINCLIENTLOCATORHELPER_H_

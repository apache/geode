/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __LOCATOR_HELPER_HPP_INCLUDED__
#define __LOCATOR_HELPER_HPP_INCLUDED__

#include <string>
#include <gfcpp/gfcpp_globals.hpp>
#include "TcrEndpoint.hpp"
#include "ServerLocation.hpp"
#include <set>
#include <list>
#include "ClientProxyMembershipID.hpp"
#include "GetAllServersRequest.hpp"
#include "GetAllServersResponse.hpp"

namespace gemfire {
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
}
#endif

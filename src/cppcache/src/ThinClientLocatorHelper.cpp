/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "ThinClientLocatorHelper.hpp"
#include "TcpSslConn.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "ClientConnectionRequest.hpp"
#include "ClientReplacementRequest.hpp"
#include "ClientConnectionResponse.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include "QueueConnectionRequest.hpp"
#include "QueueConnectionResponse.hpp"
#include "ThinClientPoolDM.hpp"
#include "LocatorListResponse.hpp"
#include "LocatorListRequest.hpp"
#include <set>
#include <algorithm>
using namespace gemfire;
const int BUFF_SIZE = 3000;

class ConnectionWrapper {
 private:
  Connector*& m_conn;

 public:
  explicit ConnectionWrapper(Connector*& conn) : m_conn(conn) {}
  ~ConnectionWrapper() {
    LOGDEBUG("closing the connection locator1");
    if (m_conn != NULL) {
      LOGDEBUG("closing the connection locator");
      m_conn->close();
      delete m_conn;
    }
  }
};

ThinClientLocatorHelper::ThinClientLocatorHelper(
    std::vector<std::string> locHostPort, const ThinClientPoolDM* poolDM)
    : m_poolDM(poolDM) {
  for (std::vector<std::string>::iterator it = locHostPort.begin();
       it != locHostPort.end(); it++) {
    ServerLocation sl(*it);
    m_locHostPort.push_back(sl);
  }
}

Connector* ThinClientLocatorHelper::createConnection(Connector*& conn,
                                                     const char* hostname,
                                                     int32_t port,
                                                     uint32_t waitSeconds,
                                                     int32_t maxBuffSizePool) {
  Connector* socket = NULL;
  if (DistributedSystem::getSystemProperties()->sslEnabled()) {
    socket = new TcpSslConn(hostname, port, waitSeconds, maxBuffSizePool);
  } else {
    socket = new TcpConn(hostname, port, waitSeconds, maxBuffSizePool);
  }
  conn = socket;
  socket->init();
  return socket;
}

GfErrType ThinClientLocatorHelper::getAllServers(
    std::vector<ServerLocation>& servers, const std::string& serverGrp) {
  ACE_Guard<ACE_Thread_Mutex> guard(m_locatorLock);
  for (unsigned i = 0; i < m_locHostPort.size(); i++) {
    ServerLocation loc = m_locHostPort[i];
    try {
      LOGDEBUG("getAllServers getting servers from server = %s ",
               loc.getServerName().c_str());
      int32_t buffSize = 0;
      if (m_poolDM) {
        buffSize = static_cast<int32_t>(m_poolDM->getSocketBufferSize());
      }
      Connector* conn = NULL;
      ConnectionWrapper cw(conn);
      createConnection(
          conn, loc.getServerName().c_str(), loc.getPort(),
          DistributedSystem::getSystemProperties()->connectTimeout(), buffSize);
      GetAllServersRequest request(serverGrp);
      DataOutput data;
      data.writeInt((int32_t)1001);  // GOSSIPVERSION
      data.writeObject(&request);
      int sentLength = conn->send(
          (char*)(data.getBuffer()), data.getBufferLength(),
          m_poolDM ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
                   : 10 * 1000 * 1000,
          0);
      if (sentLength <= 0) {
        // conn->close(); delete conn; conn = NULL;
        continue;
      }
      char buff[BUFF_SIZE];
      int receivedLength = conn->receive(
          buff, BUFF_SIZE,
          m_poolDM ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
                   : 10 * 1000 * 1000,
          0);
      // conn->close();
      // delete conn; conn = NULL;
      if (receivedLength <= 0) {
        continue;
      }

      DataInput di(reinterpret_cast<uint8_t*>(buff), receivedLength);
      GetAllServersResponsePtr response(NULLPTR);

      /* adongre
       * SSL Enabled on Location and not in the client
       */
      int8_t acceptanceCode;
      di.read(&acceptanceCode);
      if (acceptanceCode == REPLY_SSL_ENABLED &&
          !DistributedSystem::getSystemProperties()->sslEnabled()) {
        LOGERROR("SSL is enabled on locator, enable SSL in client as well");
        throw AuthenticationRequiredException(
            "SSL is enabled on locator, enable SSL in client as well");
      }
      di.rewindCursor(1);

      di.readObject(response);
      servers = response->getServers();
      return GF_NOERR;
    } catch (const AuthenticationRequiredException&) {
      continue;
    } catch (const Exception& excp) {
      LOGFINE("Exception while querying locator: %s: %s", excp.getName(),
              excp.getMessage());
      continue;
    }
  }
  return GF_NOERR;
}

GfErrType ThinClientLocatorHelper::getEndpointForNewCallBackConn(
    ClientProxyMembershipID& memId, std::list<ServerLocation>& outEndpoint,
    std::string& additionalLoc, int redundancy,
    const std::set<ServerLocation>& exclEndPts,
    /*const std::set<TcrEndpoint*>& exclEndPts,*/ const std::string&
        serverGrp) {
  ACE_Guard<ACE_Thread_Mutex> guard(m_locatorLock);
  int locatorsRetry = 3;
  if (m_poolDM) {
    int poolRetry = m_poolDM->getRetryAttempts();
    locatorsRetry = poolRetry <= 0 ? locatorsRetry : poolRetry;
  }
  LOGFINER(
      "ThinClientLocatorHelper::getEndpointForNewCallBackConn locatorsRetry = "
      "%d ",
      locatorsRetry);
  for (unsigned attempts = 0;
       attempts <
       (m_locHostPort.size() == 1 ? locatorsRetry : m_locHostPort.size());
       attempts++) {
    ServerLocation loc;
    if (m_locHostPort.size() == 1) {
      loc = m_locHostPort[0];
    } else {
      loc = m_locHostPort[attempts];
    }

    try {
      LOGFINER("Querying locator at [%s:%d] for queue server from group [%s]",
               loc.getServerName().c_str(), loc.getPort(), serverGrp.c_str());
      int32_t buffSize = 0;
      if (m_poolDM) {
        buffSize = static_cast<int32_t>(m_poolDM->getSocketBufferSize());
      }
      Connector* conn = NULL;
      ConnectionWrapper cw(conn);
      createConnection(
          conn, loc.getServerName().c_str(), loc.getPort(),
          DistributedSystem::getSystemProperties()->connectTimeout(), buffSize);
      QueueConnectionRequest request(memId, exclEndPts, redundancy, false,
                                     serverGrp);
      DataOutput data;
      data.writeInt((int32_t)1001);  // GOSSIPVERSION
      data.writeObject(&request);
      int sentLength = conn->send(
          (char*)(data.getBuffer()), data.getBufferLength(),
          m_poolDM
              ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
              : DistributedSystem::getSystemProperties()->connectTimeout() *
                    1000 * 1000,
          0);
      if (sentLength <= 0) {
        // conn->close(); delete conn; conn = NULL;
        continue;
      }
      char buff[BUFF_SIZE];
      int receivedLength = conn->receive(
          buff, BUFF_SIZE,
          m_poolDM
              ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
              : DistributedSystem::getSystemProperties()->connectTimeout() *
                    1000 * 1000,
          0);
      // conn->close();
      // delete conn; conn = NULL;
      if (receivedLength <= 0) {
        continue;
      }
      DataInput di(reinterpret_cast<uint8_t*>(buff), receivedLength);
      QueueConnectionResponsePtr response(NULLPTR);

      /* adongre
       * ssl defect
       */
      int8_t acceptanceCode;
      di.read(&acceptanceCode);
      if (acceptanceCode == REPLY_SSL_ENABLED &&
          !DistributedSystem::getSystemProperties()->sslEnabled()) {
        LOGERROR("SSL is enabled on locator, enable SSL in client as well");
        throw AuthenticationRequiredException(
            "SSL is enabled on locator, enable SSL in client as well");
      }
      di.rewindCursor(1);
      di.readObject(response);
      outEndpoint = response->getServers();
      return GF_NOERR;
    } catch (const AuthenticationRequiredException& excp) {
      throw excp;
    } catch (const Exception& excp) {
      LOGFINE("Exception while querying locator: %s: %s", excp.getName(),
              excp.getMessage());
      continue;
    }
  }
  throw NoAvailableLocatorsException("Unable to query any locators");
}

GfErrType ThinClientLocatorHelper::getEndpointForNewFwdConn(
    ServerLocation& outEndpoint, std::string& additionalLoc,
    const std::set<ServerLocation>& exclEndPts, const std::string& serverGrp,
    const TcrConnection* currentServer) {
  bool locatorFound = false;
  int locatorsRetry = 3;
  ACE_Guard<ACE_Thread_Mutex> guard(m_locatorLock);
  if (m_poolDM) {
    int poolRetry = m_poolDM->getRetryAttempts();
    locatorsRetry = poolRetry <= 0 ? locatorsRetry : poolRetry;
  }
  LOGFINER(
      "ThinClientLocatorHelper::getEndpointForNewFwdConn locatorsRetry = %d ",
      locatorsRetry);
  for (unsigned attempts = 0;
       attempts <
       (m_locHostPort.size() == 1 ? locatorsRetry : m_locHostPort.size());
       attempts++) {
    ServerLocation serLoc;
    if (m_locHostPort.size() == 1) {
      serLoc = m_locHostPort[0];
    } else {
      serLoc = m_locHostPort[attempts];
    }
    try {
      LOGFINE("Querying locator at [%s:%d] for server from group [%s]",
              serLoc.getServerName().c_str(), serLoc.getPort(),
              serverGrp.c_str());
      int32_t buffSize = 0;
      if (m_poolDM) {
        buffSize = static_cast<int32_t>(m_poolDM->getSocketBufferSize());
      }
      Connector* conn = NULL;
      ConnectionWrapper cw(conn);
      createConnection(
          conn, serLoc.getServerName().c_str(), serLoc.getPort(),
          DistributedSystem::getSystemProperties()->connectTimeout(), buffSize);
      DataOutput data;
      data.writeInt(1001);  // GOSSIPVERSION
      if (currentServer == NULL) {
        LOGDEBUG("Creating ClientConnectionRequest");
        ClientConnectionRequest request(exclEndPts, serverGrp);
        data.writeObject(&request);
      } else {
        LOGDEBUG("Creating ClientReplacementRequest for connection: ",
                 currentServer->getEndpointObject()->name().c_str());
        ClientReplacementRequest request(
            currentServer->getEndpointObject()->name(), exclEndPts, serverGrp);
        data.writeObject(&request);
      }
      int sentLength = conn->send(
          (char*)(data.getBuffer()), data.getBufferLength(),
          m_poolDM
              ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
              : DistributedSystem::getSystemProperties()->connectTimeout() *
                    1000 * 1000,
          0);
      if (sentLength <= 0) {
        // conn->close();
        // delete conn;
        continue;
      }
      char buff[BUFF_SIZE];
      int receivedLength = conn->receive(
          buff, BUFF_SIZE,
          m_poolDM
              ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
              : DistributedSystem::getSystemProperties()->connectTimeout() *
                    1000 * 1000,
          0);
      // conn->close();
      // delete conn;
      if (receivedLength <= 0) {
        continue;  // return GF_EUNDEF;
      }
      DataInput di(reinterpret_cast<uint8_t*>(buff), receivedLength);
      ClientConnectionResponsePtr response;

      /* adongre
       * SSL is enabled on locator and not in the client
       */
      int8_t acceptanceCode;
      di.read(&acceptanceCode);
      if (acceptanceCode == REPLY_SSL_ENABLED &&
          !DistributedSystem::getSystemProperties()->sslEnabled()) {
        LOGERROR("SSL is enabled on locator, enable SSL in client as well");
        throw AuthenticationRequiredException(
            "SSL is enabled on locator, enable SSL in client as well");
      }
      di.rewindCursor(1);

      di.readObject(response);
      response->printInfo();
      if (!response->serverFound()) {
        LOGFINE("Server not found");
        // return GF_NOTCON;
        locatorFound = true;
        continue;
      }
      outEndpoint = response->getServerLocation();
      LOGFINE("Server found at [%s:%d]", outEndpoint.getServerName().c_str(),
              outEndpoint.getPort());
      return GF_NOERR;
    } catch (const AuthenticationRequiredException& excp) {
      throw excp;
    } catch (const Exception& excp) {
      LOGFINE("Exception while querying locator: %s: %s", excp.getName(),
              excp.getMessage());
      continue;
    }
  }

  if (locatorFound) {
    throw NotConnectedException("No servers found");
  } else {
    throw NoAvailableLocatorsException("Unable to query any locators");
  }
}

GfErrType ThinClientLocatorHelper::updateLocators(
    const std::string& serverGrp) {
  ACE_Guard<ACE_Thread_Mutex> guard(m_locatorLock);
  for (unsigned attempts = 0; attempts < m_locHostPort.size(); attempts++) {
    ServerLocation serLoc = m_locHostPort[attempts];
    Connector* conn = NULL;
    try {
      int32_t buffSize = 0;
      if (m_poolDM) {
        buffSize = static_cast<int32_t>(m_poolDM->getSocketBufferSize());
      }
      LOGFINER("Querying locator list at: [%s:%d] for update from group [%s]",
               serLoc.getServerName().c_str(), serLoc.getPort(),
               serverGrp.c_str());
      ConnectionWrapper cw(conn);
      createConnection(
          conn, serLoc.getServerName().c_str(), serLoc.getPort(),
          DistributedSystem::getSystemProperties()->connectTimeout(), buffSize);
      LocatorListRequest request(serverGrp);
      DataOutput data;
      data.writeInt((int32_t)1001);  // GOSSIPVERSION
      data.writeObject(&request);
      int sentLength = conn->send(
          (char*)(data.getBuffer()), data.getBufferLength(),
          m_poolDM
              ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
              : DistributedSystem::getSystemProperties()->connectTimeout() *
                    1000 * 1000,
          0);
      if (sentLength <= 0) {
        //  conn->close();
        // delete conn;
        conn = NULL;
        continue;
      }
      char buff[BUFF_SIZE];
      int receivedLength = conn->receive(
          buff, BUFF_SIZE,
          m_poolDM
              ? (m_poolDM->getReadTimeout() / 1000) * 1000 * 1000
              : DistributedSystem::getSystemProperties()->connectTimeout() *
                    1000 * 1000,
          0);
      // conn->close();
      // delete conn; conn = NULL;
      if (receivedLength <= 0) {
        continue;
      }
      DataInput di(reinterpret_cast<uint8_t*>(buff), receivedLength);
      LocatorListResponsePtr response(new LocatorListResponse());

      /* adongre
       * SSL Enabled on Location and not in the client
       */
      int8_t acceptanceCode;
      di.read(&acceptanceCode);
      if (acceptanceCode == REPLY_SSL_ENABLED &&
          !DistributedSystem::getSystemProperties()->sslEnabled()) {
        LOGERROR("SSL is enabled on locator, enable SSL in client as well");
        throw AuthenticationRequiredException(
            "SSL is enabled on locator, enable SSL in client as well");
      }
      di.rewindCursor(1);

      di.readObject(response);
      std::vector<ServerLocation> locators = response->getLocators();
      if (locators.size() > 0) {
        RandGen randGen;
        std::random_shuffle(locators.begin(), locators.end(), randGen);
      }
      std::vector<ServerLocation> temp(m_locHostPort.begin(),
                                       m_locHostPort.end());
      m_locHostPort.clear();
      m_locHostPort.insert(m_locHostPort.end(), locators.begin(),
                           locators.end());
      for (std::vector<ServerLocation>::iterator it = temp.begin();
           it != temp.end(); it++) {
        std::vector<ServerLocation>::iterator it1 =
            std::find(m_locHostPort.begin(), m_locHostPort.end(), *it);
        if (it1 == m_locHostPort.end()) {
          m_locHostPort.push_back(*it);
        }
      }
      return GF_NOERR;
    } catch (const AuthenticationRequiredException& excp) {
      throw excp;
    } catch (const Exception& excp) {
      LOGFINE("Exception while querying locator: %s: %s", excp.getName(),
              excp.getMessage());
      continue;
    }
  }
  return GF_NOTCON;
}

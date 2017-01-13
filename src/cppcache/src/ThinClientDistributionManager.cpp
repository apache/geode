/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientDistributionManager.hpp"
#include <algorithm>
#include "ThinClientRegion.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "DistributedSystemImpl.hpp"

using namespace gemfire;
ThinClientDistributionManager::ThinClientDistributionManager(
    TcrConnectionManager& connManager, ThinClientRegion* region)
    : ThinClientBaseDM(connManager, region), m_activeEndpoint(-1) {}

void ThinClientDistributionManager::init() {
  std::unordered_set<std::string> endpointNames;

  getEndpointNames(endpointNames);
  m_connManager.connect(this, m_endpoints, endpointNames);
  int32_t numEndpoints = static_cast<int32_t>(m_endpoints.size());
  std::vector<int> randIndex;
  for (int index = 0; index < numEndpoints; ++index) {
    randIndex.push_back(index);
  }
  RandGen randGen;
  std::random_shuffle(randIndex.begin(), randIndex.end(), randGen);
  int index = -1;
  GfErrType err = GF_NOERR;
  while (m_activeEndpoint < 0 && ++index < numEndpoints) {
    m_endpoints[randIndex[index]]->setDM(this);
    if ((err = m_endpoints[randIndex[index]]->registerDM(
             m_clientNotification, false, true)) == GF_NOERR) {
      m_activeEndpoint = randIndex[index];
      LOGFINE("DM: Using endpoint %s",
              m_endpoints[m_activeEndpoint]->name().c_str());
    } else if (isFatalError(err)) {
      m_connManager.disconnect(this, m_endpoints);
      GfErrTypeToException("ThinClientDistributionManager::init", err);
    }
  }
  ThinClientBaseDM::init();
  m_initDone = true;
}
void ThinClientDistributionManager::destroy(bool keepAlive) {
  if (!m_initDone) {
    // nothing to be done
    return;
  }
  DistManagersLockGuard _guard(m_connManager);
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);
  if (m_activeEndpoint >= 0) {
    m_endpoints[m_activeEndpoint]->unregisterDM(m_clientNotification, this);
  }
  LOGFINEST("ThinClientDistributionManager:: starting destroy for region %s",
            (m_region != NULL ? m_region->getFullPath() : "(null)"));
  destroyAction();
  // stop the chunk processing thread
  stopChunkProcessor();
  if (Log::finestEnabled()) {
    std::string endpointStr;
    for (size_t index = 0; index < m_endpoints.size(); ++index) {
      if (index != 0) {
        endpointStr.append(",");
      }
      endpointStr.append(m_endpoints[index]->name());
    }
    LOGFINEST(
        "ThinClientDistributionManager: disconnecting endpoints %s from TCCM",
        endpointStr.c_str());
  }
  m_connManager.disconnect(this, m_endpoints, keepAlive);
  LOGFINEST("ThinClientDistributionManager: completed destroy for region %s",
            (m_region != NULL ? m_region->getFullPath() : "(null)"));
  m_initDone = false;
}

void ThinClientDistributionManager::destroyAction() {}

void ThinClientDistributionManager::getEndpointNames(
    std::unordered_set<std::string>& endpointNames) {}

bool ThinClientDistributionManager::isEndpointAttached(TcrEndpoint* ep) {
  for (std::vector<TcrEndpoint*>::const_iterator iter = m_endpoints.begin();
       iter != m_endpoints.end(); ++iter) {
    if (*iter == ep) {
      return true;
    }
  }
  return false;
}

GfErrType ThinClientDistributionManager::sendSyncRequest(TcrMessage& request,
                                                         TcrMessageReply& reply,
                                                         bool attemptFailover,
                                                         bool isBGThread) {
  GfErrType error = GF_NOTCON;
  bool useActiveEndpoint = true;
  request.setDM(this);
  reply.setDM(this);
  if (request.getMessageType() == TcrMessage::GET_ALL_70 ||
      request.getMessageType() == TcrMessage::GET_ALL_WITH_CALLBACK) {
    request.InitializeGetallMsg(
        request.getCallbackArgument());  // now initialize getall msg
  }
  int currentEndpoint = m_activeEndpoint;
  if (currentEndpoint >= 0 && m_endpoints[currentEndpoint]->connected()) {
    LOGDEBUG(
        "ThinClientDistributionManager::sendSyncRequest: trying to send on "
        "endpoint: %s",
        m_endpoints[currentEndpoint]->name().c_str());
    error = sendRequestToEP(request, reply, m_endpoints[currentEndpoint]);
    useActiveEndpoint = false;
    LOGDEBUG(
        "ThinClientDistributionManager::sendSyncRequest: completed send on "
        "endpoint: %s [error:%d]",
        m_endpoints[currentEndpoint]->name().c_str(), error);
  }

  if (!attemptFailover || error == GF_NOERR) {
    return error;
  }

  bool doRand = true;
  std::vector<int> randIndex;
  int32_t type = request.getMessageType();
  bool forceSelect = false;

  // we need to forceSelect because endpoint connection status
  // is not set to false (in tcrendpoint::send) for a query or putall timeout
  if ((type == TcrMessage::QUERY || type == TcrMessage::QUERY_WITH_PARAMETERS ||
       type == TcrMessage::PUTALL ||
       type == TcrMessage::PUT_ALL_WITH_CALLBACK ||
       type == TcrMessage::EXECUTE_FUNCTION ||
       type == TcrMessage::EXECUTE_REGION_FUNCTION ||
       type == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP ||
       type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE) &&
      error == GF_TIMOUT) {
    forceSelect = true;
  }

  if (!isFatalError(error)) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);
    GfErrType connErr = GF_NOERR;
    while (error != GF_NOERR && !isFatalError(error) &&
           (connErr = selectEndpoint(randIndex, doRand, useActiveEndpoint,
                                     forceSelect)) == GF_NOERR) {
      // if it's a query or putall and we had a timeout, just return with the
      // newly
      // selected endpoint without failover-retry
      if ((type == TcrMessage::QUERY ||
           type == TcrMessage::QUERY_WITH_PARAMETERS ||
           type == TcrMessage::PUTALL ||
           type == TcrMessage::PUT_ALL_WITH_CALLBACK ||
           type == TcrMessage::EXECUTE_FUNCTION ||
           type == TcrMessage::EXECUTE_REGION_FUNCTION ||
           type == TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP ||
           type == TcrMessage::EXECUTECQ_WITH_IR_MSG_TYPE) &&
          error == GF_TIMOUT) {
        return error;
      }
      currentEndpoint = m_activeEndpoint;
      LOGFINEST(
          "ThinClientDistributionManager::sendSyncRequest: trying send on new "
          "endpoint %s",
          m_endpoints[currentEndpoint]->name().c_str());
      error = sendRequestToEP(request, reply, m_endpoints[currentEndpoint]);
      if (error != GF_NOERR) {
        LOGFINE(
            "ThinClientDistributionManager::sendSyncRequest: failed send on "
            "new endpoint %s for message "
            "type %d [error:%d]",
            m_endpoints[currentEndpoint]->name().c_str(),
            request.getMessageType(), error);
      } else {
        LOGFINEST(
            "ThinClientDistributionManager::sendSyncRequest: completed send on "
            "new endpoint: %s",
            m_endpoints[currentEndpoint]->name().c_str());
      }
      useActiveEndpoint = false;
    }
    // : Top-level only sees NotConnectedException or TimeoutException
    if ((error == GF_NOERR && connErr != GF_NOERR) || error == GF_IOERR) {
      error = GF_NOTCON;
    }
  }
  return error;
}

void ThinClientDistributionManager::failover() {
  std::vector<int> randIndex;
  bool doRand = true;
  LOGFINEST("DM: invoked select endpoint via failover thread for region %s",
            (m_region != NULL ? m_region->getFullPath() : "(null)"));
  selectEndpoint(randIndex, doRand);
}

inline GfErrType ThinClientDistributionManager::connectToEndpoint(int epIndex) {
  GfErrType err = GF_NOERR;
  TcrEndpoint* ep = m_endpoints[epIndex];
  ep->setDM(this);
  if ((err = ep->registerDM(m_clientNotification, false, true, this)) ==
      GF_NOERR) {
    LOGFINE("DM: Attempting failover to endpoint %s", ep->name().c_str());
    if (postFailoverAction(ep)) {
      m_activeEndpoint = epIndex;
      LOGFINE("DM: Failover to endpoint %s complete.", ep->name().c_str());
    } else {
      LOGFINE("DM: Post failover action failed for endpoint %s",
              ep->name().c_str());
      ep->unregisterDM(m_clientNotification, this);
      err = GF_EUNDEF;
    }
  } else {
    LOGFINE("DM: Could not connect to endpoint %s", ep->name().c_str());
  }
  return err;
}

GfErrType ThinClientDistributionManager::selectEndpoint(
    std::vector<int>& randIndex, bool& doRand, bool useActiveEndpoint,
    bool forceSelect) {
  // Preconditions
  // 1. Number of endpoints on which DM is registered <= 1

  GfErrType err = GF_NOERR;
  int currentEndpoint = m_activeEndpoint;

  if (currentEndpoint < 0 || !m_endpoints[currentEndpoint]->connected() ||
      forceSelect) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_endpointsLock);
    if (m_activeEndpoint < 0 || !m_endpoints[m_activeEndpoint]->connected() ||
        forceSelect) {  // double check
      currentEndpoint = m_activeEndpoint;
      if (m_activeEndpoint >= 0) {
        m_activeEndpoint = -1;
        m_endpoints[currentEndpoint]->unregisterDM(m_clientNotification, this);
        postUnregisterAction();
      }
      if (!preFailoverAction()) {
        return GF_NOERR;
      }
      int32_t numEndpoints = static_cast<int32_t>(m_endpoints.size());
      err = GF_EUNDEF;
      if (doRand) {
        RandGen randGen;
        for (int idx = 0; idx < numEndpoints; idx++) {
          if (useActiveEndpoint || idx != currentEndpoint) {
            randIndex.push_back(idx);
          }
        }
        std::random_shuffle(randIndex.begin(), randIndex.end(), randGen);
        doRand = false;
      }
      while (!randIndex.empty()) {
        int currIndex = randIndex[0];
        randIndex.erase(randIndex.begin());
        if ((err = connectToEndpoint(currIndex)) == GF_NOERR) {
          LOGFINER("TCDM::selectEndpoint: Successfully selected endpoint %s",
                   m_endpoints[currIndex]->name().c_str());
          break;
        }
      }
    }
  }

// Postconditions:
// 1. If initial size of randIndex > 0 && failover was attempted,  final size of
// randIndex < initial size of randIndex
// 2. If CONN_NOERR, then m_activeEndpoint > -1, m_activeEndpoint should be
// connected.
// 3. Number of endpoints on which DM is registered <= 1
#if GF_DEVEL_ASSERTS == 1
  currentEndpoint = m_activeEndpoint;
  if ((err == GF_NOERR) &&
      (currentEndpoint < 0 || !m_endpoints[currentEndpoint]->connected())) {
    LOGWARN(
        "Current endpoint %s is not connected after failover.",
        (currentEndpoint < 0 ? "(null)"
                             : m_endpoints[currentEndpoint]->name().c_str()));
  }
#endif

  return err;
}

void ThinClientDistributionManager::postUnregisterAction() {}

bool ThinClientDistributionManager::preFailoverAction() { return true; }

bool ThinClientDistributionManager::postFailoverAction(TcrEndpoint* endpoint) {
  return true;
}

PropertiesPtr ThinClientDistributionManager::getCredentials(TcrEndpoint* ep) {
  PropertiesPtr tmpSecurityProperties =
      DistributedSystem::getSystemProperties()->getSecurityProperties();

  AuthInitializePtr authInitialize = DistributedSystem::m_impl->getAuthLoader();

  if (authInitialize != NULLPTR) {
    LOGFINER(
        "ThinClientDistributionManager::getCredentials: acquired handle to "
        "authLoader, "
        "invoking getCredentials %s",
        ep->name().c_str());
    /* adongre
     * CID 28900: Copy into fixed size buffer (STRING_OVERFLOW)
     * You might overrun the 100 byte fixed-size string "tmpEndpoint" by copying
     * the return
     * value of "stlp_std::basic_string<char, stlp_std::char_traits<char>,
     *     stlp_std::allocator<char> >::c_str() const" without checking the
     * length.
     */
    // char tmpEndpoint[100] = { '\0' } ;
    // strcpy(tmpEndpoint, ep->name().c_str());
    PropertiesPtr tmpAuthIniSecurityProperties = authInitialize->getCredentials(
        tmpSecurityProperties, /*tmpEndpoint*/ ep->name().c_str());
    return tmpAuthIniSecurityProperties;
  }
  return NULLPTR;
}

GfErrType ThinClientDistributionManager::sendUserCredentials(
    PropertiesPtr credentials, TcrEndpoint* ep) {
  LOGDEBUG("ThinClientPoolDM::sendUserCredentials");

  GfErrType err = GF_NOERR;

  TcrMessageUserCredential request(credentials, this);

  TcrMessageReply reply(true, this);

  err = ep->send(request, reply);
  LOGDEBUG(
      "ThinClientDistributionManager::sendUserCredentials: completed endpoint "
      "send for: %s [error:%d]",
      ep->name().c_str(), err);

  err = handleEPError(ep, reply, err);
  if (err == GF_IOERR) {
    err = GF_NOTCON;
  }

  if (err == GF_NOERR) {
    switch (reply.getMessageType()) {
      case TcrMessage::RESPONSE: {
        // nothing to be done;
        break;
      }
      case TcrMessage::EXCEPTION: {
        err = ThinClientRegion::handleServerException(
            "ThinClientDistributionManager::sendUserCredentials AuthException",
            reply.getException());
        break;
      }
      default: {
        LOGERROR(
            "Unknown message type %d during secure response, possible "
            "serialization mismatch",
            reply.getMessageType());
        err = GF_MSG;
        break;
      }
    }
    // throw exception if it is not authenticated
    // GfErrTypeToException("ThinClientDistributionManager::sendUserCredentials",
    // err);
  }

  return err;
}

GfErrType ThinClientDistributionManager::sendRequestToEP(
    const TcrMessage& request, TcrMessageReply& reply, TcrEndpoint* ep) {
  LOGDEBUG(
      "ThinClientDistributionManager::sendRequestToEP: invoking endpoint send "
      "for: %s",
      ep->name().c_str());
  // retry for auth n times.
  // check if auth requie excep
  // if then then sendUserCreds message, if success
  // then send back original message
  int numberOftimesAuthTried = 3;

  if (!isSecurityOn()) numberOftimesAuthTried = 1;

  GfErrType error = GF_NOERR;

  while (numberOftimesAuthTried > 0) {
    ep->setDM(this);  // we are setting here before making request...
    if (isSecurityOn() && !ep->isAuthenticated()) {
      numberOftimesAuthTried--;
      error = this->sendUserCredentials(this->getCredentials(ep), ep);
    } else {
      numberOftimesAuthTried--;
    }

    if (error != GF_NOERR) return error;

    reply.setDM(this);
    error = ep->send(request, reply);
    LOGDEBUG(
        "ThinClientDistributionManager::sendRequestToEP: completed endpoint "
        "send for: %s [error:%d]",
        ep->name().c_str(), error);
    error = handleEPError(ep, reply, error);
    if (error == GF_IOERR) {
      error = GF_NOTCON;
    }

    if (isSecurityOn() && error == GF_NOERR &&
        isAuthRequireException(reply.getException())) {
      ep->setAuthenticated(false);
      continue;
    }
    return error;
  }
  return error;
}

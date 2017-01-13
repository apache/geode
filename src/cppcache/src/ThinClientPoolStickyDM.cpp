/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientPoolStickyDM.hpp"
#include "TssConnectionWrapper.hpp"
#include <algorithm>
using namespace gemfire;
TcrConnection* ThinClientPoolStickyDM::getConnectionFromQueueW(
    GfErrType* error, std::set<ServerLocation>& excludeServers, bool isBGThread,
    TcrMessage& request, int8_t& version, bool& match, bool& connFound,
    const BucketServerLocationPtr& serverLocation) {
  TcrConnection* conn = NULL;
  TcrEndpoint* ep = NULL;
  bool maxConnLimit = false;
  if (isBGThread || request.getMessageType() == TcrMessage::GET_ALL_70 ||
      request.getMessageType() == TcrMessage::GET_ALL_WITH_CALLBACK) {
    conn = ThinClientPoolDM::getConnectionFromQueueW(
        error, excludeServers, isBGThread, request, version, match, connFound,
        serverLocation);
    return conn;
  }
  BucketServerLocationPtr slTmp = NULLPTR;
  if (m_attrs->getPRSingleHopEnabled() && !request.forTransaction()) {
    if (serverLocation != NULLPTR) {
      ep = getEndPoint(serverLocation, version, excludeServers);
    } else if (request.forSingleHop()) {
      ep = getSingleHopServer(request, version, slTmp, excludeServers);
    }
    if (ep != NULL /*&& ep->connected()*/) {
      // LOGINFO(" getSingleHopServer returns ep");
      m_manager->getSingleHopStickyConnection(ep, conn);
      if (!conn) {
        conn = getFromEP(ep);
        if (!conn) {
          *error =
              createPoolConnectionToAEndPoint(conn, ep, maxConnLimit, true);
          if (*error == GF_CLIENT_WAIT_TIMEOUT ||
              *error == GF_CLIENT_WAIT_TIMEOUT_REFRESH_PRMETADATA) {
            return NULL;
          }
        }
        if (!conn) {
          m_manager->getAnyConnection(conn);
          if (!conn) createPoolConnection(conn, excludeServers, maxConnLimit);
        }
      }
    } else if (conn == NULL) {
      // LOGINFO(" ep is null");
      m_manager->getAnyConnection(conn);
      if (!conn) {
        conn =
            getConnectionFromQueue(true, error, excludeServers, maxConnLimit);
      }
      /*if(!conn && maxConnLimit)
      {
        m_manager->getAnyConnection(conn);
      }*/
      if (!conn) {
        createPoolConnection(conn, excludeServers, maxConnLimit);
      }
    }

    if (maxConnLimit) {
      // we reach max connection limit, found connection but endpoint is
      // (not)different, no need to refresh pr-meta-data
      connFound = true;
    } else {
      // if server hints pr-meta-data refresh then refresh
      // anything else???
    }

    LOGDEBUG(
        "ThinClientPoolStickyDM::getConnectionFromQueueW return conn = %p "
        "match = %d connFound=%d",
        conn, match, connFound);
    return conn;
  }

  bool cf = m_manager->getStickyConnection(conn, error, excludeServers,
                                           request.forTransaction());

  if (request.forTransaction()) {
    TXState* txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
    if (*error == GF_NOERR && !cf && (txState == NULL || txState->isDirty())) {
      *error = doFailover(conn);
    }

    if (*error != GF_NOERR) {
      return NULL;
    }

    if (txState != NULL) {
      txState->setDirty();
    }
  }
  return conn;
}
void ThinClientPoolStickyDM::putInQueue(TcrConnection* conn, bool isBGThread,
                                        bool isTransaction) {
  if (!isBGThread) {
    if (m_attrs->getPRSingleHopEnabled() && !isTransaction) {
      m_manager->setSingleHopStickyConnection(conn->getEndpointObject(), conn);
    } else {
      m_manager->setStickyConnection(conn, isTransaction);
    }
  } else {
    ThinClientPoolDM::putInQueue(conn, isBGThread, isTransaction);
  }
}
void ThinClientPoolStickyDM::setStickyNull(bool isBGThread) {
  if (!isBGThread && !m_attrs->getPRSingleHopEnabled()) {
    m_manager->setStickyConnection(NULL, false);
  }
}

void ThinClientPoolStickyDM::cleanStickyConnections(volatile bool& isRunning) {
  if (!isRunning) {
    return;
  }
  m_manager->cleanStaleStickyConnection();
}

bool ThinClientPoolStickyDM::canItBeDeleted(TcrConnection* conn) {
  return m_manager->canThisConnBeDeleted(conn);
}
void ThinClientPoolStickyDM::releaseThreadLocalConnection() {
  m_manager->releaseThreadLocalConnection();
}
void ThinClientPoolStickyDM::setThreadLocalConnection(TcrConnection* conn) {
  m_manager->addStickyConnection(conn);
}
bool ThinClientPoolStickyDM::canItBeDeletedNoImpl(TcrConnection* conn) {
  return ThinClientPoolDM::canItBeDeleted(conn);
}

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientStickyManager.hpp"
#include "ThinClientPoolDM.hpp"
using namespace gemfire;
bool ThinClientStickyManager::getStickyConnection(
    TcrConnection*& conn, GfErrType* error,
    std::set<ServerLocation>& excludeServers, bool forTransaction) {
  bool maxConnLimit = false;
  bool connFound = false;
  // ACE_Guard<ACE_Recursive_Thread_Mutex> guard( m_stickyLock );
  conn = TssConnectionWrapper::s_gemfireTSSConn->getConnection();

  if (!conn) {
    conn =
        m_dm->getConnectionFromQueue(true, error, excludeServers, maxConnLimit);
    if (conn) {
      conn->setAndGetBeingUsed(true, forTransaction);
    }
  } else {
    if (!conn->setAndGetBeingUsed(
            true, forTransaction)) {  // manage connection thread is changing
                                      // the connectiion
      conn = m_dm->getConnectionFromQueue(true, error, excludeServers,
                                          maxConnLimit);
      if (conn) {
        connFound = true;
        conn->setAndGetBeingUsed(true, forTransaction);
      }
    } else {
      connFound = true;
    }
  }
  return connFound;
}

void ThinClientStickyManager::getSingleHopStickyConnection(
    TcrEndpoint* theEP, TcrConnection*& conn) {
  conn = TssConnectionWrapper::s_gemfireTSSConn->getSHConnection(
      theEP, m_dm->getName());
}

void ThinClientStickyManager::addStickyConnection(TcrConnection* conn) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
  TcrConnection* oldConn =
      TssConnectionWrapper::s_gemfireTSSConn->getConnection();
  if (oldConn) {
    std::set<TcrConnection**>::iterator it = m_stickyConnList.find(
        TssConnectionWrapper::s_gemfireTSSConn->getConnDoublePtr());
    if (it != m_stickyConnList.end()) {
      oldConn->setAndGetBeingUsed(false, false);
      m_stickyConnList.erase(it);
      PoolPtr p = NULLPTR;
      TssConnectionWrapper::s_gemfireTSSConn->setConnection(NULL, p);
      m_dm->put(oldConn, false);
    }
  }

  if (conn) {
    PoolPtr p(m_dm);
    TssConnectionWrapper::s_gemfireTSSConn->setConnection(conn, p);
    conn->setAndGetBeingUsed(true, true);  // this is done for transaction
                                           // thread when some one resume
                                           // transaction
    m_stickyConnList.insert(
        TssConnectionWrapper::s_gemfireTSSConn->getConnDoublePtr());
  }
}

void ThinClientStickyManager::setStickyConnection(TcrConnection* conn,
                                                  bool forTransaction) {
  // ACE_Guard<ACE_Recursive_Thread_Mutex> guard( m_stickyLock );
  if (!conn) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
    PoolPtr p = NULLPTR;
    TssConnectionWrapper::s_gemfireTSSConn->setConnection(NULL, p);
  } else {
    TcrConnection* currentConn =
        TssConnectionWrapper::s_gemfireTSSConn->getConnection();
    if (currentConn != conn)  // otherwsie no need to set it again
    {
      ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
      PoolPtr p(m_dm);
      TssConnectionWrapper::s_gemfireTSSConn->setConnection(conn, p);
      conn->setAndGetBeingUsed(
          false,
          forTransaction);  // if transaction then it will keep this as used
      m_stickyConnList.insert(
          TssConnectionWrapper::s_gemfireTSSConn->getConnDoublePtr());
    } else {
      currentConn->setAndGetBeingUsed(
          false,
          forTransaction);  // if transaction then it will keep this as used
    }
  }
}

void ThinClientStickyManager::setSingleHopStickyConnection(
    TcrEndpoint* ep, TcrConnection*& conn) {
  TssConnectionWrapper::s_gemfireTSSConn->setSHConnection(ep, conn);
}

void ThinClientStickyManager::cleanStaleStickyConnection() {
  LOGDEBUG("Cleaning sticky connections");
  std::set<ServerLocation> excludeServers;
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
  std::find_if(m_stickyConnList.begin(), m_stickyConnList.end(),
               ThinClientStickyManager::isNULL);
  while (1) {
    std::set<TcrConnection**>::iterator it =
        std::find_if(m_stickyConnList.begin(), m_stickyConnList.end(),
                     ThinClientStickyManager::isNULL);
    if (it == m_stickyConnList.end()) break;
    m_stickyConnList.erase(it);
  }
  bool maxConnLimit = false;
  for (std::set<TcrConnection**>::iterator it = m_stickyConnList.begin();
       it != m_stickyConnList.end(); it++) {
    TcrConnection** conn = (*it);
    if ((*conn)->setAndGetBeingUsed(true, false) &&
        canThisConnBeDeleted(*conn)) {
      GfErrType err = GF_NOERR;
      TcrConnection* temp = m_dm->getConnectionFromQueue(
          false, &err, excludeServers, maxConnLimit);
      if (temp) {
        TcrConnection* temp1 = *conn;
        //*conn = temp; instead of setting in thread local put in queue, thread
        // will come and pick it from there
        *conn = NULL;
        m_dm->put(temp, false);
        temp1->close();
        GF_SAFE_DELETE(temp1);
        m_dm->removeEPConnections(1, false);
        LOGDEBUG("Replaced a sticky connection");
      } else {
        (*conn)->setAndGetBeingUsed(false, false);
      }
      temp = NULL;
    }
  }
}

void ThinClientStickyManager::closeAllStickyConnections() {
  LOGDEBUG("ThinClientStickyManager::closeAllStickyConnections()");
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
  for (std::set<TcrConnection**>::iterator it = m_stickyConnList.begin();
       it != m_stickyConnList.end(); it++) {
    TcrConnection** tempConn = *it;
    if (*tempConn) {
      (*tempConn)->close();
      GF_SAFE_DELETE(*tempConn);
      m_dm->removeEPConnections(1, false);
    }
  }
}
bool ThinClientStickyManager::canThisConnBeDeleted(TcrConnection* conn) {
  bool canBeDeleted = false;
  LOGDEBUG("ThinClientStickyManager::canThisConnBeDeleted()");
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
  if (m_dm->canItBeDeletedNoImpl(conn)) return true;
  TcrEndpoint* endPt = conn->getEndpointObject();
  ACE_Guard<ACE_Recursive_Thread_Mutex> guardQueue(
      endPt->getQueueHostedMutex());
  if (endPt->isQueueHosted()) {
    for (std::set<TcrConnection**>::iterator it = m_stickyConnList.begin();
         it != m_stickyConnList.end(); it++) {
      TcrConnection* connTemp2 = *(*it);
      if (connTemp2 && connTemp2->getEndpointObject() == endPt) {
        canBeDeleted = true;
        break;
      }
    }
  }
  return canBeDeleted;
}
void ThinClientStickyManager::releaseThreadLocalConnection() {
  TcrConnection* conn = TssConnectionWrapper::s_gemfireTSSConn->getConnection();
  if (conn) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(m_stickyLock);
    std::set<TcrConnection**>::iterator it = m_stickyConnList.find(
        TssConnectionWrapper::s_gemfireTSSConn->getConnDoublePtr());
    LOGDEBUG("ThinClientStickyManager::releaseThreadLocalConnection()");
    if (it != m_stickyConnList.end()) {
      m_stickyConnList.erase(it);
      conn->setAndGetBeingUsed(false,
                               false);  // now this can be used by next one
      m_dm->put(conn, false);
    }
    PoolPtr p(m_dm);
    TssConnectionWrapper::s_gemfireTSSConn->setConnection(NULL, p);
  }
  PoolPtr p(m_dm);
  TssConnectionWrapper::s_gemfireTSSConn->releaseSHConnections(p);
}
bool ThinClientStickyManager::isNULL(TcrConnection** conn) {
  if (*conn == NULL) return true;
  return false;
}

void ThinClientStickyManager::getAnyConnection(TcrConnection*& conn) {
  conn =
      TssConnectionWrapper::s_gemfireTSSConn->getAnyConnection(m_dm->getName());
}

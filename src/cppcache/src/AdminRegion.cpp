/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "AdminRegion.hpp"
#include "CacheImpl.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "ThinClientRegion.hpp"
#include <statistics/StatisticsManager.hpp>
#include "ThinClientPoolDM.hpp"
using namespace gemfire;
AdminRegion::AdminRegion(CacheImpl* cache, ThinClientBaseDM* distMan)
    : m_distMngr((ThinClientBaseDM*)0)
      /* adongre
       * CID 28925: Uninitialized pointer field (UNINIT_CTOR)
       */
      ,
      m_connectionMgr((TcrConnectionManager*)0),
      m_destroyPending(false) {
  m_fullPath = "/__ADMIN_CLIENT_HEALTH_MONITORING__";

  // Only if Stats are enabled

  SystemProperties* props =
      cache->getCache()->getDistributedSystem()->getSystemProperties();
  if (props && props->statisticsEnabled()) {
    // no need to create a region .. just create a cacheDistribution Manager
    m_connectionMgr = &(cache->tcrConnectionManager());
    if (!distMan) {
      m_distMngr = new ThinClientCacheDistributionManager(*m_connectionMgr);
      StatisticsManager* mngr = StatisticsManager::getExistingInstance();
      if (mngr) {
        // Register it with StatisticsManager
        mngr->RegisterAdminRegion(AdminRegionPtr(this));
      }
    } else {
      m_distMngr = distMan;
    }
  }
}

void AdminRegion::init() {
  /*TryWriteGuard _guard(m_rwLock, m_destroyPending);
  if (m_destroyPending) {
    return;
  }
  */
  // Init distribution manager if it is not a pool
  ThinClientPoolDM* pool = dynamic_cast<ThinClientPoolDM*>(m_distMngr);
  if (pool == NULL) {
    m_distMngr->init();
  }
}

TcrConnectionManager* AdminRegion::getConnectionManager() {
  return m_connectionMgr;
}

void AdminRegion::put(const CacheableKeyPtr& keyPtr,
                      const CacheablePtr& valuePtr) {
  GfErrType err = putNoThrow(keyPtr, valuePtr);
  GfErrTypeToException("AdminRegion::put", err);
}

GfErrType AdminRegion::putNoThrow(const CacheableKeyPtr& keyPtr,
                                  const CacheablePtr& valuePtr) {
  // put obj to region
  GfErrType err = GF_NOERR;

  TcrMessagePut request(NULL, keyPtr, valuePtr, NULLPTR, false, m_distMngr,
                        true, false, m_fullPath.c_str());
  request.setMetaRegion(true);
  TcrMessageReply reply(true, m_distMngr);
  reply.setMetaRegion(true);
  err = m_distMngr->sendSyncRequest(request, reply, true, true);
  if (err != GF_NOERR) {
    return err;
  }

  // put the object into local region
  switch (reply.getMessageType()) {
    case TcrMessage::REPLY: {
      LOGDEBUG(
          "AdminRegion::put: entry is written into remote server "
          "at region %s",
          m_fullPath.c_str());
      break;
    }
    case TcrMessage::EXCEPTION: {
      const char* exceptionMsg = reply.getException();
      err = ThinClientRegion::handleServerException("AdminRegion::put",
                                                    exceptionMsg);
      break;
    }
    case TcrMessage::PUT_DATA_ERROR: {
      LOGERROR("A write error occurred on the endpoint %s",
               m_distMngr->getActiveEndpoint()->name().c_str());
      err = GF_CACHESERVER_EXCEPTION;
      break;
    }
    default: {
      LOGERROR("Unknown message type in put reply %d", reply.getMessageType());
      err = GF_MSG;
    }
  }
  return err;
}

void AdminRegion::close() {
  TryWriteGuard _guard(m_rwLock, m_destroyPending);
  if (m_destroyPending) {
    return;
  }
  m_destroyPending = true;

  // Close distribution manager if it is not a pool
  ThinClientPoolDM* pool = dynamic_cast<ThinClientPoolDM*>(m_distMngr);
  if (pool == NULL) {
    m_distMngr->destroy();
    GF_SAFE_DELETE(m_distMngr);
  }
}

AdminRegion::~AdminRegion() {
  // destructor should be single threaded in any case, so no need of guard
  if (m_distMngr != NULL) {
    close();
  }
}

const bool& AdminRegion::isDestroyed() { return m_destroyPending; }
ACE_RW_Thread_Mutex& AdminRegion::getRWLock() { return m_rwLock; }

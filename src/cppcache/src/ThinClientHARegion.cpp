/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "ThinClientHARegion.hpp"
#include "TcrHADistributionManager.hpp"
#include "CacheImpl.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "ReadWriteLock.hpp"
#include <gfcpp/PoolManager.hpp>
#include "ThinClientPoolHADM.hpp"
namespace gemfire {

ThinClientHARegion::ThinClientHARegion(const std::string& name,
                                       CacheImpl* cache, RegionInternal* rPtr,
                                       const RegionAttributesPtr& attributes,
                                       const CacheStatisticsPtr& stats,
                                       bool shared, bool enableNotification)
    : ThinClientRegion(name, cache, rPtr, attributes, stats, shared),
      m_attribute(attributes),
      m_processedMarker(false),
      m_poolDM(false) {
  setClientNotificationEnabled(enableNotification);
}

void ThinClientHARegion::initTCR() {
  try {
    bool isPool = m_attribute->getPoolName() != NULL &&
                  strlen(m_attribute->getPoolName()) > 0;
    if (DistributedSystem::getSystemProperties()->isGridClient()) {
      LOGWARN(
          "Region: HA region having notification channel created for grid "
          "client; force starting required notification, cleanup and "
          "failover threads");
      m_cacheImpl->tcrConnectionManager().startFailoverAndCleanupThreads(
          isPool);
    }

    if (m_attribute->getPoolName() == NULL ||
        strlen(m_attribute->getPoolName()) == 0) {
      m_poolDM = false;
      m_tcrdm = new TcrHADistributionManager(
          this, m_cacheImpl->tcrConnectionManager(),
          m_cacheImpl->getAttributes());
      m_tcrdm->init();
    } else {
      m_tcrdm = dynamic_cast<ThinClientPoolHADM*>(
          PoolManager::find(m_attribute->getPoolName()).ptr());
      if (m_tcrdm) {
        m_poolDM = true;
        // Pool DM should only be inited once and it
        // is already done in PoolFactory::create();
        // m_tcrdm->init();
        ThinClientPoolHADM* poolDM = dynamic_cast<ThinClientPoolHADM*>(m_tcrdm);
        poolDM->addRegion(this);
        poolDM->incRegionCount();

      } else {
        throw IllegalStateException("pool not found");
      }
    }
  } catch (const Exception& ex) {
    GF_SAFE_DELETE(m_tcrdm);
    LOGERROR(
        "ThinClientHARegion: failed to create a DistributionManager "
        "object due to: %s: %s",
        ex.getName(), ex.getMessage());
    throw;
  }
}

void ThinClientHARegion::acquireGlobals(bool isFailover) {
  if (isFailover) {
    ThinClientRegion::acquireGlobals(isFailover);
  } else {
    m_tcrdm->acquireRedundancyLock();
  }
}

void ThinClientHARegion::releaseGlobals(bool isFailover) {
  if (isFailover) {
    ThinClientRegion::releaseGlobals(isFailover);
  } else {
    m_tcrdm->releaseRedundancyLock();
  }
}

void ThinClientHARegion::handleMarker() {
  TryReadGuard guard(m_rwLock, m_destroyPending);
  if (m_destroyPending) {
    return;
  }

  if (m_listener != NULLPTR && !m_processedMarker) {
    RegionEvent event(RegionPtr(this), NULLPTR, false);
    int64 sampleStartNanos = Utils::startStatOpTime();
    try {
      m_listener->afterRegionLive(event);
    } catch (const Exception& ex) {
      LOGERROR("Exception in CacheListener::afterRegionLive: %s: %s",
               ex.getName(), ex.getMessage());
    } catch (...) {
      LOGERROR("Unknown exception in CacheListener::afterRegionLive");
    }
    m_cacheImpl->m_cacheStats->incListenerCalls();
    Utils::updateStatOpTime(
        m_regionStats->getStat(),
        RegionStatType::getInstance()->getListenerCallTimeId(),
        sampleStartNanos);
    m_regionStats->incListenerCallsCompleted();
  }
  m_processedMarker = true;
}

bool ThinClientHARegion::getProcessedMarker() {
  return m_processedMarker || !isDurableClient();
}

void ThinClientHARegion::destroyDM(bool keepEndpoints) {
  if (m_poolDM) {
    LOGDEBUG(
        "ThinClientHARegion::destroyDM( ): removing region from "
        "ThinClientPoolHADM list.");
    ThinClientPoolHADM* poolDM = dynamic_cast<ThinClientPoolHADM*>(m_tcrdm);
    poolDM->removeRegion(this);
    poolDM->decRegionCount();
  } else {
    ThinClientRegion::destroyDM(keepEndpoints);
  }
}
}  // namespace gemfire

void ThinClientHARegion::addDisMessToQueue() {
  if (m_poolDM) {
    ThinClientPoolHADM* poolDM = dynamic_cast<ThinClientPoolHADM*>(m_tcrdm);
    poolDM->addDisMessToQueue(this);

    if (poolDM->m_redundancyManager->m_globalProcessedMarker &&
        !m_processedMarker) {
      TcrMessage* regionMsg = new TcrMessageClientMarker(true);
      receiveNotification(regionMsg);
    }
  }
}

GfErrType ThinClientHARegion::getNoThrow_FullObject(EventIdPtr eventId,
                                                    CacheablePtr& fullObject,
                                                    VersionTagPtr& versionTag) {
  TcrMessageRequestEventValue fullObjectMsg(eventId);
  TcrMessageReply reply(true, NULL);

  ThinClientPoolHADM* poolHADM = dynamic_cast<ThinClientPoolHADM*>(m_tcrdm);
  GfErrType err = GF_NOTCON;
  if (poolHADM) {
    err = poolHADM->sendRequestToPrimary(fullObjectMsg, reply);
  } else {
    err = static_cast<TcrHADistributionManager*>(m_tcrdm)->sendRequestToPrimary(
        fullObjectMsg, reply);
  }
  if (err == GF_NOERR) {
    fullObject = reply.getValue();
  }
  versionTag = reply.getVersionTag();
  return err;
}

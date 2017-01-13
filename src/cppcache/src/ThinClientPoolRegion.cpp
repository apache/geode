/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientPoolRegion.cpp
 *
 *  Created on: Nov 20, 2008
 *      Author: abhaware
 */

#include "ThinClientPoolRegion.hpp"
#include "CacheImpl.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "ThinClientPoolDM.hpp"
#include <gfcpp/PoolManager.hpp>

using namespace gemfire;

ThinClientPoolRegion::ThinClientPoolRegion(
    const std::string& name, CacheImpl* cache, RegionInternal* rPtr,
    const RegionAttributesPtr& attributes, const CacheStatisticsPtr& stats,
    bool shared)
    : ThinClientRegion(name, cache, rPtr, attributes, stats, shared) {}

ThinClientPoolRegion::~ThinClientPoolRegion() { m_tcrdm = NULL; }

void ThinClientPoolRegion::initTCR() {
  try {
    ThinClientPoolDM* poolDM = dynamic_cast<ThinClientPoolDM*>(
        PoolManager::find(m_regionAttributes->getPoolName()).ptr());
    m_tcrdm = dynamic_cast<ThinClientBaseDM*>(poolDM);
    if (!m_tcrdm) {
      //  TODO: create a PoolNotFound exception.
      throw IllegalStateException("pool not found");
    }
    poolDM->incRegionCount();
  } catch (const Exception& ex) {
    LOGERROR("Failed to initialize region due to %s: %s", ex.getName(),
             ex.getMessage());
    throw;
  }
}

void ThinClientPoolRegion::destroyDM(bool keepEndpoints) {
  dynamic_cast<ThinClientPoolDM*>(m_tcrdm)->decRegionCount();
}

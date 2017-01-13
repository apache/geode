/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientSecurity.hpp
 *
 *  Created on: Nov 13, 2008
 *      Author: vrao
 */

#ifndef THINCLIENTSECURITY_HPP_
#define THINCLIENTSECURITY_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 1;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

void setCacheListener(const char* regName, const CacheListenerPtr& listener) {
  if (listener != NULLPTR) {
    RegionPtr reg = getHelper()->getRegion(regName);
    AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
    attrMutator->setCacheListener(listener);
  }
}

void createRegionForSecurity(const char* name, bool ackMode,
                             bool clientNotificationEnabled = false,
                             const CacheListenerPtr& listener = NULLPTR,
                             bool caching = true, int connections = -1,
                             bool isMultiuserMode = false,
                             int subscriptionRedundancy = -1) {
  char msg[128] = {'\0'};
  LOG(msg);
  LOG(" pool is creating");
  char buff[128] = {'\0'};
  const char* poolName = name;

  if (PoolManager::find(name) != NULLPTR) {
    static unsigned int index = 0;
    sprintf(buff, "%s_%d", poolName, index++);
    poolName = buff;
  }
  printf("createRegionForSecurity poolname = %s \n", poolName);
  getHelper()->createPoolWithLocators(
      poolName, locatorsG, clientNotificationEnabled,
      subscriptionRedundancy, -1, connections, isMultiuserMode);
  createRegionAndAttachPool(name, ackMode, poolName, caching);
  setCacheListener(name, listener);
}

PoolPtr getPool(const char* name) { return PoolManager::find(name); }

RegionServicePtr getVirtualCache(PropertiesPtr creds, PoolPtr pool) {
  CachePtr cachePtr = getHelper()->getCache();
  // return pool->createSecureUserCache(creds);
  return cachePtr->createAuthenticatedView(creds, pool->getName());
}


#endif /* THINCLIENTSECURITY_HPP_ */

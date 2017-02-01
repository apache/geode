#pragma once

#ifndef GEODE_INTEGRATION_TEST_THINCLIENTSECURITY_H_
#define GEODE_INTEGRATION_TEST_THINCLIENTSECURITY_H_

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
/*
 * ThinClientSecurity.hpp
 *
 *  Created on: Nov 13, 2008
 *      Author: vrao
 */


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
      poolName, locatorsG, clientNotificationEnabled, subscriptionRedundancy,
      -1, connections, isMultiuserMode);
  createRegionAndAttachPool(name, ackMode, poolName, caching);
  setCacheListener(name, listener);
}

PoolPtr getPool(const char* name) { return PoolManager::find(name); }

RegionServicePtr getVirtualCache(PropertiesPtr creds, PoolPtr pool) {
  CachePtr cachePtr = getHelper()->getCache();
  // return pool->createSecureUserCache(creds);
  return cachePtr->createAuthenticatedView(creds, pool->getName());
}


#endif // GEODE_INTEGRATION_TEST_THINCLIENTSECURITY_H_

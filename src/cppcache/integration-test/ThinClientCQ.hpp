#pragma once

#ifndef GEODE_INTEGRATION_TEST_THINCLIENTCQ_H_
#define GEODE_INTEGRATION_TEST_THINCLIENTCQ_H_

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
 * ThinClientCQ.hpp
 *
 *  Created on: Nov 14, 2008
 *      Author: vrao
 */


#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

static bool isLocator = false;
static bool isLocalServer = false;
static int numberOfLocators = 1;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

void createRegionForCQ(const char* name, bool ackMode,
                       bool clientNotificationEnabled = false,
                       int redundancyLevel = 0,
                       const CacheListenerPtr& listener = NULLPTR,
                       bool caching = true) {
  // Use region name as pool name to avoid recreating pools with the same name.
  getHelper()->createPoolWithLocators(
      name, locatorsG, clientNotificationEnabled, redundancyLevel, 1);
  createRegionAndAttachPool(name, ackMode, name, caching);
}

void createRegionForCQMU(const char* name, bool ackMode,
                         bool clientNotificationEnabled = false,
                         int redundancyLevel = 0,
                         const CacheListenerPtr& listener = NULLPTR,
                         bool caching = true,
                         bool poolIsInMultiuserMode = false) {
  // Use region name as pool name to avoid recreating pools with the same name.
  // TODO:
  // getHelper()->createPoolWithLocators( name, locatorsG,
  // clientNotificationEnabled, redundancyLevel, 1 );
  createRegionAndAttachPool(name, ackMode, name, caching);
}


#endif // GEODE_INTEGRATION_TEST_THINCLIENTCQ_H_

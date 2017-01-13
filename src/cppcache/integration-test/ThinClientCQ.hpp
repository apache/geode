/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientCQ.hpp
 *
 *  Created on: Nov 14, 2008
 *      Author: vrao
 */

#ifndef _THINCLIENTCQ_HPP_
#define _THINCLIENTCQ_HPP_

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

#endif /* _THINCLIENTCQ_HPP_ */

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
static int numberOfLocators = 1;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, numberOfLocators);

void createRegionForCQ( bool pool, bool locator, const char * name, bool ackMode, const char * endpoints,
    bool clientNotificationEnabled = false, int redundancyLevel = 0, const CacheListenerPtr& listener = NULLPTR, bool caching = true)
{
  //Use region name as pool name to avoid recreating pools with the same name.
  if (pool) {
    if (locator) {
      getHelper()->createPoolWithLocators( name, locatorsG, clientNotificationEnabled, redundancyLevel, 1 );
    }
    else {
      getHelper()->createPoolWithEPs( name, endpoints, clientNotificationEnabled, redundancyLevel, 1 );
    }
    createRegionAndAttachPool(name, ackMode, name, caching);
  }
  else {
    createRegion(name, ackMode, endpoints, clientNotificationEnabled, listener, caching);
  }
}

void createRegionForCQMU( bool pool, bool locator, const char * name, bool ackMode, const char * endpoints,
    bool clientNotificationEnabled = false, int redundancyLevel = 0, const CacheListenerPtr& listener = NULLPTR, bool caching = true, bool poolIsInMultiuserMode = false)
{
  //Use region name as pool name to avoid recreating pools with the same name.
  if (pool) {
    if (locator) {
      //TODO:hitesh
     // getHelper()->createPoolWithLocators( name, locatorsG, clientNotificationEnabled, redundancyLevel, 1 );
    }
    else {
      //void createPoolWithEPs( const char* name, const char* endpoints = NULL, bool clientNotificationEnabled = false,
      //int subscriptionRedundancy = -1, int subscriptionAckInterval = -1, int connections = -1, bool isMultiuserMode = false )
      getHelper()->createPoolWithEPs( name, endpoints, clientNotificationEnabled, redundancyLevel, 1 , -1, poolIsInMultiuserMode);
    }
    createRegionAndAttachPool(name, ackMode, name, caching);
  }
  else {
    createRegion(name, ackMode, endpoints, clientNotificationEnabled, listener, caching);
  }
}

#endif /* _THINCLIENTCQ_HPP_ */

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

static bool isLocator = false;
static int numberOfLocators = 1;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, numberOfLocators);

volatile bool g_poolConfig = false;
volatile bool g_poolLocators = false;

const char* g_Locators;

void setCacheListener( const char *regName, const CacheListenerPtr& listener )
{
  if ( listener != NULLPTR ) {
    RegionPtr reg = getHelper()->getRegion(regName);
    AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
    attrMutator->setCacheListener( listener );
  }
}

void createRegionForSecurity( const char * name, bool ackMode, const char * endpoints,
    bool clientNotificationEnabled = false, const CacheListenerPtr& listener = NULLPTR, bool caching = true, int connections = -1, bool isMultiuserMode = false, int subscriptionRedundancy = -1)
{
  char msg[128] = {'\0'};
  sprintf(msg,"createRegionForSecurity: g_poolConfig = %s, g_poolLocators = %s subscriptionRedundancy=%d",
          g_poolConfig ? "true" : "false" , g_poolLocators ? "true": "false", subscriptionRedundancy);
  LOG(msg);
  if (g_poolConfig) {
    LOG("HItesh pool is creating");
    char buff[128] = {'\0'};
    const char *poolName = name;

    if(PoolManager::find(name) != NULLPTR) {
     static unsigned int index = 0;
     sprintf(buff,"%s_%d",poolName,index++);
     poolName = buff;
    }
    printf("createRegionForSecurity poolname = %s \n", poolName);
    if (g_poolLocators) {
      getHelper()->createPoolWithLocators( poolName, locatorsG, clientNotificationEnabled, subscriptionRedundancy, -1, connections, isMultiuserMode);
    }
    else {
      getHelper()->createPoolWithEPs(poolName, endpoints, clientNotificationEnabled, subscriptionRedundancy, -1, connections, isMultiuserMode);
    }
    createRegionAndAttachPool(name, ackMode, poolName, caching);
    setCacheListener( name, listener );
  }
  else {
    createRegion(name, ackMode, endpoints, clientNotificationEnabled, listener, caching);
  }
}

PoolPtr getPool(const char * name)
{
  return PoolManager::find(name);
}

RegionServicePtr getVirtualCache(PropertiesPtr creds, PoolPtr pool)
{
  CachePtr cachePtr = getHelper()->getCache();
  //return pool->createSecureUserCache(creds);
  return cachePtr->createAuthenticatedView(creds, pool->getName());
}

void setGlobals(int testCase)
{
  switch (testCase){
    case 1: {      // normal case: pool == false, locators == false
      g_poolConfig = false;
      g_poolLocators = false;
      break;
    }
    case 2: {      // pool-with-endpoints case: pool == true, locators == false
      g_poolConfig = true;
      g_poolLocators = false;
      break;
    }
    case 3: {      // pool-with-locator case: pool == true, locators == true
      g_poolConfig = true;
      g_poolLocators = true;
      break;
    }
    default: {
      FAIL("Wrong test Case");
      break;
    }
  }
}
void setGlobals(bool poolConfig = false, bool poolLocators = false)
{
  g_poolConfig = poolConfig;
  g_poolLocators = poolLocators;
}

#endif /* THINCLIENTSECURITY_HPP_ */

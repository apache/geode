/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurableInit.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLEINIT_HPP_
#define THINCLIENTDURABLEINIT_HPP_

bool isLocalServer = false;

const char* durableIds[] = {"DurableId1", "DurableId2"};

static bool isLocator = false;
static int numberOfLocators = 1;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

void initClientAndRegion(int redundancy, int ClientIdx,
                         int subscriptionAckInterval = 1,
                         int redundancyMonitorInterval = -1,
                         int durableClientTimeout = 60) {
  PropertiesPtr pp = Properties::create();
  if (ClientIdx < 2) {
    pp->insert("durable-client-id", durableIds[ClientIdx]);
    pp->insert("durable-timeout", durableClientTimeout);
    if (redundancyMonitorInterval > 0) {
      pp->insert("redundancy-monitor-interval", redundancyMonitorInterval);
    }

    initClient(true, pp);
    getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true,
                                        redundancy, subscriptionAckInterval);
    createRegionAndAttachPool(regionNames[0], USE_ACK, "__TESTPOOL1_", true);
  }
}
void initClientAndTwoRegions(int ClientIdx, int redundancy,
                             int durableClientTimeout,
                             const char* conflation = NULL,
                             const char* rNames[] = regionNames) {
  PropertiesPtr pp = Properties::create();
  pp->insert("durable-client-id", durableIds[ClientIdx]);
  pp->insert("durable-timeout", durableClientTimeout);
  if (conflation) {
    pp->insert("conflate-events", conflation);
  }

  initClient(true, pp);
  getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true,
                                      redundancy, 1);
  createRegionAndAttachPool(rNames[0], USE_ACK, "__TESTPOOL1_", true);
  createRegionAndAttachPool(rNames[1], USE_ACK, "__TESTPOOL1_", true);
}
void initClientAndTwoRegionsAndTwoPools(int ClientIdx, int redundancy,
                                        int durableClientTimeout,
                                        const char* conflation = NULL,
                                        const char* rNames[] = regionNames) {
  PropertiesPtr pp = Properties::create();
  pp->insert("durable-client-id", durableIds[ClientIdx]);
  pp->insert("durable-timeout", durableClientTimeout);
  if (conflation) {
    pp->insert("conflate-events", conflation);
  }

  initClient(true, pp);
  getHelper()->createPoolWithLocators("__TESTPOOL2_", locatorsG, true,
                                      redundancy, 1);
  createRegionAndAttachPool(rNames[1], USE_ACK, "__TESTPOOL2_", true);
  // Calling readyForEvents() here instead of below causes duplicate durableId
  // exception reproduced.
  /*LOG( "Calling readyForEvents:");
  try {
    getHelper()->cachePtr->readyForEvents();
  }catch(...) {
    LOG("Exception occured while sending readyForEvents");
  }*/

  RegionPtr regPtr1 = getHelper()->getRegion(rNames[1]);
  regPtr1->registerAllKeys(true);
  getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true,
                                      redundancy, 1);
  createRegionAndAttachPool(rNames[0], USE_ACK, "__TESTPOOL1_", true);
  RegionPtr regPtr0 = getHelper()->getRegion(rNames[0]);
  regPtr0->registerAllKeys(true);

  LOG("Calling readyForEvents:");
  try {
    getHelper()->cachePtr->readyForEvents();
  } catch (...) {
    LOG("Exception occured while sending readyForEvents");
  }
}
#endif /* THINCLIENTDURABLEINIT_HPP_ */

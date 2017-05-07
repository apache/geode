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
const char * endPoint = CacheHelper::getTcrEndpoints( isLocalServer, 1 );
const char * endPoints = CacheHelper::getTcrEndpoints( isLocalServer, 2 );
const char *durableIds[] = { "DurableId1" , "DurableId2" };

static bool isLocator = false;
static int numberOfLocators = 1;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, numberOfLocators);
volatile bool g_poolConfig = false;
volatile bool g_poolLocators = false;

void initClientAndRegion( int redundancy, int ClientIdx, const char* endpointsList = endPoint, int subscriptionAckInterval = 1,
                          int redundancyMonitorInterval = -1, int durableClientTimeout = 60 )
{
  PropertiesPtr pp  = Properties::create();
  if( ClientIdx <2 ){
      pp->insert("durable-client-id", durableIds[ClientIdx]);
      pp->insert( "durable-timeout", durableClientTimeout );
    if ( redundancyMonitorInterval > 0 ) {
      pp->insert( "redundancy-monitor-interval", redundancyMonitorInterval );
    }

    if ( g_poolConfig ) {
      initClient(true, pp);
      if (g_poolLocators) {
        getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true, redundancy, subscriptionAckInterval);
      }
      else {
        getHelper()->createPoolWithEPs("__TESTPOOL1_", endpointsList, true, redundancy, subscriptionAckInterval);
      }
      createRegionAndAttachPool(regionNames[0], USE_ACK, "__TESTPOOL1_", true);
    }
    else {
      pp->insert( "notify-ack-interval", subscriptionAckInterval );
      initClient( endpointsList, redundancy, pp );
      createRegion( regionNames[0], USE_ACK, endpointsList, true );
    }
  }
}
void initClientAndTwoRegions( int ClientIdx, int redundancy, int durableClientTimeout, const char* conflation = NULL, const char* rNames[ ] = regionNames )
{
  PropertiesPtr pp  = Properties::create();
  pp->insert( "durable-client-id", durableIds[ ClientIdx ] );
  pp->insert( "durable-timeout", durableClientTimeout );
  if( conflation ) {
    pp->insert( "conflate-events", conflation );
  }

  const char *ep = ( redundancy == 0 ? endPoint : endPoints );

  if ( g_poolConfig ) {
    initClient( true, pp );
    if ( g_poolLocators ) {
      getHelper()->createPoolWithLocators( "__TESTPOOL1_", locatorsG, true, redundancy, 1 );
    }
    else {
      LOG( "Setting endpoints to:");
      LOG( ep );
      getHelper()->createPoolWithEPs( "__TESTPOOL1_", ep, true, redundancy, 1 );
    }
    createRegionAndAttachPool( rNames[ 0 ], USE_ACK, "__TESTPOOL1_", true );
    createRegionAndAttachPool( rNames[ 1 ], USE_ACK, "__TESTPOOL1_", true );
  }
  else {
    LOG( "Setting endpoints to:");
    LOG( ep );
    pp->insert( "notify-ack-interval", 1 );
    initClient( ep, redundancy, pp );
    createRegion( rNames[ 0 ], USE_ACK, endPoint, true );
    createRegion( rNames[ 1 ], USE_ACK, endPoint, true );
  }
}
void initClientAndTwoRegionsAndTwoPools( int ClientIdx, int redundancy, int durableClientTimeout, const char* conflation = NULL, const char* rNames[ ] = regionNames )
{
  PropertiesPtr pp  = Properties::create();
  pp->insert( "durable-client-id", durableIds[ ClientIdx ] );
  pp->insert( "durable-timeout", durableClientTimeout );
  if( conflation ) {
    pp->insert( "conflate-events", conflation );
  }

  const char *ep = ( redundancy == 0 ? endPoint : endPoints );

  if ( g_poolConfig ) {
    initClient( true, pp );
    if ( g_poolLocators ) {      
      getHelper()->createPoolWithLocators( "__TESTPOOL2_", locatorsG, true, redundancy, 1 );
    }
    else {
      LOG( "Setting endpoints to:");
      LOG( ep );      
      getHelper()->createPoolWithEPs( "__TESTPOOL2_", ep, true, redundancy, 1 );
    }
    createRegionAndAttachPool( rNames[ 1 ], USE_ACK, "__TESTPOOL2_", true );
    //Calling readyForEvents() here instead of below causes duplicate durableId exception reproduced.
    /*LOG( "Calling readyForEvents:");
    try {
      getHelper()->cachePtr->readyForEvents();
    }catch(...) {
      LOG("Exception occured while sending readyForEvents");
    }*/

    RegionPtr regPtr1 = getHelper()->getRegion( rNames[1] );
    regPtr1->registerAllKeys(true);
    if ( g_poolLocators ) {
      getHelper()->createPoolWithLocators( "__TESTPOOL1_", locatorsG, true, redundancy, 1 );
    }
    else {
      getHelper()->createPoolWithEPs( "__TESTPOOL1_", ep, true, redundancy, 1 );
    }
    createRegionAndAttachPool( rNames[ 0 ], USE_ACK, "__TESTPOOL1_", true );
    RegionPtr regPtr0 = getHelper()->getRegion( rNames[0] );
    regPtr0->registerAllKeys(true);

    LOG( "Calling readyForEvents:");
    try {
      getHelper()->cachePtr->readyForEvents();
    }catch(...) {
      LOG("Exception occured while sending readyForEvents");
    }
  }
  else {
    LOG( "Setting endpoints to:");
    LOG( ep );
    pp->insert( "notify-ack-interval", 1 );
    initClient( ep, redundancy, pp );
    createRegion( rNames[ 0 ], USE_ACK, endPoint, true );
    createRegion( rNames[ 1 ], USE_ACK, endPoint, true );
  }
}
#endif /* THINCLIENTDURABLEINIT_HPP_ */

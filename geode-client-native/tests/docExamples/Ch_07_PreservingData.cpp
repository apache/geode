/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_07_PreservingData.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
PreservingData::PreservingData()
{
}

//destructor
PreservingData::~PreservingData()
{
}

//start cacheserver
void PreservingData::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
}

//stop cacheserver
void PreservingData::stopServer()
{
  CacheHelper::closeServer( 1 );
}

//Initalize Region
void PreservingData::initRegion()
{
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr = regionFactory->setCachingEnabled(true)->create("exampleRegion");
}

/**
* @brief Example 7.2 Setting the Server Redundancy Level Programmatically.
* Following example creates a client cache with two redundant cache servers configured in addition
* to the primary server.
*/
void PreservingData::example_7_2()
{
  PropertiesPtr pp = Properties::create( );
  systemPtr = CacheFactory::createCacheFactory(pp);
  // Create a cache.
  cachePtr = systemPtr->setSubscriptionEnabled(true)
      ->addServer("localhost", 24680)
      ->addServer("localhost", 24681)
      ->addServer("localhost", 24682)
      ->setSubscriptionRedundancy(2)
      ->create();
}

/**
* @brief Example 7.5 Configuring a Durable Client Through the API (C++).
* This programmatic example creates a durable client using the CacheFactory::createCacheFactory call.
*/
void PreservingData::example_7_5()
{
  // Create durable client's properties using the C++ api.
  PropertiesPtr pp = Properties::create();
  pp->insert("durable-client-id", "DurableClientId");
  pp->insert("durable-timeout", 200);
  cacheFactoryPtr =	CacheFactory::createCacheFactory(pp);
}

/**
* @brief Example 7.6 API Client Durable Interest List Registration (C++).
* Durable client interest registration can be durable (true) or nondurable
* (default). The following programmatic example registers durable interest in Key-1. The interest
* registration happens immediately after region creation and before anything else.
*/
void PreservingData::example_7_6()
{
  // Durable client interest registration can be durable (true) or nondurable
  //(default).
  VectorOfCacheableKey keys;
  keys.push_back( CacheableString::create("Key-1") );
  regionPtr->registerKeys(keys,true);
}

/**
* @brief Example 7.7 Durable Client Cache Ready Notification (C++).
* The following example shows how to call readyForEvents
*/
void PreservingData::example_7_7()
{
  //Send ready for event message to server(only for durable clients).
  //Server will send queued events to client after receiving this.
  cachePtr->readyForEvents();
}

/**
* @brief Example 7.8 Durable Client Disconnect With Queues Maintained.
*/
void PreservingData::example_7_8()
{
  // Close the GemFire Cache with keepalive=true. Server will queue events for
  // durable registered keys and deliver all events when client reconnects
  // within timeout period and sends "readyForEvents()"
  cachePtr->close(true);
}

int main(int argc, char* argv[])
{
  try {
    printf("\nPreservingData EXAMPLES: Starting...");
    PreservingData cp7;
    printf("\nPreservingData EXAMPLES: Starting server...");
    cp7.startServer();
    printf("\nPreservingData EXAMPLES: Running example 7.2...");
    cp7.example_7_2();
    CacheHelper::cleanUp(cp7.cachePtr);
    printf("\nPreservingData EXAMPLES: Running example 7.5...");
    cp7.example_7_5();
    printf("\nPreservingData EXAMPLES: Init Cache...");
    CacheHelper::initCache(cp7.cacheFactoryPtr, cp7.cachePtr);
    printf("\nPreservingData EXAMPLES: Init Region...");
    cp7.initRegion();
    printf("\nPreservingData EXAMPLES: Running example 7.7...");
    cp7.example_7_7();
    printf("\nPreservingData EXAMPLES: Running example 7.6...");
    cp7.example_7_6();
    printf("\nPreservingData EXAMPLES: Running example 7.8...");
    cp7.example_7_8();
    printf("\nPreservingData EXAMPLES: stopping server...");
    cp7.stopServer();
    printf("\nPreservingData EXAMPLES: All Done.");
  }catch (const Exception & excp)
  {
    printf("\nEXAMPLES: %s: %s", excp.getName(), excp.getMessage());
    exit(1);
  }
  catch(...)
  {
    printf("\nEXAMPLES: Unknown exception");
    exit(1);
  }
  return 0;
}

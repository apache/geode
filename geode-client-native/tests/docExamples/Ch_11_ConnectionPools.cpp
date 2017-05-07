/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_11_ConnectionPools.hpp"

using namespace gemfire;
using namespace docExample;

//constructor
ConnectionPools::ConnectionPools()
{
}

//destructor
ConnectionPools::~ConnectionPools()
{
}

//start locator
void ConnectionPools::startLocator()
{
  CacheHelper::initLocator(1);
}

//stop locator
void ConnectionPools::stopLocator()
{
  CacheHelper::closeLocator(1);
}

//start cacheserver
void ConnectionPools::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml", "localhost:34756");
}

//stop cacheserver
void ConnectionPools::stopServer()
{
  CacheHelper::closeServer( 1 );
}

/**
* @brief Example 11.2 Connection Pool Creation and Execution Using C++.
* The following example demonstrates a simple procedure to create a pool factory and then create a
* pool instance in C++. It also help you to execute a query.
*/
void ConnectionPools::example_11_2()
{
  PropertiesPtr prptr = Properties::create();
  systemPtr = CacheFactory::createCacheFactory(prptr);

  cachePtr = systemPtr->create();
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  //to create pool add either endpoints or add locators or servers
  //pool with endpoint, adding to pool factory
  //poolFacPtr->addServer("localhost", 12345 /*port number*/);
  //pool with locator, adding to pool factory
  poolFacPtr->addLocator("localhost", 34756 /*port number*/);
  PoolPtr pptr = NULLPTR;
  if ((PoolManager::find("examplePool")) == NULLPTR) {// Pool does not exist with the same name.
    pptr = poolFacPtr->create("examplePool");
  }
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr = regionFactory ->setCachingEnabled(true) ->setPoolName(
      "examplePool") ->create("regionName");
  QueryServicePtr qs = cachePtr->getQueryService("examplePool");
}

int main(int argc, char* argv[])
{
  try {
    printf("\nConnectionPools EXAMPLES: Starting...");
    ConnectionPools cp11;
    printf("\nConnectionPools EXAMPLES: Starting locator...");
    cp11.startLocator();
    printf("\nConnectionPools EXAMPLES: Starting server with locator...");
    cp11.startServer();
    printf("\nConnectionPools EXAMPLES: Running example 11.2...");
    cp11.example_11_2();
    CacheHelper::cleanUp(cp11.cachePtr);
    printf("\nConnectionPools EXAMPLES: stopping server with locator...");
    cp11.stopServer();
    printf("\nConnectionPools EXAMPLES: stopping locator...");
    cp11.stopLocator();
    printf("\nConnectionPools EXAMPLES: All Done.");
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

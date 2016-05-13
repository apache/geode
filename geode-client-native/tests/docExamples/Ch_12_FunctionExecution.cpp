/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "CacheHelper.hpp"
#include "Ch_12_FunctionExecution.hpp"

using namespace gemfire;
using namespace docExample;

const char* poolName = (char*)"__POOL1__";
char* func = (char*)"MultiGetFunctionI";
const char * serverGroup = (char*)"ServerGroup1";

//constructor
FunctionExecution::FunctionExecution()
{
}

//destructor
FunctionExecution::~FunctionExecution()
{
}

//start cacheserver
void FunctionExecution::startServer()
{
  CacheHelper::initServer( 1, "remoteQuery.xml" );
}
//stop cacheserver
void FunctionExecution::stopServer()
{
  CacheHelper::closeServer( 1 );
}

//init region
RegionPtr FunctionExecution::initRegion()
{
  createPoolRegion();
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regPtr0 = regionFactory->setCachingEnabled(true)->setPoolName(poolName)
         ->create("partition_region");
  regPtr0->registerAllKeys();
  return regPtr0;
}

//create pool region
void FunctionExecution::createPoolRegion()
{
  pptr = createPool(poolName, serverGroup, false, true, 0, true );
}

//registers all the keys in to region
void FunctionExecution::registerAll()
{
  regPtr0->registerAllKeys();
}

PoolPtr  FunctionExecution::createPool(const char* poolName, const char* serverGroup,
                                       bool locator , bool server , int redundancy , bool clientNotification , int subscriptionAckInterval )
{
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();

  if( server == true )//with explicit server list
  {
    poolFacPtr->addServer("localhost", 24680);
    //do region creation with end
  }
  else if(locator == true) //with locator
  {
    poolFacPtr->addLocator("localhost", 24680);
    if(serverGroup){
      poolFacPtr->setServerGroup(serverGroup);
    }
  }
  else //neither server nor locator return NULLPTR
  {
    return PoolPtr( NULLPTR);
  }
  poolFacPtr->setSubscriptionRedundancy(redundancy);
  poolFacPtr->setSubscriptionEnabled(clientNotification);
  if ( subscriptionAckInterval != -1 ) {
    poolFacPtr->setSubscriptionAckInterval( subscriptionAckInterval );
  }

  return poolFacPtr->create(poolName);
}

/**
* @brief Example 12.1 Data-Dependent Function Invoked from a Client (C++).
* The following code snippet shows the execution of a data-dependent function from a GemFire client
*/
void FunctionExecution::example_12_1()
{
  regPtr0 = initRegion();
  ExecutionPtr exc = FunctionService::onRegion(regPtr0);
  CacheableVectorPtr routingObj = CacheableVector::create();
  char buf[128];
  bool getResult = true;

  sprintf(buf, "VALUE--%d", 10);
  CacheablePtr value(CacheableString::create(buf));

  sprintf(buf, "KEY--%d", 100);
  CacheableKeyPtr key = CacheableKey::create(buf);
  regPtr0->put(key, value);

  sprintf(buf, "KEY--%d", 100);
  CacheableKeyPtr key1 = CacheableKey::create(buf);
  routingObj->push_back(key1);

  CacheablePtr args = routingObj;
  CacheableVectorPtr executeFunctionResult = exc->withFilter(routingObj)
    ->withArgs(args)->execute(func, getResult)->getResult();
}

/**
* @brief Example 12.3 Function Execution on a Server Pool in a Distributed System (C++).
* The code snippet in the following example shows a function being invoked on a server pool.
* This snippet invokes ExecutionPtr on all regions on the server pool pptr.
*/
void FunctionExecution::example_12_3()
{
  pptr = PoolManager::find(poolName);
  ExecutionPtr exc = FunctionService::onServer(pptr);
  CacheableVectorPtr routingObj = CacheableVector::create();
  char buf[128];
  bool getResult = true;
  sprintf(buf, "VALUE--%d", 10);
  CacheablePtr value(CacheableString::create(buf));

  sprintf(buf, "KEY--%d", 100);
  CacheableKeyPtr key = CacheableKey::create(buf);
  regPtr0->put(key, value);

  sprintf(buf, "KEY--%d", 100);
  CacheableKeyPtr key1 = CacheableKey::create(buf);
  routingObj->push_back(key1);

  CacheablePtr args = routingObj;
  CacheableVectorPtr executeFunctionResult = exc->withArgs(args)->execute(func,
    getResult)->getResult();
}

int main(int argc, char* argv[])
{
  try {
    printf("\nFunctionExecution EXAMPLES: Starting...");
    FunctionExecution cp12;
    printf("\nFunctionExecution EXAMPLES: Starting server...");
    cp12.startServer();
    CacheHelper::connectToDs(cp12.cacheFactoryPtr);
    printf("\nFunctionExecution EXAMPLES: Initialize cache...");
    CacheHelper::initCache(cp12.cacheFactoryPtr, cp12.cachePtr, true);
    printf("\nFunctionExecution EXAMPLES: Running example 12.1...");
    cp12.example_12_1();
    printf("\nFunctionExecution EXAMPLES: Running example 12.3...");
    cp12.example_12_3();
    CacheHelper::cleanUp(cp12.cachePtr);
    printf("\nFunctionExecution EXAMPLES: stopping server...");
    cp12.stopServer();
    printf("\nFunctionExecution EXAMPLES: All Done.");
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

/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

// Example 13.3 Implementing a Cache Loader Using the C++ API
#include "CacheHelper.hpp"
#include "Ch_13_ExampleCacheLoader.hpp"

using namespace gemfire;
using namespace docExample;

class ExampleCacheLoader;

typedef gemfire::SharedPtr< ExampleCacheLoader >
ExampleCacheLoaderPtr;

/**
* @brief Example 13.3 Implementing a Cache Loader Using the C++ API
* The following example uses the C++ API to implement a cache loader, which is generally used to
* retrieve data from an outside source.
*/
class ExampleCacheLoader : virtual public CacheLoader {
private:
  int32_t m_loads;
public:

  ExampleCacheLoader():CacheLoader(),
    m_loads( 0 )
  {}

  virtual ~ExampleCacheLoader() {}

  CacheablePtr load(const RegionPtr& rp,
    const CacheableKeyPtr& key,
    const UserDataPtr& aCallbackArgument) {
      printf( "\nExampleCacheLoader.load : successful");
      return CacheableInt32::create(m_loads++);
  }

  virtual void close( const RegionPtr& region )
  {
    printf("ExampleCacheLoader::close");
  }
};

CacheLoaderCheck::CacheLoaderCheck()
{
}

CacheLoaderCheck::~CacheLoaderCheck()
{
}

//start cacheserver
void CacheLoaderCheck::startServer()
{
  CacheHelper::initServer( 1, "cacheserver_notify_subscription.xml" );
}
//stop cacheserver
void CacheLoaderCheck::stopServer()
{
  CacheHelper::closeServer( 1 );
}

// init region
void CacheLoaderCheck::initRegion()
{
  // Create a  region.
  RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);
  regionPtr = regionFactory
      ->setCachingEnabled(true)
      ->create("exampleRegion");
}

int main(int argc, char* argv[])
{
  try {
    printf("\nCacheLoaderCheck EXAMPLES: Starting...");
    CacheLoaderCheck cl;
    printf("\nCacheLoaderCheck EXAMPLES: Starting server...");
    cl.startServer();
    CacheHelper::connectToDs(cl.cacheFactoryPtr);
    printf("\nCacheLoaderCheck EXAMPLES: Init Cache...");
    CacheHelper::initCache(cl.cacheFactoryPtr, cl.cachePtr);
    printf("\nCacheLoaderCheck EXAMPLES: get Region...");
    cl.initRegion();

    cl.regionPtr->get("Key1");
    cl.regionPtr->put("Key1", "Value1");

    printf("\nCacheLoaderCheck EXAMPLES: cleanup...");
    CacheHelper::cleanUp(cl.cachePtr);
    printf("\nCacheLoaderCheck EXAMPLES: stopping server...");
    cl.stopServer();

    printf("\nCacheLoaderCheck EXAMPLES: All Done.");
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

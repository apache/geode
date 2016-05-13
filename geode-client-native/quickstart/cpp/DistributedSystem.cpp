/*
 * The DistributedSystem QuickStart Example.
 * This example creates two pools programatically.
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Now it creates Pool with poolName1.
 * 3. Adds server(localhost:40404) to pool factory.
 * 4. Creates region "root1" with pool.
 * 5. Put Entries (Key and Value pairs) into the Region.
 * 6. Get Entries from the Region.
 * 7. Invalidate an Entry in the Region.
 * 8. Destroy an Entry in the Region.
 * 9. Now it creates another reagion "root2" with another pool "poolName2", which connects to the server(localhost:40405).
 * 10. Now it do put/get operations.
 * 10. Close the Cache.
 *
 */


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;


void distributedsystem( CachePtr cachePtr, char * hostname, int port, char * poolName, char * regionName)
{
  try
  {
    //create pool factory to create the pool.
    PoolFactoryPtr poolFacPtr = PoolManager::createFactory();

    //adding host(endpoint) in pool
    poolFacPtr->addServer(hostname, port);

    //enabling subscription on pool
    poolFacPtr->setSubscriptionEnabled(true);

    //creating pool with name "examplePool"
    poolFacPtr->create(poolName);

    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

    LOGINFO("Created the RegionFactory");

    //setting pool to attach with region    
    regionFactory->setPoolName(poolName);

    //creating first root region
    RegionPtr regionPtr = regionFactory->create( regionName);
    
    LOGINFO("Created Region.");
    
    // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
    regionPtr->put("Key1", "Value1");
    
    LOGINFO("Put the first Entry into the Region");
    
    // Put an Entry into the Region by manually creating a Key and a Value pair.
    CacheableKeyPtr keyPtr = CacheableInt32::create(123);
    CacheablePtr valuePtr = CacheableString::create("123");
    regionPtr->put(keyPtr, valuePtr);
    
    LOGINFO("Put the second Entry into the Region");
    
    // Get Entries back out of the Region.
    CacheablePtr result1Ptr = regionPtr->get("Key1");
    
    LOGINFO("Obtained the first Entry from the Region");
    
    CacheablePtr result2Ptr = regionPtr->get(keyPtr);
    
    LOGINFO("Obtained the second Entry from the Region");
    
    // Invalidate an Entry in the Region.
    regionPtr->invalidate("Key1");
    
    LOGINFO("Invalidated the first Entry in the Region");
    
    // Destroy an Entry in the Region.
    regionPtr->destroy(keyPtr);
    
    LOGINFO("Destroyed the second Entry in the Region");
    
 
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {    
    LOGERROR("DistributedSystem GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


// The DistributedSystem QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
    
    LOGINFO("Connected to the GemFire Distributed System");
    
    // Create a GemFire Cache.
    CachePtr cachePtr = cacheFactory->create();

    LOGINFO("Created the GemFire Cache");

    distributedsystem( cachePtr, (char *)"localhost", 40404, (char *)"poolName1", (char *)"exampleRegion1");
    distributedsystem( cachePtr, (char *)"localhost", 40405, (char *)"poolName2", (char *)"exampleRegion2");

    // Close the GemFire Cache.
    cachePtr->close();
    
    LOGINFO("Closed the GemFire Cache");
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {    
    LOGERROR("DistributedSystem GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


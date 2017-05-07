/*
 * The Transaction QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Create the example Region Programmatically.
 * 3 Begin Transaction
 * 4. Put Entries (Key and Value pairs) into the Region.
 * 5. Commit Transaction
 * 6. Get Entries from the Region.
 * 7. Begin Transaction
 * 8. Put Entries (Key and Value pairs) into the Region.
 * 9. Destroy key
 * 10. Rollback transaction
 * 11. Get Entries from the Region.
 * 12. Close the Cache.
 *
 */


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The Transaction QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    // Create a GemFire Cache.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    CachePtr cachePtr = cacheFactory->create();       

    LOGINFO("Created the GemFire Cache");

    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

    LOGINFO("Created the RegionFactory");

    // Create the example Region Programmatically.
    RegionPtr regionPtr = regionFactory->create("exampleRegion");

    CacheableKeyPtr keyPtr = CacheableInt32::create(123);
    LOGINFO("Created the Region Programmatically.");
    CacheTransactionManagerPtr txManager = cachePtr->getCacheTransactionManager();
    //start a transaction
    txManager->begin();
    LOGINFO("Transaction Started");

    regionPtr->put("Key1", "Value1");
    regionPtr->put("Key2", "Value2");        
    LOGINFO("Put two entries into the region");

    try{
      // Commit the transaction
      txManager->commit();
      LOGINFO("Transaction Committed");
    } catch(const CommitConflictException&){
      LOGINFO("Got CommitConflictException");
    }

    if(regionPtr->containsKey("Key1")) {
      LOGINFO("Obtained the first entry from the Region");
    }

    if(regionPtr->containsKey("Key2")) {
      LOGINFO("Obtained the second entry from the Region");
    }

    txManager->begin();
    LOGINFO("Transaction Started");

    regionPtr->put("Key3", "Value3");
    LOGINFO("Put the third entry into the Region");

    regionPtr->destroy("Key1");
    LOGINFO("destroy the first entry");

    txManager->rollback();
    LOGINFO("Transaction Rollbacked");

    if(regionPtr->containsKey("Key1")) {
      LOGINFO("Obtained the first entry from the Region");
    }

    if(regionPtr->containsKey("Key2")) {
      LOGINFO("Obtained the second entry from the Region");
    }

    if(regionPtr->containsKey("Key3")) {
      LOGINFO("ERROR: Obtained the third entry from the Region.");
    }

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {    
    LOGERROR("Transaction GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


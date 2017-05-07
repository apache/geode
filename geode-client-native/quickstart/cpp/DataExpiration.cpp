/*
 * The DataExpiration QuickStart Example.
 *
 * This example takes the following steps:
 *
 *  1. Create a GemFire Cache programmatically.
 *  2. Create the example Region programmatically.
 *  3. Set the SimpleCacheListener plugin on the Region.
 *  4. Put 3 Entries into the Region.
 *  5. Get the Entry Idle Timeout setting from the Region.
 *  6. Count the Keys in the Region before the Timeout duration elapses.
 *  7. Wait for the Timeout Expiration Action to be reported by the SimpleCacheListener.
 *  8. Count the remaining Keys in the Region after the Timeout duration elapses.
 *  9. Close the Cache.
 * 
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Include the SimpleCacheListener plugin.
#include "plugins/SimpleCacheListener.hpp"

// Use the "gemfire" namespace.
using namespace gemfire;

// The DataExpiration QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    // Create a GemFire Cache Programmatically.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)
                                    ->create();

    LOGINFO("Created the GemFire Cache");

    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

    LOGINFO("Created the RegionFactory");


    // Create the example Region programmatically.
    RegionPtr regionPtr = regionFactory
      ->setEntryIdleTimeout(ExpirationAction::DESTROY, 10)
      ->create("exampleRegion");

    LOGINFO("Created the Region from the Cache");

    // Plugin the SimpleCacheListener to the Region.
    AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
    attrMutatorPtr->setCacheListener(CacheListenerPtr(new SimpleCacheListener()));

    LOGINFO("Set the SimpleCacheListener on the Region");

    // Put 3 Entries into the Region
    regionPtr->put("Key1", "Value1");
    regionPtr->put("Key2", "Value2");
    regionPtr->put("Key3", "Value3");

    LOGINFO("Put 3 Entries into the Region");

    // Get the Entry Idle Timeout specified in the Cache XML file.
    int entryIdleTimeout = regionPtr->getAttributes()->getEntryIdleTimeout();

    LOGINFO("Got Entry Idle Timeout as %d seconds", entryIdleTimeout);

    // Wait for half the Entry Idle Timeout duration, using gemfire::millisleep().
    millisleep(entryIdleTimeout * 1000 / 2);

    // Get the number of Keys remaining in the Region, should be all 3.
    VectorOfCacheableKey keys;
    regionPtr->keys(keys);

    LOGINFO("Got %d keys before the Entry Idle Timeout duration elapsed", keys.size());

    // Get 2 of the 3 Entries from the Region to "reset" their Idle Timeouts.
    CacheablePtr value1Ptr = regionPtr->get("Key1");
    CacheablePtr value2Ptr = regionPtr->get("Key2");

    LOGINFO("The SimpleCacheListener should next report the expiration action");

    // Wait for the entire Entry Idle Timeout duration, using gemfire::millisleep().
    gemfire::millisleep(entryIdleTimeout * 1000);

    // Get the number of Keys remaining in the Region, should be 0 now.
    regionPtr->keys(keys);

    LOGINFO("Got %d keys after the Entry Idle Timeout duration elapsed", keys.size());

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("DataExpiration GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


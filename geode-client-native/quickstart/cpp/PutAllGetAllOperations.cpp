/*
 * The PutAllGetAllOperations QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache using CacheFactory. By default it will connect to "localhost" at port 40404".
 * 2. Create a Example Region.
 * 3. PutAll Entries (Key and Value pairs) into the Region.
 * 4. GetAll Entries from the Region.
 * 5. Close the Cache.
 *
 */


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The PutAllGetAllOperations QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    //Create a GemFire Cache using CacheFactory. By default it will connect to "localhost" at port 40404".
    CachePtr cachePtr  = CacheFactory::createCacheFactory()->create();

    LOGINFO("Created the GemFire Cache");

    //Set Attributes for the region.
    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

    //Create exampleRegion.
    RegionPtr regionPtr = regionFactory->create( "exampleRegion" );

    LOGINFO("Created the Region");

    // Put bulk Entries (Key and Value pairs) into the Region.
    HashMapOfCacheable entryMap;
    char key[2048];
    char value[2048];
    for (int32_t item = 0; item < 100; item++) {
      sprintf(key, "key-%d", item);
      sprintf(value, "%d", item);
      entryMap.insert(CacheableKey::create(key), CacheableString::create(value));
    }
    regionPtr->putAll(entryMap);


    LOGINFO("PutAll 100 entries into the Region");

    //GetAll Entries back out of the Region
    VectorOfCacheableKey keys;
    for (int32_t item = 0; item < 100; item++) {
      sprintf(key, "key-%d", item);
      keys.push_back(CacheableKey::create(key));
    }

    HashMapOfCacheablePtr values(new HashMapOfCacheable());
    regionPtr->getAll(keys, values, NULLPTR, true);

    LOGINFO("Obtained 100 entries from the Region");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("PutAllGetAllOperations GemFire Exception: %s", gemfireExcp.getMessage());
  }
}

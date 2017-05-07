/*
 * The BasicOperations QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Create the example Region Programmatically.
 * 3.a. Put Entries (Key and Value pairs) into the Region.
 * 3.b. If in 64 bit mode put over 4 GB data to demonstrate capacity.
 * 4. Get Entries from the Region.
 * 5. Invalidate an Entry in the Region.
 * 6. Destroy an Entry in the Region.
 * 7. Close the Cache.
 *
 */


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The BasicOperations QuickStart example.
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
    
    LOGINFO("Created the Region Programmatically.");
    
    // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
    regionPtr->put("Key1", "Value1");
    LOGINFO("Put the first Entry into the Region");     
    
    // Put an Entry into the Region by manually creating a Key and a Value pair.
    CacheableKeyPtr keyPtr = CacheableInt32::create(123);
    CacheablePtr valuePtr = CacheableString::create("123");
    regionPtr->put(keyPtr, valuePtr);
    
    LOGINFO("Put the second Entry into the Region");
    
#if ( defined(_WIN64) || defined(__sparcv9) || defined(__x86_64__) )
    // Put just over 4 GB of data locally
    char * text = new char[1024 * 1024 /* 1 MB */];
    memset(text, 'A', 1024 * 1024 - 1);
    text[1024 * 1024 - 1] = '\0';
    
    for (int item = 0; item < ( 5 * 1024 /* 5 GB */ ); item++)
    {
      regionPtr->localPut(item, text);
    }
    
    LOGINFO("Put over 4 GB data locally");
#endif // 64-bit
    
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
    
    // Close the GemFire Cache.
    cachePtr->close();
    
    LOGINFO("Closed the GemFire Cache");
    
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {    
    LOGERROR("BasicOperations GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


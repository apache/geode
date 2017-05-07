/*
 * The RefIDExample QuickStart Example.
 * This example creates two pools through XML and sets region attributes using refid.
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Now it creates 2 Pools with the names poolName1, poolName2 respectively.
 * 3. Sets the region attribute using refid.
 * 4. Gets the region "root1" with poolName1, and region "root2" with poolName2.
 * 5. Check for the region attribute set through refid.
 * 6. Put Entries (Key and Value pairs) into both the Regions.
 * 7. Get Entries from the Regions.
 * 8. Invalidate an Entry in both the Regions.
 * 9. Destroy an Entry in both the Regions.
 * 10. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;


// The RefIDExample QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    // Create a GemFire Cache using XMLs/clientRefIDExample.xml.
    PropertiesPtr prptr = Properties::create();
    prptr->insert("cache-xml-file", "XMLs/clientRefIDExample.xml");

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(prptr);
   
    CachePtr cachePtr = cacheFactory->create();       

    LOGINFO("Created the GemFire Cache");

    // get the root1 Region.
    RegionPtr regionPtr1 = cachePtr->getRegion("root1");

    LOGINFO("Obtained the root1 Region from the Cache");

    // get the root2 Region.
    RegionPtr regionPtr2 = cachePtr->getRegion("root2");

    LOGINFO("Obtained the root2 Region from the Cache");

    RegionAttributesPtr rAttPtr1 = regionPtr1->getAttributes();
    RegionAttributesPtr rAttPtr2 = regionPtr2->getAttributes();

    bool isCacheEnabled1 = rAttPtr1->getCachingEnabled();
    LOGINFO("For region root1 cachingEnabled is %s ", isCacheEnabled1 ? "true" : "false");

    bool isCacheEnabled2 = rAttPtr2->getCachingEnabled();
    LOGINFO("For region root2 cachingEnabled is %s ", isCacheEnabled2 ? "true" : "false");
    
    // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
    regionPtr1->put("Key1", "Value1");
    regionPtr2->put("Key1", "Value1");
    
    LOGINFO("Put the first Entries into both the Regions");
    
    // Put an Entry into the Region by manually creating a Key and a Value pair.
    CacheableKeyPtr keyPtr = CacheableInt32::create(123);
    CacheablePtr valuePtr = CacheableString::create("123");
    regionPtr1->put(keyPtr, valuePtr);
    regionPtr2->put(keyPtr, valuePtr);
    
    LOGINFO("Put the second Entries into both the Regions.");
    
    // Get Entries back out of the Region.
    CacheablePtr resultPtr1 = regionPtr1->get("Key1");
    CacheablePtr resultPtr2 = regionPtr2->get("Key1");
    LOGINFO("Obtained the first Entry from both the Regions");

    resultPtr1 = regionPtr1->get(keyPtr);
    resultPtr2 = regionPtr2->get(keyPtr);    
    LOGINFO("Obtained the second Entry from both the Regions");
    
    // Invalidate an Entry in the Region.
    regionPtr1->invalidate("Key1");
    regionPtr2->invalidate("Key1");
    
    LOGINFO("Invalidated the first Entry in both the Regions.");
    
    // Destroy an Entry in the Region.
    regionPtr1->destroy(keyPtr);
    regionPtr2->destroy(keyPtr);
    
    LOGINFO("Destroyed the second Entry in both the Regions");

    // Close the GemFire Cache.
    cachePtr->close();
    
    LOGINFO("Closed the GemFire Cache");
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {    
    LOGERROR("RefIDExample GemFire Exception: %s", gemfireExcp.getMessage());
  }
}

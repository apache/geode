/*
 * The LoaderListenerWriter QuickStart Example.
 *
 * This example takes the following steps:
 *
 *  1. Create a GemFire Cache.
 *  2. Get the example Region from the Cache.
 *  3. Set the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWriter plugins on the Region.
 *  4. Put 3 Entries into the Region.
 *  5. Update an Entry in the Region.
 *  6. Destroy an Entry in the Region.
 *  7. Invalidate an Entry in the Region.
 *  8. Get a new Entry from the Region.
 *  9. Get the destroyed Entry from the Region.
 * 10. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Include the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWriter plugins.
#include "plugins/SimpleCacheLoader.hpp"
#include "plugins/SimpleCacheListener.hpp"
#include "plugins/SimpleCacheWriter.hpp"

// Use the "gemfire" namespace.
using namespace gemfire;

// The LoaderListenerWriter QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    // Create a GemFire Cache with the "clientLoaderListenerWriter.xml" Cache XML file.
    CachePtr cachePtr = cacheFactory->set("cache-xml-file", "XMLs/clientLoaderListenerWriter.xml")->create();

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML file.
    RegionPtr regionPtr =cachePtr->getRegion("exampleRegion");

    LOGINFO("Obtained the Region from the Cache");

    // Plugin the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWrite to the Region.
    AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
    attrMutatorPtr->setCacheLoader(CacheLoaderPtr(new SimpleCacheLoader()));
    attrMutatorPtr->setCacheListener(CacheListenerPtr(new SimpleCacheListener()));
    attrMutatorPtr->setCacheWriter(CacheWriterPtr(new SimpleCacheWriter()));

    LOGINFO("Attached the simple plugins on the Region");

    // The following operations should cause the plugins to print the events.

    // Put 3 Entries into the Region.
    regionPtr->put("Key1", "Value1");
    regionPtr->put("Key2", "Value2");
    regionPtr->put("Key3", "Value3");

    // Update Key3.
    regionPtr->put("Key3", "Value3-updated");

    // Destroy Key3.
    regionPtr->destroy("Key3");

    // Invalidate Key2.
    regionPtr->invalidate("Key2");

    // Get a new Key.
    regionPtr->get("Key4");

    // Get a destroyed Key.
    regionPtr->get("Key3");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("LoaderListenerWriter GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


/*
 * The Interop QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Cache.
 * 3. Put an Entry (Key and Value pair) into the Region.
 * 4. Get Entries from the Region put by other clients.
 * 5. Close the Cache.
 *
 */


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The Interop QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    // Create CacheFactory using the default system properties.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
    
    // Create a GemFire Cache.
    CachePtr cachePtr = cacheFactory->set("cache-xml-file", "XMLs/clientInterop.xml")->create();
    
    LOGINFO("CPP CLIENT: Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML file.
    RegionPtr regionPtr = cachePtr->getRegion("exampleRegion");

    LOGINFO("CPP CLIENT: Obtained the Region from the Cache");

    // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
    regionPtr->put("Key-CPP", "Value-CPP");

    LOGINFO("CPP CLIENT: Put the C++ Entry into the Region");

    // Wait for all values to be available.
    CacheablePtr value1 = NULLPTR;
    CacheablePtr value2 = NULLPTR;
    CacheablePtr value3 = NULLPTR;

    while (value1 == NULLPTR || (argc > 1 && value2 == NULLPTR) || value3 == NULLPTR)
    {
      LOGINFO("CPP CLIENT: Checking server for keys...");
      value1 = regionPtr->get("Key-CPP");
      value2 = regionPtr->get("Key-CSHARP");
      value3 = regionPtr->get("Key-JAVA");
      gemfire::millisleep(1000);
    }

    LOGINFO("CPP CLIENT: Key-CPP value is %s", value1->toString()->asChar());
    LOGINFO("CPP CLIENT: Key-CSHARP value is %s", value2 != NULLPTR ? value2->toString()->asChar() : "null");
    LOGINFO("CPP CLIENT: Key-JAVA value is %s", value3->toString()->asChar());

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("CPP CLIENT: Closed the GemFire Cache");
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("CPP CLIENT: Interop GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


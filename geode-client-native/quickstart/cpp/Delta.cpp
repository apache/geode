// Include the GemFire library.

/*
 * The Delta QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Cache.
 * 3. Put an Entry into the Region.
 * 4. Set delta for a value.
 * 5. Put entry with delta in region.
 * 6. Local-invalidate entry in region.
 * 7. Get entry from server.
 * 8. Verify that delta was applied on server, by examining entry.
 * 9. Close the Cache.
 * 
 */

#include <gfcpp/GemfireCppCache.hpp>

#include "deltaobjects/DeltaExample.hpp"

// Use the "gemfire" namespace.
using namespace gemfire;

// The Delta QuickStart example.
typedef SharedPtr<DeltaExample> DeltaExamplePtr;
int main(int argc, char ** argv)
{
  try
  {
    // Create a GemFire Cache.
    PropertiesPtr prptr = Properties::create();
    prptr->insert("cache-xml-file", "XMLs/clientDelta.xml");

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(prptr);
   
    CachePtr cachePtr = cacheFactory->create();       

    LOGINFO("Created the GemFire Cache");

    // get the example Region.
    RegionPtr regPtr = cachePtr->getRegion("exampleRegion");

    LOGINFO("Obtained the Region from the Cache");

    // Register our Serializable/Cacheable Delta objects, DeltaExample.
    Serializable::registerType( DeltaExample::create );

    //Creating Delta Object.
    DeltaExample* ptr = new DeltaExample(10, 15, 20 );
    CacheablePtr valPtr(ptr);
    //Put the delta object. This will send the complete object to the server.
    regPtr->put( "Key1", valPtr );
    LOGINFO("Completed put for a delta object");
    
    //Changing state of delta object.
    ptr->setField1( 9 );
    
    //Put delta object again. Since delta flag is set true it will calculate
    //Delta and send only Delta to the server.
    regPtr->put( "Key1", valPtr );
    LOGINFO("Completed put with delta");
    
    //Locally invalidating the key.
    regPtr->localInvalidate("Key1");
    
    //Fetching the value from server.
    DeltaExamplePtr retVal = dynCast<DeltaExamplePtr> (regPtr->get("Key1"));
    
    //Verification
    if( retVal->getField1() != 9 )
      throw Exception("First field should have been 9");
    if( retVal->getField2() != 15 )
      throw Exception("Second field should have been 15");
    if( retVal->getField3() != 20 )
      throw Exception("Third field should have been 20");
    LOGINFO("Delta has been successfully applied at server");

    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur

  catch(const Exception & gemfireExcp)
  {
    LOGERROR("Delta GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


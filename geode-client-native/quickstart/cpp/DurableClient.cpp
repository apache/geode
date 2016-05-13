/*
 * The Durable Client QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache with durable client properties Programmatically.
 * 2. Create the example Region Programmatically.
 * 3. Set Cacelistener with "afterRegionLive implementation to region.
 * 4. Register Interest to region with durable option
 * 5. call to readyForEvent().
 * 6. Close the Cache with keepalive options as true.
 *
 */


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

//Include cachelistener
#include "plugins/DurableCacheListener.hpp"

// Use the "gemfire" namespace.
using namespace gemfire;

void RunDurableClient()
{
    // Create durable client's properties using api.
    PropertiesPtr pp  = Properties::create();
    pp->insert("durable-client-id", "DurableClientId");
    pp->insert("durable-timeout", 300);

    // Create a GemFire Cache Programmatically.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(pp);
    CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)
                                    ->create();

    LOGINFO("Created the GemFire Cache Programmatically");
    
    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(CACHING_PROXY);

    // Create the Region Programmatically.
    RegionPtr regionPtr = regionFactory->create("exampleRegion");    

    LOGINFO("Created the Region Programmatically");

    // Plugin the CacheListener with afterRegionLive. "afterRegionLive()"  will be called
    // after all the queued events are recieved by client
    AttributesMutatorPtr attrMutatorPtr = regionPtr->getAttributesMutator();
    attrMutatorPtr->setCacheListener(CacheListenerPtr(new DurableCacheListener()));

    LOGINFO("DurableCacheListener set to region.");

    // For durable Clients, Register Intrest can be durable or non durable ( default ),
    // Unregister Interest APIs remain same.

    VectorOfCacheableKey keys;
    keys.push_back( CacheableString::create("Key-1") );
    regionPtr->registerKeys(keys, true);

    LOGINFO("Called Register Interest for Key-1 with isDurable as true");

    //Send ready for Event message to Server( only for Durable Clients ).
    //Server will send queued events to client after recieving this.
    cachePtr->readyForEvents();

    LOGINFO("Sent ReadyForEvents message to server");

    //wait for some time to recieve events
    gemfire::millisleep(1000);

    // Close the GemFire Cache with keepalive = true.  Server will queue events for
    // durable registered keys and will deliver all events when client will reconnect
    // within timeout period and send "readyForEvents()"
    cachePtr->close(true);

    LOGINFO("Closed the GemFire Cache with keepalive as true");

}
void RunFeeder()
{

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();
    LOGINFO("Feeder connected to the GemFire Distributed System");

    CachePtr cachePtr = cacheFactory->create();       
    
    LOGINFO("Created the GemFire Cache");
    
    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(PROXY);
    
    LOGINFO("Created the RegionFactory");
    
    // Create the Region Programmatically.
    RegionPtr regionPtr = regionFactory->create("exampleRegion");

    LOGINFO("Created the Region Programmatically.");

    // create two keys in region
    const char* keys[] = {"Key-1","Key-2" };
    const char* vals[] = { "Value-1", "Value-2" };

    for ( int i =0; i < 2; i++) 
    {
      CacheableKeyPtr keyPtr = createKey( keys[i] );
      CacheableStringPtr valPtr = CacheableString::create( vals[i] );

      regionPtr->create( keyPtr, valPtr );
    }
    LOGINFO("Created Key-1 and Key-2 in region. Durable interest was registered only for Key-1.");

    // Close the GemFire Cache
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

}


// The DurableClient QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    //First Run of Durable Client
    RunDurableClient();

    //Intermediate Feeder, feeding events
    RunFeeder();

    //Reconnect Durable Client
    RunDurableClient();
  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("DurableClient GemFire Exception: %s", gemfireExcp.getMessage());
  }
}


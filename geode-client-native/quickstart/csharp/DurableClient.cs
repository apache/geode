/*
 * The Durable Client QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache with durable client properties Programmatically.
 * 2. Create the example generic Region programmatically.
 * 3. Set DurableCacheListener with "AfterRegionLive" implementation to region.
 * 4. Register Interest to region with durable option.
 * 5. call to readyForEvent().
 * 6. Close the Cache with keepalive options as true.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The DurableClient QuickStart example.
  class DurableClientExample
  {
    public void RunDurableClient()
    {
        // Create durable client's properties using api.
      Properties<string, string> durableProp = Properties<string, string>.Create<string, string>();
        durableProp.Insert("durable-client-id", "DurableClientId");
        durableProp.Insert("durable-timeout", "300");

        // Create a Gemfire Cache programmatically.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(durableProp);

        Cache cache = cacheFactory.SetSubscriptionEnabled(true)
                                  .Create();

        Console.WriteLine("Created the GemFire Cache");
             
        // Create the example Region programmatically.
        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        IRegion<string, string> region = regionFactory.Create<string, string>("exampleRegion");

        Console.WriteLine("Created the generic Region programmatically.");            

        // Plugin the CacheListener with afterRegionLive. "afterRegionLive()"  will be called 
        // after all the queued events are recieved by client
        AttributesMutator<string, string> attrMutator = region.AttributesMutator;
        attrMutator.SetCacheListener(new DurableCacheListener<string, string>());

        Console.WriteLine("DurableCacheListener set to region.");
        
        // For durable Clients, Register Intrest can be durable or non durable ( default ), 
        // Unregister Interest APIs remain same.

        string [] keys = new string[] { "Key-1" };
        region.GetSubscriptionService().RegisterKeys(keys, true, true);

        Console.WriteLine("Called Register Interest for Key-1 with isDurable as true");

        //Send ready for Event message to Server( only for Durable Clients ). 
        //Server will send queued events to client after recieving this.
        cache.ReadyForEvents();
    	
        Console.WriteLine("Sent ReadyForEvents message to server");

        //wait for some time to recieve events
        System.Threading.Thread.Sleep(1000);

        // Close the GemFire Cache with keepalive = true.  Server will queue events for
        // durable registered keys and will deliver all events when client will reconnect
        // within timeout period and send "readyForEvents()"
        cache.Close(true);

        Console.WriteLine("Closed the GemFire Cache with keepalive as true");
    }

    public void RunFeeder()
    {
        // Create a GemFire Cache Programmatically.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
        Cache cache = cacheFactory.SetSubscriptionEnabled(true).Create();

        Console.WriteLine("Created the GemFire Cache");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.PROXY);

        // Created the Region Programmatically.
        IRegion<string, string> region = regionFactory.Create<string, string>("exampleRegion");

        Console.WriteLine("Created the Region Programmatically.");

        // create two keys with value
        string key1 = "Key-1";
        string value1 = "Value-1";
        region[key1] = value1;
        string key2 = "Key-2";
        string value2 = "Value-2";
        region[key2] = value2;

        Console.WriteLine("Created Key-1 and Key-2 in region. Durable interest was registered only for Key-1.");

        // Close the GemFire Cache
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

    }
    static void Main(string[] args)
    {
      try
      {
        DurableClientExample ex = new DurableClientExample();
 
        //First Run of Durable Client
        ex.RunDurableClient();

        //Intermediate Feeder, feeding events
        ex.RunFeeder();

        //Reconnect Durable Client
        ex.RunDurableClient();

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("DurableClient GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

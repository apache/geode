/*
 * The HA QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Connect to a GemFire Distributed System which has two cache servers.
 * 2. Create a GemFire Cache with redundancy level = 1.
 * 3. Get the example generic Region from the Cache.
 * 4. Call registerKeys() on the Region.
 * 5. Call registerRegex() on the Region.
 * 6. Put two keys in the Region.
 * 7. Verify that the keys are destroyed via expiration in server. 
 * 8. Close the Cache.
 *
 */

// Use standard namespaces
using System;
using System.Threading;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The HA QuickStart example.
  class HA
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache.
        GemStone.GemFire.Cache.Generic.CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientHACache.xml")
                  .AddServer("localhost", 40404)
                  .AddServer("localhost", 40405)
                  .SetSubscriptionRedundancy(1)
                  .SetSubscriptionEnabled(true)
                  .Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<object, int> region = cache.GetRegion<object, int>("/exampleRegion");

        Console.WriteLine("Obtained the generic Region from the Cache");

        // Register and Unregister Interest on Region for Some Keys.
        object [] keys = new object[] { 123, "Key-123" };
        region.GetSubscriptionService().RegisterKeys(keys);
        region.GetSubscriptionService().RegisterRegex("Keys.*");

        Console.WriteLine("Called RegisterKeys() and RegisterRegex()");

        region[123] = 1;
        region["Key-123"] = 2;
        
        Console.WriteLine("Called put() on Region");
        
        Console.WriteLine("Waiting for updates on keys");
        Thread.Sleep(10000);
        
        int count = 0;

        //try to get the entries for keys destroyed by server.
        try
        {
          int value1 = region[123];
          Console.WriteLine("UNEXPECTED: First get should not have succeeded");

        }
        catch(KeyNotFoundException){
          Console.WriteLine("gfex.Message: Verified that key1 has been destroyed");
          count++;
        }

        try
        {
          int value2 = region["Key-123"];
          Console.WriteLine("UNEXPECTED: Second get should not have succeeded");
        }
        catch (KeyNotFoundException)
        {
          Console.WriteLine("gfex.Message: Verified that key2 has been destroyed");
          count++;
        }

        if (count == 2) {
          Console.WriteLine("Verified all updates");
        }
        else {
          Console.WriteLine("Could not verify all updates");
        }

        region.GetSubscriptionService().UnregisterKeys(keys);
        region.GetSubscriptionService().UnregisterRegex("Keys.*");
    
        Console.WriteLine("Unregistered keys");
            
        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("HACache GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

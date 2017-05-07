/*
 * The PoolWithEndpoints QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create CacheFactory using the settings from the gfcpp.properties file by default.
 * 2. Create a GemFire Cache.
 * 3. Create Poolfactory with endpoint and then create pool using poolfactory.
 * 4. Create a Example Region programmatically.
 * 5. Put Entries (Key and Value pairs) into the Region.
 * 6. Get Entries from the Region.
 * 7. Invalidate an Entry in the Region.
 * 8. Destroy an Entry in the Region.
 * 9. Close the Cache.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The PoolWithEndpoints QuickStart example.
  class PoolWithEndpoints
  {
    static void Main(string[] args)
    {
      try
      {
        // Create CacheFactory using the settings from the gfcpp.properties file by default.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Console.WriteLine("Created CacheFactory");

        // Create a GemFire Cache.
        Cache cache = cacheFactory.SetSubscriptionEnabled(true).Create();

        Console.WriteLine("Created the GemFire Cache");

        //Create Poolfactory with endpoint and then create pool using poolfactory.
        PoolFactory pfact = PoolManager.CreateFactory();
        Pool pptr = pfact.AddServer("localhost", 40404).Create("examplePool");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        Console.WriteLine("Created the Regionfactory");

        // Create the example Region programmatically.
        IRegion<string, string> region = regionFactory.SetPoolName("examplePool").Create<string, string>("exampleRegion");

        Console.WriteLine("Created the Region Programmatically");

        // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
        region["Key1"] = "Value1";

        Console.WriteLine("Put the first Entry into the Region");

        // Put an Entry into the Region by manually creating a Key and a Value pair.
        string key = "key-123";
        string value = "val-123";
        region[key] = value;

        Console.WriteLine("Put the second Entry into the Region");

        // Get Entries back out of the Region.
        string result1 = region["Key1"];

        Console.WriteLine("Obtained the first Entry from the Region");

        string result2 = region[key];

        Console.WriteLine("Obtained the second Entry from the Region");

        // Invalidate an Entry in the Region.
        region.Invalidate("Key1");

        Console.WriteLine("Invalidated the first Entry in the Region");

        // Destroy an Entry in the Region.
        region.Remove(key);

        Console.WriteLine("Destroyed the second Entry in the Region");

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("PoolWithEndpoints GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

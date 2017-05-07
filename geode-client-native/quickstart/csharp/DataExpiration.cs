/*
 * The DataExpiration QuickStart Example.
 *
 * This example takes the following steps:
 *
 *  1. Create a GemFire Cache programmatically.
 *  2. Create the example Region with generics support programmatically.
 *  3. Set the generic SimpleCacheListener plugin on the Region.
 *  4. Put 3 Entries into the Region.
 *  5. Get the Entry Idle Timeout setting from the Region.
 *  6. Count the Keys in the Region before the Timeout duration elapses.
 *  7. Wait for the Timeout Expiration Action to be reported by the SimpleCacheListener.
 *  8. Count the remaining Keys in the Region after the Timeout duration elapses.
 *  9. Close the Cache.
 *
 */

// Use standard namespaces
using System;
using System.Threading;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

// Use the .NET generics namespace
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The DataExpiration QuickStart example.
  class DataExpiration
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache Programmatically.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
        Cache cache = cacheFactory.SetSubscriptionEnabled(true)
                                  .Create();

        Console.WriteLine("Created the GemFire Cache");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
        // Create the example Region programmatically.
        IRegion<string, string> region = regionFactory
          .SetEntryIdleTimeout(ExpirationAction.Destroy, 10)          
          .Create<string, string>("exampleRegion");

        Console.WriteLine("Created the Region with generics support programmatically.");

        // Plugin the SimpleCacheListener to the Region.
        AttributesMutator<string, string> attrMutator = region.AttributesMutator;
        attrMutator.SetCacheListener(new SimpleCacheListener<string, string>());

        Console.WriteLine("Set the generic SimpleCacheListener on the Region");

        // Put 3 Entries into the Region using the IDictionary interface.
        region["Key1"] = "Value1";
        region["Key2"] = "Value2";
        region["Key3"] = "Value3";

        Console.WriteLine("Put 3 Entries into the Region");

        // Get the Entry Idle Timeout specified in the Cache XML file.
        int entryIdleTimeout = region.Attributes.EntryIdleTimeout;

        Console.WriteLine("Got Entry Idle Timeout as {0} seconds", entryIdleTimeout);

        // Wait for half the Entry Idle Timeout duration.
        Thread.Sleep(entryIdleTimeout * 1000 / 2);

        // Get the number of Keys remaining in the Region, should be all 3.
        ICollection<string> keys = region.GetLocalView().Keys;

        Console.WriteLine("Got {0} keys before the Entry Idle Timeout duration elapsed", keys.Count);

        // Get 2 of the 3 Entries from the Region to "reset" their Idle Timeouts.
        string value1 = region["Key1"];
        string value2 = region["Key2"];

        Console.WriteLine("The SimpleCacheListener should next report the expiration action");

        // Wait for the entire Entry Idle Timeout duration.
        Thread.Sleep(entryIdleTimeout * 1000);

        // Get the number of Keys remaining in the Region, should be 0 now.
        keys = region.GetLocalView().Keys;

        Console.WriteLine("Got {0} keys after the Entry Idle Timeout duration elapsed", keys.Count);

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("DataExpiration GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

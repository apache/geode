/*
 * The LoaderListenerWriter QuickStart Example.
 *
 * This example takes the following steps:
 *
 *  1. Connect to a GemFire Distributed System.
 *  2. Create a GemFire Cache.
 *  3. Get the generic example Region from the Cache.
 *  4. Set the generic SimpleCacheLoader, SimpleCacheListener and SimpleCacheWriter plugins on the Region.
 *  5. Put 3 Entries into the Region.
 *  6. Update an Entry in the Region.
 *  7. Destroy an Entry in the Region.
 *  8. Invalidate an Entry in the Region.
 *  9. Get a new Entry from the Region.
 * 10. Get the destroyed Entry from the Region.
 * 11. Close the Cache.
 * 12. Disconnect from the Distributed System.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The LoaderListenerWriter QuickStart example.
  class LoaderListenerWriter
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientLoaderListenerWriter.xml")
                                  .SetSubscriptionEnabled(true)
                                  .Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, string> region = cache.GetRegion<string, string>("/exampleRegion");

        Console.WriteLine("Obtained the generic Region from the Cache");

        // Plugin the SimpleCacheLoader, SimpleCacheListener and SimpleCacheWrite to the Region.
        AttributesMutator<string, string> attrMutator = region.AttributesMutator;
        attrMutator.SetCacheLoader(new SimpleCacheLoader<string, string>());
        attrMutator.SetCacheListener(new SimpleCacheListener<string, string>());
        attrMutator.SetCacheWriter(new SimpleCacheWriter<string, string>());

        Console.WriteLine("Attached the simple generic plugins on the Region");

        // The following operations should cause the plugins to print the events.

        // Put 3 Entries into the Region using the IDictionary interface.
        region["Key1"] = "Value1";
        region["Key2"] = "Value2";
        region["Key3"] = "Value3";

        Console.WriteLine("Put 3 Entries into the Region");

        // Update Key3.
        region["Key3"] = "Value3-Updated";

        // Destroy Key3 using the IDictionary interface.
        region.Remove("Key3");

        // Invalidate Key2.
        region.Invalidate("Key2");

        string value = null;
        
        try
        {
          // Get a new Key.
          value = region["Key4"];
        }
        catch (KeyNotFoundException knfex)
        {
          Console.WriteLine("Got expected KeyNotFoundException: {0}", knfex.Message);
        }
        
        try
        {
          // Get a destroyed Key.
          value = region["Key3"];
        }
        catch (KeyNotFoundException knfex)
        {
          Console.WriteLine("Got expected KeyNotFoundException: {0}", knfex.Message);
        }

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("LoaderListenerWriter GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

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

// Use standard namespaces
using System;
using System.Threading;

// Use the GemFire namespace
using GemStone.GemFire.Cache;

namespace GemStone.GemFire.Cache.QuickStart
{
  // The Interop QuickStart example.
  class Interop
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientInterop.xml").Create();

        Console.WriteLine("CSHARP CLIENT: Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        Region region = cache.GetRegion("exampleRegion");

        Console.WriteLine("CSHARP CLIENT: Obtained the Region from the Cache");

        // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
        region.Put("Key-CSHARP", "Value-CSHARP");

        Console.WriteLine("CSHARP CLIENT: Put the C# Entry into the Region");

        // Wait for all values to be available.
        IGFSerializable value1 = null;
        IGFSerializable value2 = null;
        IGFSerializable value3 = null;
        
        while (value1 == null || value2 == null || value3 == null)
        {
          Console.WriteLine("CSHARP CLIENT: Checking server for keys...");
          value1 = region.Get("Key-CPP");
          value2 = region.Get("Key-CSHARP");
          value3 = region.Get("Key-JAVA");
          Thread.Sleep(1000);
        }
        
        Console.WriteLine("CSHARP CLIENT: Key-CPP value is {0}", value1);
        Console.WriteLine("CSHARP CLIENT: Key-CSHARP value is {0}", value2);
        Console.WriteLine("CSHARP CLIENT: Key-JAVA value is {0}", value3);
        
        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("CSHARP CLIENT: Closed the GemFire Cache");
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("CSHARP CLIENT: Interop GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

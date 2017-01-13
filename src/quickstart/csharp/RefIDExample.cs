/*
 * The RefIDExample QuickStart Example.
 * This example creates two pools through XML and sets region attributes using refid.
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Now it creates 2 Pools with the names poolName1, poolName2 respectively.
 * 3. Sets the region attribute using refid.
 * 4. Gets the region "root1" with poolName1, and region "root2" with poolName2.
 * 5. Check for the region attribute set through refid.
 * 6. Put Entries (Key and Value pairs) into both the Regions.
 * 7. Get Entries from the Regions.
 * 8. Invalidate an Entry in both the Regions.
 * 9. Destroy an Entry in both the Regions.
 * 10. Close the Cache.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The RefIDExample QuickStart example.
  class RefIDExample
  {
    static void Main(string[] args)
    {
      try
      {
        Properties<string, string> prop = Properties<string, string>.Create<string, string>();
        prop.Insert("cache-xml-file", "XMLs/clientRefIDExample.xml");
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prop);
        Cache cache = cacheFactory.Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the Regions from the Cache which is declared in the Cache XML file.
        IRegion<string, string> region1 = cache.GetRegion<string, string>("root1");

        Console.WriteLine("Obtained the root1 Region from the Cache");

        IRegion<string, string> region2 = cache.GetRegion<string, string>("root2");

        Console.WriteLine("Obtained the root2 Region from the Cache");

        Console.WriteLine("For region root1 cachingEnabled is {0} ", region1.Attributes.CachingEnabled);

        Console.WriteLine("For region root2 cachingEnabled is {0} ", region2.Attributes.CachingEnabled);

        // Put an Entry (Key and Value pair) into the Region using the direct/shortcut method.
        region1["Key1"] = "Value1";
        region2["Key1"] = "Value1";

        Console.WriteLine("Put the first Entries into both the Regions");

        // Put an Entry into the Region by manually creating a Key and a Value pair.
        string key = "123";
        string value = "123";
        region1[key] = value;
        region2[key] = value;

        Console.WriteLine("Put the second Entries into both the Regions.");

        // Get Entries back out of the Region.
        string result1 = region1["Key1"];
        string result2 = region2["Key1"];

        Console.WriteLine("Obtained the first Entry from both the Regions");

        result1 = region1[key];
        result2 = region2[key];

        Console.WriteLine("Obtained the second Entry from both the Regions");

        // Invalidate an Entry in the Region.
        region1.Invalidate("Key1");
        region2.Invalidate("Key1");

        Console.WriteLine("Invalidated the first Entry in both the Regions.");

        // Destroy an Entry in the Region.
        region1.Remove(key);
        region2.Remove(key);

        Console.WriteLine("Destroyed the second Entry in both the Regions");

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");        
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("RefIDExample GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

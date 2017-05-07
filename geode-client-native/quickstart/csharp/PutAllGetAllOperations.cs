/*
 * The PutAllGetAllOperations QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache using CacheFactory. By default it will connect to "localhost" at port 40404".
 * 2. Create a Example Region.
 * 3. PutAll Entries (Key and Value pairs) into the Region.
 * 4. GetAll Entries from the Region.
 * 5. Close the Cache.
 *
 */

// Use standard namespaces
using System;
using System.Collections.Generic;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The PutAllGetAllOperations QuickStart example.
  class PutAllGetAllOperations
  {
    static void Main(string[] args)
    {
      try
      {
        //Create a GemFire Cache using CacheFactory. By default it will connect to "localhost" at port 40404".
        Cache cache = CacheFactory.CreateCacheFactory().Create();

        Console.WriteLine("Created the GemFire Cache");

        //Set Attributes for the region.
        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        //Create exampleRegion
        IRegion<int, string> region = regionFactory.Create<int, string>("exampleRegion");
        
        Console.WriteLine("Created exampleRegion");

        // PutAll Entries (Key and Value pairs) into the Region.
        Dictionary<int, string> entryMap = new Dictionary<int, string>();
        for (Int32 item = 0; item < 100; item++)
        {
          int key = item;
          string value = item.ToString();
          entryMap.Add(key, value);
        }
        region.PutAll(entryMap);
        Console.WriteLine("PutAll 100 entries into the Region");
        
        //GetAll Entries back out of the Region
        List<int> keys  = new List<int>();
        for (int item = 0; item < 100; item++)
        {
          int key = item;
          keys.Add(key);
        }
        Dictionary<int, string> values = new Dictionary<int, string>();
        region.GetAll(keys.ToArray(), values, null, true);

        Console.WriteLine("Obtained 100 entries from the Region");

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("PutAllGetAllOperations GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

/*
 * The Exceptions QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create CacheFactory using the user specified settings or from the gfcpp.properties file by default.
 * 2. Create a GemFire Cache.
 * 3. Get the example generic Regions from the Cache.
 * 4. Perform some operations which should cause exceptions.
 * 5. Close the Cache.
 * 6. Put an Entry into the Region when Cache is already closed.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The Exceptions QuickStart example.
  class Exceptions
  {
    static void Main(string[] args)
    {
      try
      {
        // Create CacheFactory using the user specified settings or from the gfcpp.properties file by default.
        Properties<string, string> prp = Properties<string, string>.Create<string, string>();
        prp.Insert("cache-xml-file", "XMLs/clientExceptions.xml");

        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prp);

        Console.WriteLine("Created CacheFactory");

        // Create a GemFire Cache with the "clientExceptions.xml" Cache XML file.
        Cache cache = cacheFactory.SetSubscriptionEnabled(true).Create();

        Console.WriteLine("Created the GemFire Cache");
        
        // Get the example Regions from the Cache which are declared in the Cache XML file.
        IRegion<object, string> region = cache.GetRegion<object, string>("exampleRegion");
        IRegion<object, string> region2 = cache.GetRegion<object, string>("exampleRegion2");

        Console.WriteLine("Obtained the generic Region from the Cache");

        // Put an Entry (Key and Value pair) into the Region using the IDictionary interface.
        region["Key1"] = "Value1";

        Console.WriteLine("Put the first Entry into the Region");

        // Put an Entry into the Region by manually creating a Key and a Value pair.
        int key = 123;
        string value = "123";
        region[key] = value;

        Console.WriteLine("Put the second Entry into the Region");

        // Get Entries back out of the Region.
        string result1 = region["Key1"];

        Console.WriteLine("Obtained the first Entry from the Region");

        string result2 = region[key];

        Console.WriteLine("Obtained the second Entry from the Region");

        //Destroy exampleRegion2.
        object userData = null;
        region2.DestroyRegion(userData);
        
        try
        {
          // Try to Put an Entry into a destroyed Region.
          region2["Key1"] = "Value1";
  
          Console.WriteLine("UNEXPECTED: Put should not have succeeded");
        }
        catch (RegionDestroyedException gfex)
        {
          Console.WriteLine("Expected RegionDestroyedException: {0}", gfex.Message);
        }
        
        try
        {
          //Its not valid to create two instance of Cache with different settings.
          //If the settings are the same it returns the existing Cache instance.
          CacheFactory cacheFactory2 = CacheFactory.CreateCacheFactory(prp);
          Cache cache1 = cacheFactory2.SetSubscriptionEnabled(true).AddServer("localhost", 40405).Create();

          Console.WriteLine("UNEXPECTED: Cache create should not have succeeded");
        }
        catch (IllegalStateException gfex)
        {
          Console.WriteLine("Expected IllegalStateException: {0}", gfex.Message);
        }
        
        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

        try
        {
          // Put an Entry into the Region when Cache is already closed.
          region["Key1"] = "Value1";
  
          Console.WriteLine("UNEXPECTED: Put should not have succeeded");
        }
        catch (RegionDestroyedException gfex)
        {
          Console.WriteLine("Expected RegionDestroyedException: {0}", gfex.Message);
        }
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("Exceptions GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

/*
 * The Delta QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Cache.
 * 3. Put an Entry into the Region.
 * 4. Set delta for a value.
 * 5. Put entry with delta in region.
 * 6. Local-invalidate entry in region.
 * 7. Get entry from server.
 * 8. Verify that delta was applied on server, by examining entry.
 * 9. Close the Cache.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The Delta QuickStart example.
  class Delta
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache through XMLs/clientDelta.xml
        Properties<string, string> prop = Properties<string, string>.Create<string, string>();
        prop.Insert("cache-xml-file", "XMLs/clientDelta.xml");
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prop);
        Cache cache = cacheFactory.Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, DeltaExample> region = cache.GetRegion<string, DeltaExample>("exampleRegion");

        Console.WriteLine("Obtained the Region from the Cache");

        Serializable.RegisterTypeGeneric(DeltaExample.create);

        //Creating Delta Object.
        DeltaExample ptr = new DeltaExample(10, 15, 20);
        
        //Put the delta object. This will send the complete object to the server.
        region["Key1"] = ptr;
        
        Console.WriteLine("Completed put for a delta object");


        //Changing state of delta object.
        ptr.setField1(9);

        //Put delta object again. Since delta flag is set true it will calculate
        //Delta and send only Delta to the server.
        region["Key1"] = ptr;
        Console.WriteLine("Completed put with delta");

        //Locally invalidating the key.
        region.GetLocalView().Invalidate("Key1");
        //Fetching the value from server.
        DeltaExample retVal = (DeltaExample) region["Key1"];

        //Verification
        if( retVal.getField1() != 9 )
          throw new GemFireException("First field should have been 9");
        if( retVal.getField2() != 15 )
          throw new GemFireException("Second field should have been 15");
        if( retVal.getField3() != 20 )
          throw new GemFireException("Third field should have been 20");
        
        Console.WriteLine("Delta has been successfully applied at server");

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("Delta GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

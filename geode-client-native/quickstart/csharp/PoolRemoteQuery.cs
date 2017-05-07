/*
 * The PoolRemoteQuery QuickStart Example.
 * This examples creates pool using locator.
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Cache.
 * 3. Populate some query objects on the Region.
 * 4. Get the pool, get the Query Service from Pool. Pool is define in clientRemoteQueryWithPool.xml. Pool has locator to get the server. Apart from that pool is bind to server group "ServerGroup1".
 * 5. Execute a query that returns a Result Set.
 * 6. Execute a query that returns a Struct Set.
 * 7. Execute the region shortcut/convenience query methods.
 * 8. Close the Cache.
 *
 */
// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;

// Use the "Tests" namespace for the query objects.
using GemStone.GemFire.Cache.Tests.NewAPI;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The PoolRemoteQuery QuickStart example.
  class PoolRemoteQuery
  {
    static void Main(string[] args)
    {
      try
      {

        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
       
        Console.WriteLine("Connected to the GemFire Distributed System");

        // Create a GemFire Cache with the "clientPoolRemoteQuery.xml" Cache XML file.
        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientPoolRemoteQuery.xml").Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, Portfolio> region = cache.GetRegion<string, Portfolio>("Portfolios");

        Console.WriteLine("Obtained the Region from the Cache");
        
        // Register our Serializable/Cacheable Query objects, viz. Portfolio and Position.
        Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
        Serializable.RegisterTypeGeneric(Position.CreateDeserializable);

        Console.WriteLine("Registered Serializable Query Objects");

        // Populate the Region with some Portfolio objects.
        Portfolio port1 = new Portfolio(1 /*ID*/, 10 /*size*/);
        Portfolio port2 = new Portfolio(2 /*ID*/, 20 /*size*/);
        Portfolio port3 = new Portfolio(3 /*ID*/, 30 /*size*/);
        region["Key1"] = port1;
        region["Key2"] = port2;
        region["Key3"] = port3;

        Console.WriteLine("Populated some Portfolio Objects");

        //find the pool
        Pool pool = PoolManager.Find("examplePool");

        // Get the QueryService from the pool
        QueryService<string, Portfolio> qrySvc = pool.GetQueryService<string, Portfolio>();

        Console.WriteLine("Got the QueryService from the Pool");

        // Execute a Query which returns a ResultSet.    
        Query<Portfolio> qry = qrySvc.NewQuery("SELECT DISTINCT * FROM /Portfolios");
        ISelectResults<Portfolio> results = qry.Execute();

        Console.WriteLine("ResultSet Query returned {0} rows", results.Size);

        // Execute a Query which returns a StructSet.
        QueryService<string, Struct> qrySvc1 = pool.GetQueryService<string, Struct>();
        Query<Struct> qry1 = qrySvc1.NewQuery("SELECT DISTINCT ID, status FROM /Portfolios WHERE ID > 1");
        ISelectResults<Struct> results1 = qry1.Execute();

        Console.WriteLine("StructSet Query returned {0} rows", results1.Size);

        // Iterate through the rows of the query result.
        int rowCount = 0;
        foreach (Struct si in results1)
        {
          rowCount++;
          Console.WriteLine("Row {0} Column 1 is named {1}, value is {2}", rowCount, si.Set.GetFieldName(0), si[0].ToString());
          Console.WriteLine("Row {0} Column 2 is named {1}, value is {2}", rowCount, si.Set.GetFieldName(1), si[1].ToString());
        }

        // Execute a Region Shortcut Query (convenience method).
        results = region.Query<Portfolio>("ID = 2");

        Console.WriteLine("Region Query returned {0} rows", results.Size);

        // Execute the Region selectValue() API.
        object result = region.SelectValue("ID = 3");

        Console.WriteLine("Region selectValue() returned an item:\n {0}", result.ToString());

        // Execute the Region existsValue() API.
        bool existsValue = region.ExistsValue("ID = 4");

        Console.WriteLine("Region existsValue() returned {0}", existsValue ? "true" : "false");

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("PoolRemoteQuery GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

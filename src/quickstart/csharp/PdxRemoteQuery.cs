/*
 * The PdxRemoteQuery QuickStart Example.
 * This example takes the following steps:
 *
 * This example shows IPdxSerializable usage with remote query. It can query .NET objects without having corresponding java classes at server.
 * Look PortfolioPdx.cs and PositionPdx.cs to know more.
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Cache.
 * 3. Populate some query Pdx objects on the Region.
 * 4. Get the pool, get the Query Service from Pool. Pool is define in clientPdxRemoteQuery.xml. 
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
using PdxTests;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The PdxRemoteQuery QuickStart example.
  class PdxRemoteQuery
  {
    static void Main(string[] args)
    {
      try
      {

        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Console.WriteLine("Connected to the GemFire Distributed System");

        // Create a GemFire Cache with the "clientPdxRemoteQuery.xml" Cache XML file.
        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientPdxRemoteQuery.xml").Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, PortfolioPdx> region = cache.GetRegion<string, PortfolioPdx>("Portfolios");

        Console.WriteLine("Obtained the Region from the Cache");

        // Register our Serializable/Cacheable Query objects, viz. Portfolio and Position.
        Serializable.RegisterPdxType(PortfolioPdx.CreateDeserializable);
        Serializable.RegisterPdxType(PositionPdx.CreateDeserializable);

        Console.WriteLine("Registered Serializable Query Objects");

        // Populate the Region with some PortfolioPdx objects.
        PortfolioPdx port1 = new PortfolioPdx(1 /*ID*/, 10 /*size*/);
        PortfolioPdx port2 = new PortfolioPdx(2 /*ID*/, 20 /*size*/);
        PortfolioPdx port3 = new PortfolioPdx(3 /*ID*/, 30 /*size*/);
        region["Key1"] = port1;
        region["Key2"] = port2;
        region["Key3"] = port3;

        Console.WriteLine("Populated some PortfolioPdx Objects");

        //find the pool
        Pool pool = PoolManager.Find("examplePool");

        // Get the QueryService from the pool
        QueryService<string, PortfolioPdx> qrySvc = pool.GetQueryService<string, PortfolioPdx>();

        Console.WriteLine("Got the QueryService from the Pool");

        // Execute a Query which returns a ResultSet.    
        Query<PortfolioPdx> qry = qrySvc.NewQuery("SELECT DISTINCT * FROM /Portfolios");
        ISelectResults<PortfolioPdx> results = qry.Execute();

        Console.WriteLine("ResultSet Query returned {0} rows", results.Size);

        // Execute a Query which returns a StructSet.
        QueryService<string, Struct> qrySvc1 = pool.GetQueryService<string, Struct>();
        Query<Struct> qry1 = qrySvc1.NewQuery("SELECT DISTINCT id, status FROM /Portfolios WHERE id > 1");
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
        results = region.Query<PortfolioPdx>("id = 2");

        Console.WriteLine("Region Query returned {0} rows", results.Size);

        // Execute the Region selectValue() API.
        object result = region.SelectValue("id = 3");

        Console.WriteLine("Region selectValue() returned an item:\n {0}", result.ToString());

        // Execute the Region existsValue() API.
        bool existsValue = region.ExistsValue("id = 4");

        Console.WriteLine("Region existsValue() returned {0}", existsValue ? "true" : "false");

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("PdxRemoteQuery GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

/*
 * The RemoteQuery QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache Programmatically.
 * 2. Create the example Region Programmatically.
 * 3. Populate some query objects on the Region.
 * 4. Execute a query that returns a Result Set.
 * 5. Execute a query that returns a Struct Set.
 * 6. Execute the region shortcut/convenience query methods.
 * 7. Close the Cache.
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
  // The RemoteQuery QuickStart example.
  class RemoteQuery
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache Programmatically.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
        Cache cache = cacheFactory.SetSubscriptionEnabled(true).Create();

        Console.WriteLine("Created the GemFire Cache");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);

        // Create the example Region programmatically.
        IRegion<string, Portfolio> region = regionFactory.Create<string, Portfolio>("Portfolios");

        Console.WriteLine("Created the Region Programmatically.");    

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

        // Get the QueryService from the Cache.
        QueryService<string, Portfolio> qrySvc = cache.GetQueryService<string, Portfolio>();

        Console.WriteLine("Got the QueryService from the Cache");

        // Execute a Query which returns a ResultSet.    
        Query<Portfolio> qry = qrySvc.NewQuery("SELECT DISTINCT * FROM /Portfolios");
        ISelectResults<Portfolio> results = qry.Execute();

        Console.WriteLine("ResultSet Query returned {0} rows", results.Size);

        // Execute a Query which returns a StructSet.
        QueryService<string, Struct> qrySvc1 = cache.GetQueryService<string, Struct>();
        Query<Struct> qry1 = qrySvc1.NewQuery("SELECT DISTINCT ID, status FROM /Portfolios WHERE ID > 1");
        ISelectResults<Struct> results1 = qry1.Execute();

        Console.WriteLine("StructSet Query returned {0} rows", results1.Size);

        // Iterate through the rows of the query result.
        int rowCount = 0;
        foreach (Struct si in results1)
        {
          rowCount++;
          Console.WriteLine("Row {0} Column 1 is named {1}, value is {2}", rowCount, si.Set.GetFieldName(0), si[0].ToString());
          Console.WriteLine("Row {0} Column 2 is named {1}, value is {2}", rowCount, si.Set.GetFieldName(0), si[1].ToString());
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

        //Execute the parameterized query
        //Populate the parameter list (paramList) for the query.
        //TODO:remove once query service is generic
        QueryService<string, Struct> pqrySvc = cache.GetQueryService<string, Struct>();

        Query<Struct> pquery = pqrySvc.NewQuery("SELECT DISTINCT ID, status FROM /Portfolios WHERE ID > $1 and status=$2");

        object[] paramList = new object[2];
        paramList[0] = 1; //param-1
        paramList[1] = "active"; //param-2

        ISelectResults<Struct> pqresults = pquery.Execute(paramList);

        Console.WriteLine("Parameterized Query returned {0} rows", pqresults.Size);

        // Iterate through the rows of the query result.
        rowCount = 0;
        foreach (Struct st in pqresults)
        {
          rowCount++;
          Console.WriteLine("Row {0} Column 1 is named {1}, value is {2}", rowCount, st.Set.GetFieldName(0), st[0].ToString());
          Console.WriteLine("Row {0} Column 2 is named {1}, value is {2}", rowCount, st.Set.GetFieldName(0), st[1].ToString());
        }
        
        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");
        
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("RemoteQuery GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

/*
 * The ExecuteFunction QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Cache.
 * 3. Populate some query objects on the Region.
 * 4. Create Execute Objects
 * 5. Execute Functions
 * 6. Close the Cache.
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;
using System.Collections.Generic;
using System.Collections;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  // The Function Execution QuickStart example.

  class ExecuteFunctions
  {
    private static string getFuncName = "MultiGetFunction";
    private static string getFuncIName = "MultiGetFunctionI";

    static void Main(string[] args)
    {
      try
      {
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.SetSubscriptionEnabled(true).AddServer("localhost", 50505).AddServer("localhost", 40404).Create();

        Console.WriteLine("Created the GemFire Cache");
        IRegion<string, string> region = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY).Create<string, string>("partition_region");
        Console.WriteLine("Created the Region");

        region.GetSubscriptionService().RegisterAllKeys();

        for (int i = 0; i < 34; i++)
        {
          region["KEY--" + i] = "VALUE--" + i;
        }

        object[] routingObj = new object[17];
        int j = 0;
        for (int i = 0; i < 34; i++)
        {
          if (i % 2 == 0) continue;
          routingObj[j] = "KEY--" + i;
          j++;
        }
        Console.WriteLine("routingObj count= {0}.", routingObj.Length);

        bool args0 = true;
        //test data dependant function execution
        //test get function with result
        Execution<object> exc = FunctionService<object>.OnRegion<string, string>(region);
        IResultCollector<object> rc = exc.WithArgs<bool>(args0).WithFilter<object>(routingObj).Execute(getFuncName);
        ICollection<object> executeFunctionResult = rc.GetResult();

        List<object> resultList = new List<object>();
        foreach (List<object> item in executeFunctionResult)
        {

          foreach (object subitem in item)
          {
            resultList.Add(subitem);
          }
        }

        Console.WriteLine("on region: result count= {0}.", resultList.Count);
        for (int i = 0; i < resultList.Count; i++)
        {
          Console.WriteLine("on region:get:result[{0}]={1}.", i, (string)resultList[i]);
        }

        //test date independant fucntion execution on one server
        //test get function with result
        exc = FunctionService<object>.OnServer(cache);
        ArrayList args1 = new ArrayList();
        for (int i = 0; i < routingObj.Length; i++)
        {
          Console.WriteLine("routingObj[{0}]={1}.", i, (string)routingObj[i]);
          args1.Add(routingObj[i]);
        }
        rc = exc.WithArgs<ArrayList>(args1).Execute(getFuncIName);
        executeFunctionResult = rc.GetResult();
        Console.WriteLine("on one server: result count= {0}.", executeFunctionResult.Count);
        List<object> resultList1 = new List<object>();

        foreach (List<object> item in executeFunctionResult)
        {
          foreach (object subitem in item)
          {
            resultList1.Add(subitem);
          }
        }
        if (resultList1.Count != 17)
          Console.WriteLine("result count check failed on one server:get:");
        for (int i = 0; i < resultList1.Count; i++)
        {
          Console.WriteLine("on one server:get:result[{0}]={1}.", i, (string)resultList1[i]);
        }

        //test date independant fucntion execution on all servers
        //test get function with result
        exc = FunctionService<object>.OnServers(cache);
        rc = exc.WithArgs<ArrayList>(args1).Execute(getFuncIName);
        executeFunctionResult = rc.GetResult();
        Console.WriteLine("on all servers: result count= {0}.", executeFunctionResult.Count);

        List<object> resultList2 = new List<object>();

        foreach (List<object> item in executeFunctionResult)
        {
          foreach (object subitem in item)
          {
            resultList2.Add(subitem);
          }
        }

        if (resultList2.Count != 34)
          Console.WriteLine("result count check failed on all servers");
        for (int i = 0; i < resultList2.Count; i++)
        {
          Console.WriteLine("on all servers:result[{0}]={1}.", i, (string)resultList2[i]);
        }

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");
      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("ExecuteFunctions GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

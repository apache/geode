/*
 * The ExecuteFunction Example.
 */

// Use standard namespaces
using System;
using System.Threading;

// Use the GemFire namespace
using GemStone.GemFire.Cache;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.Examples
{
  // The Function Execution example.
  //customer result collector
  public class MyResultCollector : IResultCollector
  {
    private bool m_resultReady = false;
    private CacheableVector m_results = null;
    private int m_addResultCount = 0;
    private int m_getResultCount = 0;
    private int m_endResultCount = 0;

    public int GetAddResultCount()
    {
      return m_addResultCount ;
    }
    public int GetGetResultCount()
    {
      return m_getResultCount ;
    }
    public int GetEndResultCount()
    {
      return m_endResultCount ;
    }
    public MyResultCollector()
    {
      m_results = new CacheableVector();
    }
    public void AddResult(IGFSerializable result)
    {
      m_addResultCount++;
      CacheableArrayList rs = result as CacheableArrayList;
      for(int i = 0; i < rs.Count; i++)
      {
	m_results.Add(rs[i]);
      }
    }
    public IGFSerializable[] GetResult()
    {
      return GetResult(50);
    }
    public IGFSerializable[] GetResult(UInt32 timeout)
    {
      m_getResultCount++;
      if(m_resultReady == true)
      {
	return m_results.ToArray();
      }
      else 
      {
	for(int i=0; i < timeout; i++)
	{
	  Thread.Sleep(1000);
          if(m_resultReady == true)
          {
	    return m_results.ToArray();
          }
	}
	throw new FunctionExecutionException(
	           "Result is not ready, endResults callback is called before invoking getResult() method");
	
      }
    }
    public void EndResults()
    {
       m_endResultCount++;
       m_resultReady = true;
    }
    public void ClearResults()
    {
      m_results.Clear();
    }
  }

  class ExecuteFunctions
  {        
    private static string getFuncName = "MultiGetFunction";
    private static string getFuncIName = "MultiGetFunctionI";

    static void Main(string[] args)
    {
      try
      {
        // Create CacheFactory using the settings from the gfcpp.properties file by default.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Cache cache = cacheFactory.SetSubscriptionEnabled(true)
                                  .AddServer("localhost", 40404)
                                  .AddServer("localhost", 50505)          
                                  .Create();

        Console.WriteLine("Created the GemFire Cache");

        Region region = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY)          
                             .Create("partition_region");

        Console.WriteLine("Created the partition_region.");

        for(int i=0; i < 34; i++)
        {
          region.Put("KEY--"+i, "VALUE--"+i);
        }
      
        IGFSerializable[] routingObj = new IGFSerializable[17];
        int j=0;
        for(int i=0; i < 34; i++)
        {
          if(i%2==0) continue;
          routingObj[j] = new CacheableString("KEY--"+i);
	  j++;
        }
        Console.WriteLine("routingObj count= {0}.", routingObj.Length);

        //test data dependant function execution
        //     test get function with result
        Boolean getResult = true;
        IGFSerializable args0 = new CacheableBoolean(true);
        Execution exc = FunctionService.OnRegion(region);
        IResultCollector rc =  exc.WithArgs(args0).WithFilter(routingObj).Execute(
	  getFuncName, getResult);
        IGFSerializable[] executeFunctionResult = rc.GetResult();
        Console.WriteLine("on region: result count= {0}.", executeFunctionResult.Length);

        List<IGFSerializable> resultList = new List<IGFSerializable>();

        for (int pos = 0; pos < executeFunctionResult.Length; pos++) {
          CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
          foreach (IGFSerializable item in resultItem) {
            resultList.Add(item);
          }
        }
        Console.WriteLine("on region: result count= {0}.", resultList.Count);
        for (int i = 0; i < resultList.Count; i++) {
          Console.WriteLine("on region:get:result[{0}]={1}.", i, (resultList[i] as CacheableString).Value);
        }        
            
        getResult = true;
        //test date independant fucntion execution on one server
        //     test get function with result
        exc = FunctionService.OnServer(cache);
        CacheableVector args1 = new  CacheableVector();
        for(int i=0; i < routingObj.Length; i++)
        {
          Console.WriteLine("routingObj[{0}]={1}.", i, (routingObj[i] as CacheableString).Value);
          args1.Add(routingObj[i]);
        }
        rc =  exc.WithArgs(args1).Execute(
	  getFuncIName, getResult);
        executeFunctionResult = rc.GetResult();
        Console.WriteLine("on one server: result count= {0}.", executeFunctionResult.Length);

        List<IGFSerializable> resultList1 = new List<IGFSerializable>();
        for (int pos = 0; pos < executeFunctionResult.Length; pos++) {
          CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
          foreach (IGFSerializable item in resultItem) {
            resultList1.Add(item);
          }
        }
        
        for (int i = 0; i < resultList1.Count; i++) {
          Console.WriteLine("on one server:get:result[{0}]={1}.", i, (resultList1[i] as CacheableString).Value);
        }        

        //test date independant fucntion execution on all servers
        //     test get function with result
        exc = FunctionService.OnServers(cache);
        rc =  exc.WithArgs(args1).Execute(getFuncIName, getResult);
        executeFunctionResult = rc.GetResult();
        Console.WriteLine("on all servers: result count= {0}.", executeFunctionResult.Length);

        List<IGFSerializable> resultList2 = new List<IGFSerializable>();
        for (int pos = 0; pos < executeFunctionResult.Length; pos++) {
          CacheableArrayList resultItem = executeFunctionResult[pos] as CacheableArrayList;
          foreach (IGFSerializable item in resultItem) {
            resultList2.Add(item);
          }
        }
        if (resultList2.Count != 34)
          Console.WriteLine("result count check failed on all servers");
        for (int i = 0; i < resultList2.Count; i++) {
          Console.WriteLine("on all servers:result[{0}]={1}.", i, (resultList2[i] as CacheableString).Value);
        }       

        //test withCollector
        MyResultCollector myRC = new MyResultCollector();
        rc =  exc.WithArgs(args1).WithCollector(myRC).Execute(getFuncIName, getResult);
        executeFunctionResult = rc.GetResult();
        Console.WriteLine("add result count= {0}.", myRC.GetAddResultCount());
        Console.WriteLine("get result count= {0}.", myRC.GetGetResultCount());
        Console.WriteLine("end result count= {0}.", myRC.GetEndResultCount());
        Console.WriteLine("on all servers with collector: result count= {0}.", executeFunctionResult.Length);

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

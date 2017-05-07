/*
 * The CqQuery QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache Programmatically.
 * 2. Create the example Region Programmatically.
 * 3. Populate some query objects on the Region.
 * 4. Register a cqQuery listener
 * 5. Execute a cqQuery with initial Results 
 * 6. Close the Cache.
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
  // The CqQuery QuickStart example.

  //User Listener
  public class MyCqListener<TKey, TResult> : ICqListener<TKey, TResult>
  {
    public virtual void OnEvent(CqEvent<TKey, TResult> ev)
    {
      Portfolio val = ev.getNewValue() as  Portfolio;
      TKey key = ev.getKey();
      CqOperationType opType = ev.getQueryOperation();
      string opStr = "DESTROY";
      if(opType == CqOperationType.OP_TYPE_CREATE)
         opStr = "CREATE";
      else if(opType == CqOperationType.OP_TYPE_UPDATE)
         opStr = "UPDATE";
      Console.WriteLine("MyCqListener::OnEvent called with key {0}, value ({1},{2}), op {3}.", key, val.ID, val.Pkid,opStr);
    }
    public virtual void OnError(CqEvent<TKey, TResult> ev)
    {
      Console.WriteLine("MyCqListener::OnError called");
    }
    public virtual void Close()
    {
      Console.WriteLine("MyCqListener::close called");
    }
  }
  class ContinuousQuery
  {
    static void Main(string[] args)
    {
      try
      {
        // Create a GemFire Cache Programmatically.
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
        Cache cache = cacheFactory.SetSubscriptionEnabled(true)
                                  .AddServer("localhost", 50505)
                                  .Create();

        Console.WriteLine("Created the GemFire Cache");

        RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.PROXY);
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
        QueryService<string, object> qrySvc = cache.GetQueryService<string, object>();

        Console.WriteLine("Got the QueryService from the Cache");

	//create CqAttributes with listener
        CqAttributesFactory<string, object> cqFac = new CqAttributesFactory<string, object>();
        ICqListener<string, object> cqLstner = new MyCqListener<string, object>();
        cqFac.AddCqListener(cqLstner);
        CqAttributes<string, object> cqAttr = cqFac.Create();

	//create a new cqQuery
        CqQuery<string, object> qry = qrySvc.NewCq("MyCq", "select * from /Portfolios" + "  p where p.ID!=2", cqAttr, false);

        // Execute a CqQuery with Initial Results
        ICqResults<object> results = qry.ExecuteWithInitialResults();

        Console.WriteLine("ResultSet Query returned {0} rows", results.Size);
	//make changes to generate cq events
        region["Key2"] = port1;
        region["Key3"] = port2;
        region["Key1"] = port3;

        SelectResultsIterator<object> iter = results.GetIterator();

        while (iter.HasNext)
        {
          object item = iter.Next();

          if (item != null)
          {
            Struct st = item as Struct;

            string key = st["key"] as string;

            Console.WriteLine("Got key " + key);

            Portfolio port = st["value"] as Portfolio;

            if (port == null)
            {
              Position pos = st["value"] as Position;
              if (pos == null)
              {
                string cs = st["value"] as string;
                if (cs == null)
                {
                  Console.WriteLine("Query got other/unknown object.");
                }
                else
                {
                  Console.WriteLine("Query got string : {0}.", cs);
                }
              }
              else
              {
                Console.WriteLine("Query got Position object with secId {0}, shares {1}.", pos.SecId, pos.SharesOutstanding);
              }
            }
            else
            {
              Console.WriteLine("Query got Portfolio object with ID {0}, pkid {1}.", port.ID, port.Pkid);
            }
          }
        }
	//Stop the cq
        qry.Stop();

	//Close the cq
        qry.Close();

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("CqQuery GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

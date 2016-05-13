/*
 * The CqQuery Example.
 *
 * This example takes the following steps:
 *
 */

// Use standard namespaces
using System;

// Use the GemFire namespace
using GemStone.GemFire.Cache;

// Use the "Tests" namespace for the query objects.
using GemStone.GemFire.Cache.Tests;

namespace GemStone.GemFire.Cache.Examples
{
  // The CqQuery example.

  //User Listener
  public class MyCqListener : ICqListener
  {
    private int m_updateCnt;
    private int m_createCnt;
    private int m_destroyCnt;
    private int m_errorCnt;
    private int m_eventCnt;
    private int m_id;
    bool m_verbose;
    public MyCqListener(int id, bool verbose)
    {
      m_updateCnt = 0;
      m_createCnt = 0;
      m_destroyCnt = 0;
      m_errorCnt = 0;
      m_eventCnt = 0;
      m_id = id;
      m_verbose = verbose;
    }
    public virtual void OnEvent(CqEvent ev)
    {
      m_eventCnt++;
      Portfolio val = ev.getNewValue() as Portfolio;
      CacheableString key = ev.getKey() as CacheableString;
      CqOperationType opType = ev.getQueryOperation();
      CqQuery cq = ev.getCq();
      string opStr = "DESTROY";
      if (opType == CqOperationType.OP_TYPE_CREATE) {
        m_createCnt++;
        opStr = "CREATE";
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE) {
        m_updateCnt++;
        opStr = "UPDATE";
      }
      else if (opType == CqOperationType.OP_TYPE_DESTROY) {
        m_destroyCnt++;
        opStr = "DESTROY";
      }
      if (m_eventCnt % 5000 == 0) {
        if (m_verbose == true) {
          Console.WriteLine("MyCqListener{0}::OnEvent called with key {1}, value ({2},{3}), op {4}.", m_id, key.Value, val.ID, val.Pkid, opStr);
        }
        else {
          Console.WriteLine("cq{0}, listener{1}::OnEvent update count={2}, create Count={3}, destroy Count={4}, total count={5}", cq.Name, m_id, m_updateCnt, m_createCnt, m_destroyCnt, m_eventCnt);
        }
        Console.WriteLine("*******Type \'q\' to quit !!!! ******");
      }
    }
    public virtual void OnError(CqEvent ev)
    {
      m_errorCnt++;
      m_eventCnt++;
      Console.WriteLine("MyCqListener{0}::OnError called", m_id);
    }
    public virtual void Close()
    {
      m_eventCnt++;
      Console.WriteLine("MyCqListener{0}::close called", m_id);
    }
  }

  class MyCacheListener : CacheListenerAdapter
  {
    private int m_eventCount;
    private bool m_verbose;
    private void check(EntryEvent ev, string opStr)
    {
      m_eventCount++;
      Portfolio val = ev.NewValue as Portfolio;
      CacheableString key = ev.Key as CacheableString;
      if (m_eventCount % 3000 == 0) {
        if (m_verbose == true) {
          Console.WriteLine("MyCacheListener called with key {0}, value ({1},{2}), op {3}.", key.Value, val.ID, val.Pkid, opStr);
        }
        else {
          Console.WriteLine("MyCacheListener::event count={0}", m_eventCount);
        }
        Console.WriteLine("*******Type \'q\' to quit !!!! ******");
      }
    }
    public MyCacheListener(bool verbose)
    {
      m_eventCount = 0;
      m_verbose = verbose;
    }

    public override void AfterCreate(EntryEvent ev)
    {
      check(ev, "AfterCreate");
    }
    public override void AfterUpdate(EntryEvent ev)
    {
      check(ev, "AfterUpdate");
    }
    public override void AfterDestroy(EntryEvent ev)
    {
      check(ev, "AfterDestroy");
    }
    public override void AfterInvalidate(EntryEvent ev)
    {
      check(ev, "AfterInvalidate");
    }
  }
  class ContinuousQuery
  {
    private static string[] cqNames = new string[8]{
  "MyCq_0",
  "MyCq_1",
  "MyCq_2",
  "MyCq_3",
  "MyCq_4",
  "MyCq_5",
  "MyCq_6",
  "MyCq_7"
};

    private static string[] queryStrings = new string[8]{
  "select * from /Portfolios p where p.ID < 4",
  "select * from /Portfolios p where p.ID < 9",
  "select * from /Portfolios p where p.ID < 12",
  "select * from /Portfolios p where p.ID < 3",
  "select * from /Portfolios p where p.ID < 14",
  "select * from /Portfolios p where p.ID < 5",
  "select * from /Portfolios p where p.ID < 6",
  "select * from /Portfolios p where p.ID < 7"
};
    static void Main(string[] args)
    {
      bool verbose = false;
      if (args.Length == 1 && args[0] == "-v")
        verbose = true;
      try {
        // Connect to the GemFire Distributed System using the settings from the gfcpp.properties file by default.
        Properties prop = Properties.Create();
        prop.Insert("cache-xml-file", "clientCqQuery.xml");
        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(prop);
        Cache cache = cacheFactory.SetSubscriptionEnabled(true)
                                  .Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the Portfolios Region from the Cache which is declared in the Cache XML file.
        Region region = cache.GetRegion("Portfolios");

        Console.WriteLine("Obtained the Region from the Cache");

        region.GetAttributesMutator().SetCacheListener(new MyCacheListener(verbose));

        // Register our Serializable/Cacheable Query objects, viz. Portfolio and Position.
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);

        //Register all keys
        region.RegisterAllKeys();

        Console.WriteLine("Registered Serializable Query Objects");

        // Populate the Region with some Portfolio objects.
        Portfolio port1 = new Portfolio(1 /*ID*/, 10 /*size*/);
        Portfolio port2 = new Portfolio(2 /*ID*/, 20 /*size*/);
        Portfolio port3 = new Portfolio(3 /*ID*/, 30 /*size*/);
        region.Put("Key1", port1);
        region.Put("Key2", port2);
        region.Put("Key3", port3);

        Console.WriteLine("Populated some Portfolio Objects");

        // Get the QueryService from the Cache.
        QueryService qrySvc = cache.GetQueryService();

        Console.WriteLine("Got the QueryService from the Cache");

        //create CqAttributes with listener
        CqAttributesFactory cqFac = new CqAttributesFactory();
        ICqListener cqLstner = new MyCqListener(0, verbose);
        cqFac.AddCqListener(cqLstner);
        CqAttributes cqAttr = cqFac.Create();

        //create a new cqQuery
        CqQuery qry = qrySvc.NewCq(cqNames[0], queryStrings[0], cqAttr, true);

        // Execute a CqQuery with Initial Results
        ICqResults results = qry.ExecuteWithInitialResults();

        Console.WriteLine("ResultSet Query returned {0} rows", results.Size);

        SelectResultsIterator iter = results.GetIterator();

        while (iter.HasNext) {
          IGFSerializable item = iter.Next();

          if (item != null) {
            Struct st = item as Struct;

            CacheableString key = st["key"] as CacheableString;

            Console.WriteLine("Got key " + key.Value);

            Portfolio port = st["value"] as Portfolio;

            if (port == null) {
              Position pos = st["value"] as Position;
              if (pos == null) {
                CacheableString cs = st["value"] as CacheableString;
                if (cs == null) {
                  Console.WriteLine("Query got other/unknown object.");
                }
                else {
                  Console.WriteLine("Query got string : {0}.", cs.Value);
                }
              }
              else {
                Console.WriteLine("Query got Position object with secId {0}, shares {1}.", pos.SecId, pos.SharesOutstanding);
              }
            }
            else {
              Console.WriteLine("Query got Portfolio object with ID {0}, pkid {1}.", port.ID, port.Pkid);
            }
          }
        }        
        //Stop the cq
        qry.Stop();
        //Restart the cq
        qry.Execute();

        for (int i = 1; i < cqNames.Length; i++) {
          ICqListener cqLstner1 = new MyCqListener(i, verbose);
          cqFac.AddCqListener(cqLstner1);
          cqAttr = cqFac.Create();
          qry = qrySvc.NewCq(cqNames[i], queryStrings[i], cqAttr, true);
        }

        qry = qrySvc.GetCq(cqNames[6]);
        cqAttr = qry.GetCqAttributes();
        ICqListener[] vl = cqAttr.getCqListeners();
        Console.WriteLine("number of listeners for cq[{0}] is {1}", cqNames[6], vl.Length);
        qry = qrySvc.GetCq(cqNames[0]);
        CqAttributesMutator cqam = qry.GetCqAttributesMutator();
        for (int i = 0; i < vl.Length; i++) {
          cqam.AddCqListener(vl[i]);
        }

        //Stop the cq
        qry.Stop();

        //Start all Cq Query
        qrySvc.ExecuteCqs();

        for (int i = 0; i < cqNames.Length; i++) {
          Console.WriteLine("get info for cq[{0}]:", cqNames[i]);
          CqQuery cqy = qrySvc.GetCq(cqNames[i]);
          CqStatistics cqStats = cqy.GetStatistics();
          Console.WriteLine("Cq[{0}]: CqStatistics: numInserts[{1}], numDeletes[{2}], numUpdates[{3}], numEvents[{4}]", 
                              cqNames[i], cqStats.numInserts(), cqStats.numDeletes(), cqStats.numUpdates(), cqStats.numEvents());
        }

        CqServiceStatistics serviceStats = qrySvc.GetCqStatistics();
        Console.WriteLine("numCqsActive={0}, numCqsCreated={1}, numCqsClosed={2}, numCqsStopped={3}, numCqsOnClient={4}", 
                              serviceStats.numCqsActive(), serviceStats.numCqsCreated(), serviceStats.numCqsClosed(), 
                              serviceStats.numCqsStopped(), serviceStats.numCqsOnClient());

        while (true) {
          Console.WriteLine("*******Type \'q\' to quit !!!! ******");
          ConsoleKeyInfo ckey;
          ckey = Console.ReadKey(true);
          if (ckey.Key == ConsoleKey.Q)
            break;
        }

        //Stop all cqs
        qrySvc.StopCqs();

        for (int i = 0; i < cqNames.Length; i++) {
          Console.WriteLine("get info for cq[{0}]:", cqNames[i]);
          CqQuery cqy = qrySvc.GetCq(cqNames[i]);
          cqAttr = qry.GetCqAttributes();
          vl = cqAttr.getCqListeners();
          Console.WriteLine("number of listeners for cq[{0}] is {1}", cqNames[i], vl.Length);
          CqStatistics cqStats = cqy.GetStatistics();
          Console.WriteLine("Cq[{0}]: CqStatistics: numInserts[{1}], numDeletes[{2}], numUpdates[{3}], numEvents[{4}]", 
                         cqNames[i], cqStats.numInserts(), cqStats.numDeletes(), cqStats.numUpdates(), cqStats.numEvents());
        }

        //Close all cqs
        qrySvc.CloseCqs();

        Console.WriteLine("numCqsActive={0}, numCqsCreated={1}, numCqsClosed={2}, numCqsStopped={3}, numCqsOnClient={4}", 
                         serviceStats.numCqsActive(), serviceStats.numCqsCreated(), serviceStats.numCqsClosed(), serviceStats.numCqsStopped(),
                         serviceStats.numCqsOnClient());

        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex) {
        Console.WriteLine("CqQuery GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}

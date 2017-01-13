using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;

  using GemStone.GemFire.Cache.Generic;

  using GIRegion = GemStone.GemFire.Cache.Generic.IRegion<int, object>;
  using System.Collections.Generic;

  public class CallbackListener : CacheListenerAdapter<int, object>
  {
    int m_creates;
    int m_updates;
    int m_invalidates;
    int m_destroys;
    int m_regionInvalidate;
    int m_regionDestroy;
    int m_regionClear; 
    object m_callbackArg;

    #region Getters

    public int Creates
    {
      get { return m_creates; }
    }

    public int Updates
    {
      get { return m_updates; }
    }

    public int Invalidates
    {
      get { return m_invalidates; }
    }

    public int Destroys
    {
      get { return m_destroys; }
    }

    public int RegionInvalidates
    {
      get { return m_regionInvalidate; }
    }

    public int RegionDestroys
    {
      get { return m_regionDestroy; }
    }
    public int RegionClear
 	{ 
 	  get { return m_regionClear; }
 	}
    public object CallbackArg
    {
      get { return m_callbackArg; }
    }
    #endregion

    public void SetCallbackArg(object callbackArg)
    {
      m_callbackArg = callbackArg;
    }

    private void check(object eventCallback, ref int updateCount)
    {
      Log.Fine("check..");
      if (eventCallback != null)
      {
        string callbackArg = eventCallback as string;

        if (callbackArg != null)
        {
          string cs = m_callbackArg as string;
          if (cs != null)
          {
            if (callbackArg == cs)
            {
              Log.Fine("value matched");
              updateCount++;
            }
            else
              Log.Fine("value matched NOT");
          }
        }
        else
        {
          Log.Fine("Callbackarg is not cacheable string");
          Portfolio pfCallback = eventCallback as Portfolio;
          if (pfCallback != null)
          {
            Portfolio pf = m_callbackArg as Portfolio;
            if (pf != null)
            {
              if (pf.Pkid == pfCallback.Pkid && pfCallback.ArrayNull == null
                    && pfCallback.ArrayZeroSize != null && pfCallback.ArrayZeroSize.Length == 0)
              {
                Log.Fine("value matched");
                updateCount++;
              }
            }
          }
        }
      }
    }

    private void checkCallbackArg(EntryEvent<int, object> entryEvent, ref int updateCount)
    {
      check(entryEvent.CallbackArgument, ref updateCount);
    }

    private void checkCallbackArg(RegionEvent<int, object> regionEvent, ref int updateCount)
    {
      check(regionEvent.CallbackArgument, ref updateCount);
    }

    #region CacheListener Members

    public override void AfterCreate(EntryEvent<int, object> ev)
    {
      checkCallbackArg(ev, ref m_creates);
    }

    public override void AfterUpdate(EntryEvent<int, object> ev)
    {
      checkCallbackArg(ev, ref m_updates);
    }

    public override void AfterInvalidate(EntryEvent<int, object> ev)
    {
      checkCallbackArg(ev, ref m_invalidates);
    }

    public override void AfterDestroy(EntryEvent<int, object> ev)
    {
      checkCallbackArg(ev, ref m_destroys);
    }

    public override void AfterRegionInvalidate(RegionEvent<int, object> rev)
    {
      checkCallbackArg(rev, ref m_regionInvalidate);
    }

    public override void AfterRegionDestroy(RegionEvent<int, object> rev)
    {
      checkCallbackArg(rev, ref m_regionDestroy);
    }
    public override void AfterRegionClear(RegionEvent<int, object> rev)
    {
        checkCallbackArg(rev, ref m_regionClear);
    }

    #endregion
  }

  [TestFixture]
  [Category("generics")]
  public class ThinClientCallbackArg : ThinClientRegionSteps
  {
    private TallyWriter<int, object> m_writer;
    private TallyListener<int, object> m_listener;
    private CallbackListener m_callbackListener;
    RegionOperation o_region;
    private UnitProcess m_client1, m_client2;
    int key0 = 12;
    object m_callbackarg = "Gemstone's Callback";
    
    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    public void CreateRegion(string locators,
      bool caching, bool listener, bool writer)
    {
      if (listener)
      {
        m_listener = new TallyListener<int, object>();

      }
      else
      {
        m_listener = null;
      }
      GIRegion region = null;
      region = CacheHelper.CreateTCRegion_Pool<int, object>(RegionName, true, caching,
        m_listener, locators, "__TESTPOOL1_", true);
      if (listener)
        m_listener.SetCallBackArg(key0);

      if (writer)
      {
        m_writer = new TallyWriter<int, object>();

      }
      else
      {
        m_writer = null;
      }
      if (writer)
      {
        AttributesMutator<int, object> at = region.AttributesMutator;
        at.SetCacheWriter(m_writer);
        m_writer.SetCallBackArg(key0);
      }
    }

    public void CreateRegion2(string locators,
      bool caching, bool listener, bool writer)
    {
      CallbackListener callbackLis = null;
      if (listener)
      {
        m_callbackListener = new CallbackListener();
        m_callbackListener.SetCallbackArg(m_callbackarg);
        callbackLis = m_callbackListener;
      }
      else
      {
        m_listener = null;
      }
      GIRegion region = null;
      region = CacheHelper.CreateTCRegion_Pool<int, object>(RegionName, true, caching,
        callbackLis, locators, "__TESTPOOL1_", true);
    }

    public void ValidateLocalListenerWriterData()
    {
      Thread.Sleep(2000);
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer should be invoked");
      Assert.AreEqual(true, m_listener.IsListenerInvoked, "Listener should be invoked");
      Assert.AreEqual(true, m_writer.IsCallBackArgCalled, "Writer CallbackArg should be invoked");
      Assert.AreEqual(true, m_listener.IsCallBackArgCalled, "Listener CallbackArg should be invoked");
      m_listener.ShowTallies();
      m_writer.ShowTallies();
    }

    public void ValidateEvents()
    {
      Assert.AreEqual(15, m_writer.Creates, "Should be 10 creates");
      Assert.AreEqual(15, m_listener.Creates, "Should be 10 creates");
      Assert.AreEqual(15, m_writer.Updates, "Should be 5 updates");
      Assert.AreEqual(15, m_listener.Updates, "Should be 5 updates");
      Assert.AreEqual(0, m_writer.Invalidates, "Should be 0 invalidates");
      Assert.AreEqual(5, m_listener.Invalidates, "Should be 5 invalidates");
      Assert.AreEqual(10, m_writer.Destroys, "Should be 10 destroys");  // 5 destroys + 5 removes
      Assert.AreEqual(10, m_listener.Destroys, "Should be 10 destroys"); // 5 destroys + 5 removes
    }

    public void CallOp()
    {
      o_region = new RegionOperation(RegionName);
      o_region.PutOp(5, key0);
      Thread.Sleep(1000); // let the events reach at other end.
      o_region.PutOp(5, key0);
      Thread.Sleep(1000);
      o_region.InvalidateOp(5, key0);
      Thread.Sleep(1000);
      o_region.DestroyOp(5, key0);
      Thread.Sleep(1000); // let the events reach at other end.
      o_region.PutOp(5, key0);
      Thread.Sleep(1000);
      o_region.RemoveOp(5, key0);
      Thread.Sleep(1000);
    }

    void RegisterPdxType8()
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
    }

    void runCallbackArgTest()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion, CacheHelper.Locators,
        true, true, true);
      m_client1.Call(RegisterAllKeys, new string[] { RegionName });

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Locators,
        true, true, true);
      m_client2.Call(RegisterAllKeys, new string[] { RegionName });

      m_client2.Call(RegisterPdxType8);

      m_client1.Call(CallOp);
      

      m_client1.Call(ValidateLocalListenerWriterData);
      m_client1.Call(ValidateEvents);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    private bool m_isSet = false;
    public void SetCallbackArg()
    {
      if (!m_isSet)
      {
        m_isSet = true;
        m_callbackarg = new Portfolio(1, 1);
        //TODO:;split
        Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
        Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
      }
    }

    public void TestCreatesAndUpdates()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.Add("Key-1", "Val-1", m_callbackarg);
      o_region.Region.Put("Key-1", "NewVal-1", m_callbackarg);
      Thread.Sleep(10000);
    }

    public void TestInvalidates()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.GetLocalView().Add(1234, 1234, m_callbackarg);
      o_region.Region.GetLocalView().Add(12345, 12345, m_callbackarg);
      o_region.Region.GetLocalView().Add(12346, 12346, m_callbackarg);
      o_region.Region.GetLocalView().Put(1234, "Val-1", m_callbackarg);
      o_region.Region.GetLocalView().Invalidate(1234, m_callbackarg);
      Assert.AreEqual(o_region.Region.GetLocalView().Remove(12345, 12345, m_callbackarg), true, "Result of remove should be true, as this value exists locally.");
      Assert.AreEqual(o_region.Region.GetLocalView().ContainsKey(12345), false, "containsKey should be false");
      Assert.AreEqual(o_region.Region.GetLocalView().Remove(12346, m_callbackarg), true, "Result of remove should be true, as this value exists locally.");
      Assert.AreEqual(o_region.Region.GetLocalView().ContainsKey(12346), false, "containsKey should be false"); 
      o_region.Region.Invalidate("Key-1", m_callbackarg);
      o_region.Region.InvalidateRegion(m_callbackarg);
    }

    public void TestDestroy()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.Remove("Key-1", m_callbackarg);
      //o_region.Region.DestroyRegion(m_callbackarg);
    }

    public void TestRemove()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.Remove("Key-1", "NewVal-1", m_callbackarg);
      o_region.Region.DestroyRegion(m_callbackarg);
    }

    public void TestlocalClear()
    {
        o_region = new RegionOperation(RegionName);
        o_region.Region.GetLocalView().Clear(m_callbackarg); 
    }
    public void TestValidate()
    {
      Thread.Sleep(10000);
      Assert.AreEqual(5, m_callbackListener.Creates, "Should be 5 creates");
      Assert.AreEqual(3, m_callbackListener.Updates, "Should be 3 update");
      Assert.AreEqual(2, m_callbackListener.Invalidates, "Should be 2 invalidate");
      Assert.AreEqual(4, m_callbackListener.Destroys, "Should be 4 destroy");
      Assert.AreEqual(1, m_callbackListener.RegionInvalidates, "Should be 1 region invalidates");
      Assert.AreEqual(1, m_callbackListener.RegionDestroys, "Should be 1 regiondestroy");
      Assert.AreEqual(1, m_callbackListener.RegionClear, "Should be 1 RegionClear");
    }

    void runCallbackArgTest2(int callbackArgChange)
    {
      if (callbackArgChange == 1)
      {
        //change now custom type
        m_callbackarg = new Portfolio(1, 1);
        m_client1.Call(SetCallbackArg);
        m_client2.Call(SetCallbackArg);
      }

      m_callbackListener = new CallbackListener();
      m_callbackListener.SetCallbackArg(m_callbackarg);
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription5N.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion2, CacheHelper.Locators,
        true, true, false);
      m_client1.Call(RegisterAllKeys, new string[] { RegionName });

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion2, CacheHelper.Locators,
        true, false, false);

      m_client2.Call(TestCreatesAndUpdates);
      m_client1.Call(TestInvalidates);
      m_client2.Call(TestDestroy);
      m_client2.Call(TestCreatesAndUpdates);
      m_client1.Call(TestlocalClear); 
      m_client2.Call(TestRemove);
      m_client1.Call(TestValidate);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    private bool isRegistered = false;
    public void registerDefaultCacheableType()
    {
      if (!isRegistered)
      {
        Serializable.RegisterTypeGeneric(DefaultType.CreateDeserializable);
        isRegistered = true;
      }
    }


    public void CallOp2()
    {
      o_region = new RegionOperation(RegionName);
      DefaultType dc = new DefaultType(true);
      o_region.Region.Put("key-1", dc, null);
      Thread.Sleep(1000); // let the events reach at other end.
    }

    public void ValidateData()
    {
      o_region = new RegionOperation(RegionName);
      DefaultType dc = (DefaultType)o_region.Region.Get("key-1", null);

      Assert.AreEqual(dc.CBool, true, "bool is not equal");
      Assert.AreEqual(dc.CInt, 1000, "int is not equal");

      int[] cia = dc.CIntArray;
      Assert.IsNotNull(cia, "Int array is null");
      Assert.AreEqual(3, cia.Length, "Int array are not three");

      string[] csa = dc.CStringArray;
      Assert.IsNotNull(csa, "String array is null");
      Assert.AreEqual(2, csa.Length, "String array length is not two");

      Assert.AreEqual(dc.CFileName, "gemstone.txt", "Cacheable filename is not equal");

      /*
      Assert.IsNotNull(dc.CHashSet, "hashset is null");
      Assert.AreEqual(2, dc.CHashSet.Count, "hashset size is not two");
       * */

      Assert.IsNotNull(dc.CHashMap, "hashmap is null");
      Assert.AreEqual(1, dc.CHashMap.Count, "hashmap size is not one");

      //Assert.IsNotNull(dc.CDate, "Date is null");

      Assert.IsNotNull(dc.CVector);
      Assert.AreEqual(2, dc.CVector.Count, "Vector size is not two");

      //Assert.IsNotNull(dc.CObject);
      //Assert.AreEqual("key", ((CustomSerializableObject)dc.CObject).key, "Object key is not same");
      //Assert.AreEqual("value", ((CustomSerializableObject)dc.CObject).value, "Object value is not same");
    }

    void runCallbackArgTest3()
    {

      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription6.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion, CacheHelper.Locators,
        true, false, false);
      // m_client1.Call(RegisterAllKeys, new string[] { RegionName });

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Locators,
        true, false, false);

      m_client1.Call(registerDefaultCacheableType);
      m_client2.Call(registerDefaultCacheableType);

      m_client2.Call(RegisterAllKeys, new string[] { RegionName });

      m_client1.Call(CallOp2);

      m_client2.Call(ValidateData);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    public void TestRemoveAll()
    {
      o_region = new RegionOperation(RegionName);
      ICollection<object> keys = new LinkedList<object>(); 
      for(int i =0; i< 10; i++)
      {
        o_region.Region["Key-"+i] = "Value-"+i;
        keys.Add("Key-" + i);
      }
      o_region.Region.RemoveAll(keys, m_callbackarg);
    }

    public void TestPutAll()
    {
      o_region = new RegionOperation(RegionName);
      Dictionary<Object, Object> entryMap = new Dictionary<Object, Object>();
      for (Int32 item = 0; item < 10; item++)
      {
        int K = item;
        string value = item.ToString();
        entryMap.Add(K, value);
      }
      o_region.Region.PutAll(entryMap, 15, m_callbackarg);
    }

    public void TestGetAll()
    {
      o_region = new RegionOperation(RegionName);
      List<Object> keys = new List<Object>();
      for (int item = 0; item < 10; item++)
      {
        Object K = item;
        keys.Add(K);
      }
      Dictionary<Object, Object> values = new Dictionary<Object, Object>();
      o_region.Region.GetAll(keys.ToArray(), values, null, true, m_callbackarg);

      Dictionary<Object, Object>.Enumerator enumerator = values.GetEnumerator();
      while (enumerator.MoveNext())
      {
        Util.Log("Values after getAll with CallBack Key = {0} Value = {1} ", enumerator.Current.Key.ToString(), enumerator.Current.Value.ToString());
      }
    }

    public void TestValidateRemoveAllCallback()
    {
      Thread.Sleep(10000);
      Assert.AreEqual(10, m_callbackListener.Destroys, "Should be 10 destroy");
    }

    public void TestValidatePutAllCallback()
    {
      Thread.Sleep(10000);
      Assert.AreEqual(10, m_callbackListener.Creates, "Should be 10 creates");      
      Assert.AreEqual("Gemstone's Callback", m_callbackListener.CallbackArg, "CallBackArg for putAll should be same");
    }

    void runPutAllCallbackArgTest()
    {
      m_callbackListener = new CallbackListener();
      m_callbackListener.SetCallbackArg(m_callbackarg);
      
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription5N.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion2, CacheHelper.Locators,
        true, true, false);
      m_client1.Call(RegisterAllKeys, new string[] { RegionName });
      Util.Log("RegisterAllKeys completed..");

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion2, CacheHelper.Locators,
        true, false, false);
      Util.Log("CreateRegion2 completed..");

      m_client2.Call(TestPutAll);
      Util.Log("TestPutAll completed..");
      m_client1.Call(TestValidatePutAllCallback);
      Util.Log("TestValidatePutAllCallback completed..");
      m_client2.Call(TestGetAll);
      Util.Log("TestGetAll completed..");

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    void runRemoveAllCallbackArgTest()
    {

      m_callbackListener = new CallbackListener();
      m_callbackListener.SetCallbackArg(m_callbackarg);
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription5N.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion2, CacheHelper.Locators,
        true, true, false);
      m_client1.Call(RegisterAllKeys, new string[] { RegionName });
      Util.Log("RegisterAllKeys completed..");

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion2,CacheHelper.Locators,
        true, false, false);
      Util.Log("CreateRegion2 completed..");

      m_client2.Call(TestRemoveAll);
      Util.Log("TestRemoveAll completed..");
      m_client1.Call(TestValidateRemoveAllCallback);
      Util.Log("TestValidateRemoveAllCallback completed..");

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    [Test]
    public void ThinClientCallbackArgTest()
    {
      runCallbackArgTest();
    }

    [Test]
    public void ThinClientCallbackArgTest2()
    {
      for (int i = 0; i < 2; i++)
      {
        runCallbackArgTest2(i);
      }
    }

    [Test]
    public void ThinClientCallbackArgTest3()
    {
      runCallbackArgTest3();
    }

    [Test]
    public void RemoveAllCallbackArgTest()
    {
      runRemoveAllCallbackArgTest();
    }

    [Test]
    public void PutAllCallbackArgTest()
    {
      runPutAllCallbackArgTest();
    }
  }
}
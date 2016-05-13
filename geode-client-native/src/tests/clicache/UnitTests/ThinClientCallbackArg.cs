using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  public class CallbackListener : CacheListenerAdapter
  {
    int m_creates;
    int m_updates;
    int m_invalidates;
    int m_destroys;
    int m_regionInvalidate;
    int m_regionDestroy;
    //CacheableString m_callbackArg;
    IGFSerializable m_callbackArg;

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

    #endregion

    public void SetCallbackArg(IGFSerializable callbackArg)
    {
      m_callbackArg = callbackArg;
    }

    private void check(IGFSerializable eventCallback, ref int updateCount)
    {
      Log.Fine("check..");
      if (eventCallback != null)
      {
        CacheableString callbackArg = eventCallback as CacheableString;

        if (callbackArg != null)
        {
          CacheableString cs = m_callbackArg as CacheableString;
          if (cs != null)
          {
            if (callbackArg.Value == cs.Value)
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
              if (pf.Pkid.Value == pfCallback.Pkid.Value && pfCallback.ArrayNull == null
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

    private void checkCallbackArg(EntryEvent entryEvent, ref int updateCount)
    {
      check(entryEvent.CallbackArgument, ref updateCount);
    }

    private void checkCallbackArg(RegionEvent entryEvent, ref int updateCount)
    {
      check(entryEvent.CallbackArgument, ref updateCount);
    }

    #region CacheListener Members

    public override void AfterCreate(EntryEvent ev)
    {
      checkCallbackArg(ev, ref m_creates);
    }

    public override void AfterUpdate(EntryEvent ev)
    {
      checkCallbackArg(ev, ref m_updates);
    }

    public override void AfterInvalidate(EntryEvent ev)
    {
      checkCallbackArg(ev, ref m_invalidates);
    }

    public override void AfterDestroy(EntryEvent ev)
    {
      checkCallbackArg(ev, ref m_destroys);
    }

    public override void AfterRegionInvalidate(RegionEvent ev)
    {
      checkCallbackArg(ev, ref m_regionInvalidate);
    }

    public override void AfterRegionDestroy(RegionEvent ev)
    {
      checkCallbackArg(ev, ref m_regionDestroy);
    }

    #endregion
  }

  [TestFixture]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientCallbackArg : ThinClientRegionSteps
  {
    private TallyWriter m_writer;
    private TallyListener m_listener;
    private CallbackListener m_callbackListener;
    RegionOperation o_region;
    private UnitProcess m_client1, m_client2;
    IGFSerializable key0 = new CacheableInt32(12);
    //CacheableString m_callbackarg = new CacheableString("Gemstone's Callback");
    IGFSerializable m_callbackarg = new CacheableString("Gemstone's Callback");



    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    public void CreateRegion(string endpoints, string locators,
      bool caching, bool listener, bool writer, bool pool, bool locator)
    {
      if (listener)
      {
        m_listener = new TallyListener();

      }
      else
      {
        m_listener = null;
      }
      Region region = null;
      if (pool)
      {
        region = CacheHelper.CreateTCRegion_Pool(RegionName, true, caching,
          m_listener, endpoints, locators, "__TESTPOOL1_", true);
      }
      else
      {
        region = CacheHelper.CreateTCRegion(RegionName, true, caching,
          m_listener, endpoints, true);
      }
      if (listener)
        m_listener.SetCallBackArg(key0);

      if (writer)
      {
        m_writer = new TallyWriter();

      }
      else
      {
        m_writer = null;
      }
      if (writer)
      {
        AttributesMutator at = region.GetAttributesMutator();
        at.SetCacheWriter(m_writer);
        m_writer.SetCallBackArg(key0);
      }
    }

    public void CreateRegion2(string endpoints, string locators,
      bool caching, bool listener, bool writer, bool pool, bool locator)
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
      Region region = null;
      if (pool)
      {
        region = CacheHelper.CreateTCRegion_Pool(RegionName, true, caching,
          callbackLis, endpoints, locators, "__TESTPOOL1_", true);
      }
      else
      {
        region = CacheHelper.CreateTCRegion(RegionName, true, caching,
          callbackLis, endpoints, true);
      }
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
      Assert.AreEqual(10, m_writer.Creates, "Should be 10 creates");
      Assert.AreEqual(10, m_listener.Creates, "Should be 10 creates");
      Assert.AreEqual(5, m_writer.Updates, "Should be 5 updates");
      Assert.AreEqual(5, m_listener.Updates, "Should be 5 updates");
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

    void runCallbackArgTest(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, "cacheserver_notify_subscription.xml");
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        true, true, true, pool, locator);
      m_client1.Call(RegisterAllKeys, new string[] { RegionName });

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        true, true, true, pool, locator);
      m_client2.Call(RegisterAllKeys, new string[] { RegionName });

      m_client1.Call(CallOp);

      m_client1.Call(ValidateLocalListenerWriterData);
      m_client1.Call(ValidateEvents);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }

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
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);
      }
    }

    public void TestCreatesAndUpdates()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.Create("Key-1", "Val-1", m_callbackarg);
      o_region.Region.Put("Key-1", "NewVal-1", m_callbackarg);
      Thread.Sleep(10000);
    }

    public void TestInvalidates()
    {
      CacheableString tmp = new CacheableString("hitesh");
      o_region = new RegionOperation(RegionName);
      o_region.Region.Invalidate("Key-1", m_callbackarg);
      o_region.Region.InvalidateRegion(m_callbackarg);
    }

    public void TestDestroy()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.Destroy("Key-1", m_callbackarg);
      //o_region.Region.DestroyRegion(m_callbackarg);
    }

    public void TestRemove()
    {
      o_region = new RegionOperation(RegionName);
      o_region.Region.Remove("Key-1", "NewVal-1", m_callbackarg);
      o_region.Region.DestroyRegion(m_callbackarg);
    }

    public void TestValidate()
    {
      Thread.Sleep(10000);
      Assert.AreEqual(2, m_callbackListener.Creates, "Should be 1 creates");
      Assert.AreEqual(2, m_callbackListener.Updates, "Should be 1 update");
      Assert.AreEqual(1, m_callbackListener.Invalidates, "Should be 1 invalidate");
      Assert.AreEqual(2, m_callbackListener.Destroys, "Should be 1 destroy");
      Assert.AreEqual(1, m_callbackListener.RegionInvalidates, "Should be 1 region invalidates");
      Assert.AreEqual(1, m_callbackListener.RegionDestroys, "Should be 1 regiondestroy");
    }

    void runCallbackArgTest2(int callbackArgChange, bool pool, bool locator)
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
      CacheHelper.SetupJavaServers(pool && locator, "cacheserver_notify_subscription5.xml");
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion2, CacheHelper.Endpoints, CacheHelper.Locators,
        true, true, false, pool, locator);
      m_client1.Call(RegisterAllKeys, new string[] { RegionName });

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion2, CacheHelper.Endpoints, CacheHelper.Locators,
        true, false, false, pool, locator);

      m_client2.Call(TestCreatesAndUpdates);
      m_client1.Call(TestInvalidates);
      m_client2.Call(TestDestroy);
      m_client2.Call(TestCreatesAndUpdates);
      m_client2.Call(TestRemove);
      m_client1.Call(TestValidate);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    private bool isRegistered = false;
    public void registerDefaultCacheableType()
    {
      if (!isRegistered)
      {
        Serializable.RegisterType(DefaultCacheable.CreateDeserializable);
        isRegistered = true;
      }
    }


    public void CallOp2()
    {
      o_region = new RegionOperation(RegionName);
      DefaultCacheable dc = new DefaultCacheable(true);
      o_region.Region.Put("key-1", dc);
      Thread.Sleep(1000); // let the events reach at other end.
    }

    public void ValidateData()
    {
      o_region = new RegionOperation(RegionName);
      DefaultCacheable dc = (DefaultCacheable)o_region.Region.Get("key-1");

      Assert.AreEqual(dc.CBool.Value, true, "bool is not equal");
      Assert.AreEqual(dc.CInt.Value, 1000, "int is not equal");

      CacheableInt32Array cia = dc.CIntArray;
      Assert.IsNotNull(cia, "Int array is null");
      Assert.AreEqual(3, cia.Value.Length, "Int array are not three");

      CacheableStringArray csa = dc.CStringArray;
      Assert.IsNotNull(csa, "String array is null");
      Assert.AreEqual(2, csa.Length, "String array length is not two");

      //Assert.AreEqual(dc.CFileName.Value, "gemstone.txt", "Cacheable filename is not equal");

      Assert.IsNotNull(dc.CHashSet, "hashset is null");
      Assert.AreEqual(2, dc.CHashSet.Count, "hashset size is not two");

      Assert.IsNotNull(dc.CHashMap, "hashmap is null");
      Assert.AreEqual(1, dc.CHashMap.Count, "hashmap size is not one");

      Assert.IsNotNull(dc.CDate, "Date is null");

      Assert.IsNotNull(dc.CVector);
      Assert.AreEqual(2, dc.CVector.Count, "Vector size is not two");

      Assert.IsNotNull(dc.CObject);
      Assert.AreEqual("key", ((CustomSerializableObject)dc.CObject.Value).key, "Object key is not same");
      Assert.AreEqual("value", ((CustomSerializableObject)dc.CObject.Value).value, "Object value is not same");
    }

    void runCallbackArgTest3(bool pool, bool locator)
    {

      CacheHelper.SetupJavaServers(pool && locator, "cacheserver_notify_subscription6.xml");
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, cache-enabled, with listener and writer");
      m_client1.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        true, false, false, pool, locator);
      // m_client1.Call(RegisterAllKeys, new string[] { RegionName });

      Util.Log("Creating region in client2 , no-ack, cache-enabled, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        true, false, false, pool, locator);

      m_client1.Call(registerDefaultCacheableType);
      m_client2.Call(registerDefaultCacheableType);

      m_client2.Call(RegisterAllKeys, new string[] { RegionName });

      m_client1.Call(CallOp2);

      m_client2.Call(ValidateData);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }

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
      runCallbackArgTest(false, false); // region config
      runCallbackArgTest(true, false); // region config
      runCallbackArgTest(true, true); // region config
    }

    [Test]
    public void ThinClientCallbackArgTest2()
    {
      for (int i = 0; i < 2; i++)
      {
        runCallbackArgTest2(i, false, false); // region config
        runCallbackArgTest2(i, true, false); // region config
        runCallbackArgTest2(i, true, true); // region config
      }
    }

    [Test]
    public void ThinClientCallbackArgTest3()
    {
      runCallbackArgTest3(false, false); // region config
      runCallbackArgTest3(true, false); // region config
      runCallbackArgTest3(true, true); // region config
    }
  }
}
//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.Cache.Generic;

  #region Listener
  class ConflationListner<TKey, TValue> : ICacheListener<TKey, TValue>
  {
    #region Private members

    private int m_events = 0;
    private TValue m_value = default(TValue);
    #endregion

    public static ConflationListner<TKey, TValue> Create()
    {
      Util.Log(" ConflationListner Created");

      return new ConflationListner<TKey, TValue>();
    }

    private void check(EntryEvent<TKey, TValue> ev)
    {
      m_events++;
      TKey key = ev.Key;
      TValue value = ev.NewValue;
      m_value = value;
      Util.Log("Region:{0}:: Key:{1}, Value:{2}", ev.Region.Name, key, value);

    }

    public void validate(bool conflation)
    {
      if (conflation)
      {
        string msg1 = string.Format("Conflation On: Expected 2 events but got {0}", m_events);
        Assert.AreEqual(2, m_events, msg1);
      }
      else
      {
        string msg2 = string.Format("Conflation Off: Expected 5 events but got {0}", m_events);
        Assert.AreEqual(5, m_events, msg2);
      }

      string msg3 = string.Format("Expected Value =5, Actual = {0}", m_value);
      Assert.AreEqual(5, m_value, msg3);
    }

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent<TKey, TValue> ev)
    {
      check(ev);
    }

    public virtual void AfterUpdate(EntryEvent<TKey, TValue> ev)
    {
      check(ev);
    }

    public virtual void AfterDestroy(EntryEvent<TKey, TValue> ev) { }

    public virtual void AfterInvalidate(EntryEvent<TKey, TValue> ev) { }

    public virtual void AfterRegionDestroy(RegionEvent<TKey, TValue> ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent<TKey, TValue> ev) { }

    public virtual void AfterRegionClear(RegionEvent<TKey, TValue> ev) { }

    public virtual void AfterRegionLive(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("DurableListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public virtual void Close(IRegion<TKey, TValue> region) { }
    public virtual void AfterRegionDisconnected(IRegion<TKey, TValue> region) { }

    #endregion
  }
  #endregion

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  
  public class ThinClientConflationTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2, m_feeder;
    private string[] keys = { "Key-1", "Key-2", "Key-3", "Key-4", "Key-5" };

    private static string[] DurableClientIds = { "DurableClientId1", "DurableClientId2" };

    static string[] Regions = { "ConflatedRegion", "NonConflatedRegion" };

    private static ConflationListner<object, object> m_listener1C1, m_listener2C1, m_listener1C2, m_listener2C2;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_feeder = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_feeder };
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);
        m_feeder.Call(CacheHelper.Close);
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    #region Common Functions

    public void InitFeeder(string locators, int redundancyLevel)
    {
      CacheHelper.CreatePool<object, object>("__TESTPOOL1_", locators, (string)null, redundancyLevel, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(Regions[0], false, false, null,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      CacheHelper.CreateTCRegion_Pool<object, object>(Regions[1], false, false, null,
        CacheHelper.Locators, "__TESTPOOL1_", false);
    }

    public void InitDurableClient(int client, string locators, string conflation)
    {
      // Create DurableListener for first time and use same afterward.
      ConflationListner<object, object> checker1 = null;
      ConflationListner<object, object> checker2 = null;
      string durableId = ThinClientConflationTests.DurableClientIds[client - 1];
      if (client == 1)
      {
        ThinClientConflationTests.m_listener1C1 = ConflationListner<object, object>.Create();
        ThinClientConflationTests.m_listener2C1 = ConflationListner<object, object>.Create();
        checker1 = ThinClientConflationTests.m_listener1C1;
        checker2 = ThinClientConflationTests.m_listener2C1;
      }
      else // client == 2 
      {
        ThinClientConflationTests.m_listener1C2 = ConflationListner<object, object>.Create();
        ThinClientConflationTests.m_listener2C2 = ConflationListner<object, object>.Create();
        checker1 = ThinClientConflationTests.m_listener1C2;
        checker2 = ThinClientConflationTests.m_listener2C2;
      }
      CacheHelper.InitConfigForConflation_Pool(locators, durableId, conflation);
      CacheHelper.CreateTCRegion_Pool<object, object>(Regions[0], false, true, checker1,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(Regions[1], false, true, checker2,
        CacheHelper.Locators, "__TESTPOOL1_", true);

      //CacheHelper.DCache.ReadyForEvents();

      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(Regions[0]);
      region1.GetSubscriptionService().RegisterAllKeys(true);
      IRegion<object, object> region2 = CacheHelper.GetVerifyRegion<object, object>(Regions[1]);
      region2.GetSubscriptionService().RegisterAllKeys(true);
    }

    public void ReadyForEvents()
    {
      CacheHelper.DCache.ReadyForEvents();
    }

    public void FeederUpdate(int keyIdx)
    {
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(Regions[0]);

      
      region1[keys[keyIdx]] = 1;
      region1[keys[keyIdx]] = 2;
      region1[keys[keyIdx]] = 3;
      region1[keys[keyIdx]] = 4;
      region1[keys[keyIdx]] = 5;

      IRegion<object, object> region2 = CacheHelper.GetVerifyRegion<object, object>(Regions[1]);

      region2[keys[keyIdx]] = 1;
      region2[keys[keyIdx]] = 2;
      region2[keys[keyIdx]] = 3;
      region2[keys[keyIdx]] = 4;
      region2[keys[keyIdx]] = 5;
    }

    public void ClientDown()
    {
      CacheHelper.Close();
    }


    public void KillServer()
    {
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    public delegate void KillServerDelegate();

    #endregion


    public void Validate(int client, int region, bool conflate)
    {
      ConflationListner<object, object> checker = null;
      if (client == 1)
      {
        if (region == 1)
          checker = ThinClientConflationTests.m_listener1C1;
        else
          checker = ThinClientConflationTests.m_listener2C1;
      }
      else // client == 2
      {
        if (region == 1)
          checker = ThinClientConflationTests.m_listener1C2;
        else
          checker = ThinClientConflationTests.m_listener2C2;
      }

      if (checker != null)
      {
        checker.validate(conflate);
      }
      else
      {
        Assert.Fail("Checker is NULL!");
      }
    }

    void runConflationBasic()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_conflation.xml");

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_feeder.Call(InitFeeder, CacheHelper.Locators, 0);
      Util.Log("Feeder initialized.");

      //Test "true" and "false" settings
      m_client1.Call(InitDurableClient, 1, CacheHelper.Locators, "true");
      m_client2.Call(InitDurableClient, 2, CacheHelper.Locators, "false");
      Util.Log("Clients initialized for first time.");

      m_feeder.Call(FeederUpdate, 0);
      Util.Log("Feeder performed first update.");

      Util.Log("Client1 sending readyForEvents().");
      m_client1.Call(ReadyForEvents);
      Thread.Sleep(5000);
      Util.Log("Validating Client 1 Region 1.");
      m_client1.Call(Validate, 1, 1, true);
      Util.Log("Validating Client 1 Region 2.");
      m_client1.Call(Validate, 1, 2, true);

      Util.Log("Client2 sending readyForEvents().");
      m_client2.Call(ReadyForEvents);
      Thread.Sleep(5000);
      Util.Log("Validating Client 2 Region 1.");
      m_client2.Call(Validate, 2, 1, false);
      Util.Log("Validating Client 2 Region 1.");
      m_client2.Call(Validate, 2, 2, false);

      //Close Clients.
      m_client1.Call(ClientDown);
      m_client2.Call(ClientDown);
      Util.Log("First step complete, tested true/false options.");

      //Test "server" and not set settings
      m_client1.Call(InitDurableClient, 1, CacheHelper.Locators, "server");
      m_client2.Call(InitDurableClient, 2, CacheHelper.Locators, "");
      Util.Log("Clients initialized second times.");

      m_feeder.Call(FeederUpdate, 1);
      Util.Log("Feeder performed second update.");

      Util.Log("Client1 sending readyForEvents().");
      m_client1.Call(ReadyForEvents);
      Thread.Sleep(5000);
      Util.Log("Validating Client 1 Region 1.");
      m_client1.Call(Validate, 1, 1, true);
      Util.Log("Validating Client 1 Region 2.");
      m_client1.Call(Validate, 1, 2, false);

      Util.Log("Client2 sending readyForEvents().");
      m_client2.Call(ReadyForEvents);
      Thread.Sleep(5000);
      Util.Log("Validating Client 2 Region 1.");
      m_client2.Call(Validate, 2, 1, true);
      Util.Log("Validating Client 2 Region 2.");
      m_client2.Call(Validate, 2, 2, false);

      //Close Clients.
      m_client1.Call(ClientDown);
      m_client2.Call(ClientDown);
      m_feeder.Call(ClientDown);
      Util.Log("Feeder and Clients closed.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    [Test]
    public void ConflationBasic()
    {
      runConflationBasic();
    }

  }
}

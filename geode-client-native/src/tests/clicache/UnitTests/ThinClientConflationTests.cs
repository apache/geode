//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  #region Listener
  class ConflationListner : ICacheListener
  {
    #region Private members

    private int m_events = 0;
    private int m_value = 0;

    #endregion

    public static ConflationListner Create()
    {
      Util.Log(" ConflationListner Created");

      return new ConflationListner();
    }

    private void check(EntryEvent ev)
    {
      m_events++;

      CacheableString key = ev.Key as CacheableString;
      CacheableInt32 value = ev.NewValue as CacheableInt32;

      m_value = value.Value;
      Util.Log("Region:{0}:: Key:{1}, Value:{2}",ev.Region.Name, key.Value, value.Value );

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

    public virtual void AfterCreate(EntryEvent ev)
    {
      check(ev);
    }

    public virtual void AfterUpdate(EntryEvent ev)
    {
      check(ev);
    }

    public virtual void AfterDestroy(EntryEvent ev) { }

    public virtual void AfterInvalidate(EntryEvent ev) { }

    public virtual void AfterRegionDestroy(RegionEvent ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent ev) { }

    public virtual void AfterRegionClear(RegionEvent ev) { }

    public virtual void AfterRegionLive(RegionEvent ev)
    {
      Util.Log("DurableListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public virtual void Close(Region region) { }
    public virtual void AfterRegionDisconnected(Region region){ }

    #endregion
  }
  #endregion

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientConflationTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2, m_feeder;
    private string[] keys = { "Key-1", "Key-2", "Key-3", "Key-4", "Key-5" };

    private static string[] DurableClientIds = { "DurableClientId1", "DurableClientId2" };

    static string[] Regions = {"ConflatedRegion", "NonConflatedRegion" };

    private static ConflationListner m_listener1C1, m_listener2C1, m_listener1C2, m_listener2C2;

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

    public void InitFeeder(string endpoints, string locators, int redundancyLevel,
      bool pool, bool locator)
    {
      if (pool)
      {
        CacheHelper.CreatePool("__TESTPOOL1_", endpoints, locators,
          (string)null, redundancyLevel, false);
        CacheHelper.CreateTCRegion_Pool(Regions[0], false, false, null,
          CacheHelper.Endpoints, CacheHelper.Locators, "__TESTPOOL1_", false);
        CacheHelper.CreateTCRegion_Pool(Regions[1], false, false, null,
          CacheHelper.Endpoints, CacheHelper.Locators, "__TESTPOOL1_", false);
      }
      else
      {
        CacheHelper.InitConfig(endpoints, redundancyLevel);
        CacheHelper.CreateTCRegion(Regions[0], false, false, null, CacheHelper.Endpoints, false);
        CacheHelper.CreateTCRegion(Regions[1], false, false, null, CacheHelper.Endpoints, false);
      }
    }

    public void InitDurableClient(int client, string endpoints, string locators,
      string conflation, bool pool, bool locator)
    {
      // Create DurableListener for first time and use same afterward.
      ConflationListner checker1 = null;
      ConflationListner checker2 = null;
      string durableId = ThinClientConflationTests.DurableClientIds[client -1 ]; 
      if (client == 1)
      {
        ThinClientConflationTests.m_listener1C1 = ConflationListner.Create();
        ThinClientConflationTests.m_listener2C1 = ConflationListner.Create();
        checker1 = ThinClientConflationTests.m_listener1C1;
        checker2 = ThinClientConflationTests.m_listener2C1;
      }
      else // client == 2 
      {
        ThinClientConflationTests.m_listener1C2 = ConflationListner.Create();
        ThinClientConflationTests.m_listener2C2 = ConflationListner.Create();
        checker1 = ThinClientConflationTests.m_listener1C2;
        checker2 = ThinClientConflationTests.m_listener2C2;
      }
      if (pool)
      {
        CacheHelper.InitConfigForConflation_Pool(endpoints, locators, durableId, conflation);
        CacheHelper.CreateTCRegion_Pool(Regions[0], false, true, checker1,
          CacheHelper.Endpoints, CacheHelper.Locators, "__TESTPOOL1_", true);
        CacheHelper.CreateTCRegion_Pool(Regions[1], false, true, checker2,
          CacheHelper.Endpoints, CacheHelper.Locators, "__TESTPOOL1_", true);
      }
      else
      {
        CacheHelper.InitConfigForConflation(endpoints, durableId, conflation);
        CacheHelper.CreateTCRegion(Regions[0], false, true, checker1, CacheHelper.Endpoints, true);
        CacheHelper.CreateTCRegion(Regions[1], false, true, checker2, CacheHelper.Endpoints, true);
      }

      //CacheHelper.DCache.ReadyForEvents();

      Region region1 = CacheHelper.GetVerifyRegion(Regions[0]);
      region1.RegisterAllKeys( true);
      Region region2 = CacheHelper.GetVerifyRegion(Regions[1]);
      region2.RegisterAllKeys(true);
    }

    public void ReadyForEvents()
    {
      CacheHelper.DCache.ReadyForEvents();
    }

    public void FeederUpdate(int keyIdx)
    {
      Region region1 = CacheHelper.GetVerifyRegion(Regions[0]);

      region1.Put(keys[keyIdx], new CacheableInt32(1));
      region1.Put(keys[keyIdx], new CacheableInt32(2));
      region1.Put(keys[keyIdx], new CacheableInt32(3));
      region1.Put(keys[keyIdx], new CacheableInt32(4));
      region1.Put(keys[keyIdx], new CacheableInt32(5));

      Region region2 = CacheHelper.GetVerifyRegion(Regions[1]);

      region2.Put(keys[keyIdx], new CacheableInt32(1));
      region2.Put(keys[keyIdx], new CacheableInt32(2));
      region2.Put(keys[keyIdx], new CacheableInt32(3));
      region2.Put(keys[keyIdx], new CacheableInt32(4));
      region2.Put(keys[keyIdx], new CacheableInt32(5));
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


    public void Validate(int client,int region, bool conflate)
    {
      ConflationListner checker = null;
      if (client == 1 )
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

    void runConflationBasic(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, "cacheserver_conflation.xml");

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

      m_feeder.Call(InitFeeder, CacheHelper.Endpoints, CacheHelper.Locators, 0, pool, locator);
      Util.Log("Feeder initialized.");

      //Test "true" and "false" settings
      m_client1.Call(InitDurableClient, 1, CacheHelper.Endpoints, CacheHelper.Locators,
        "true", pool, locator);
      m_client2.Call(InitDurableClient, 2, CacheHelper.Endpoints, CacheHelper.Locators,
        "false", pool, locator);
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
      m_client1.Call(InitDurableClient, 1, CacheHelper.Endpoints, CacheHelper.Locators,
        "server", pool, locator);
      m_client2.Call(InitDurableClient, 2, CacheHelper.Endpoints, CacheHelper.Locators,
        "", pool, locator);
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

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }

      CacheHelper.ClearLocators();
      CacheHelper.ClearEndpoints();
    }

    [Test]
    public void ConflationBasic()
    {
      runConflationBasic(false, false); // region config
      runConflationBasic(true, false); // pool with server endpoints
      runConflationBasic(true, true); // pool with locator
    }

  }
}

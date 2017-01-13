//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [TestFixture]
  [Category("generics")]
  public class CachelessTests : UnitTests
  {
    private const string RegionName = "DistRegionAck";
    private RegionWrapper m_regionw;
    private TallyListener<object, object> m_listener;

    private UnitProcess m_client1, m_client2, m_client3, m_client4;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      m_client4 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3, m_client4 };
    }

    public void CreateRegion(string locators,
      bool caching, bool listener)
    {
      if (listener)
      {
        m_listener = new TallyListener<object, object>();
      }
      else
      {
        m_listener = null;
      }
      IRegion<object, object> region = null;

      region = CacheHelper.CreateTCRegion_Pool<object, object>(RegionName, true, caching,
        m_listener, locators, "__TESTPOOL1_", true);
      m_regionw = new RegionWrapper(region);
    }

    public void NoEvents()
    {
      Util.Log("Verifying TallyListener has received nothing.");
      Assert.AreEqual(0, m_listener.Creates, "Should be no creates");
      Assert.AreEqual(0, m_listener.Updates, "Should be no updates");
      Assert.IsNull(m_listener.LastKey, "Should be no key");
      Assert.IsNull(m_listener.LastValue, "Should be no value");
    }

    public void SendPut(int key, int val)
    {
      m_regionw.Put(key, val);
    }



    public void CheckEmpty()
    {
      Util.Log("check s2p2-subset is still empty.");
      Thread.Sleep(100); //let it do receiving...
      m_regionw.Test(1, -1);
      Assert.AreEqual(0, m_listener.Creates, "Should be no creates");
      Assert.AreEqual(0, m_listener.Updates, "Should be no updates");
      m_regionw.Put(2, 1);
      Assert.AreEqual(1, m_listener.ExpectCreates(1), "Should have been 1 create.");
      Assert.AreEqual(0, m_listener.ExpectUpdates(0), "Should be no updates");
    }

    void runCacheless()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      Util.Log("Creating region in s1p1-pusher, no-ack, no-cache,  no-listener");
      m_client1.Call(CreateRegion, CacheHelper.Locators,
        false, true);

      Util.Log("Creating region in s1p2-listener, no-ack, no-cache, with-listener");
      m_client2.Call(CreateRegion, CacheHelper.Locators,
        false, true);

      Util.Log("Creating region in s2p1-storage, no-ack, cache, no-listener");
      m_client3.Call(CreateRegion, CacheHelper.Locators,
        true, true);

      Util.Log("Creating region in s2p2-subset, no-ack, cache,  no-listener");
      m_client4.Call(CreateRegion, CacheHelper.Locators,
        true, true);

      Util.Log("createRegion seems to return before peers have handshaked... waiting a while.");
      Thread.Sleep(10000);
      Util.Log("tired of waiting....");

      m_client2.Call(NoEvents);

      Util.Log("put(1,1) from s1p1-pusher");
      m_client1.Call(SendPut, 1, 1);

      Util.Log("update from s2p1-storage");
      m_client3.Call(SendPut, 1, 2);

      m_client2.Call(CheckEmpty);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);
      m_client3.Call(CacheHelper.Close);
      m_client4.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    [Test]
    public void Cacheless()
    {
      runCacheless();
    }
  }
}

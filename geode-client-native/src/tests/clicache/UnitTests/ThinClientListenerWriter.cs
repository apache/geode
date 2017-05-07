//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientListenerWriter : ThinClientRegionSteps
  {
    private TallyWriter m_writer;
    private TallyListener m_listener;
    RegionOperation o_region;

    private UnitProcess m_client1, m_client2, m_client3;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
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

      if (writer)
      {
        m_writer = new TallyWriter();
        AttributesMutator at = region.GetAttributesMutator();
        at.SetCacheWriter(m_writer);
      }

    }

    public void ValidateEvents()
    {
      Thread.Sleep(5000);
      m_listener.ShowTallies();
      m_writer.ShowTallies();
      Assert.AreEqual(0, m_listener.Creates, "Should be 0 creates");
      Assert.AreEqual(0, m_listener.Updates, "Should be 0 updates");
      Assert.AreEqual(10, m_listener.Invalidates, "Should be 10 invalidates");
      Assert.AreEqual(5, m_listener.Destroys, "Should be 5 destroys");
      Assert.AreEqual(false, m_writer.IsWriterInvoked, "Writer Should not be invoked");
    }

    public void ValidateListenerWriterWithNBSTrue()
    {
      Thread.Sleep(5000);
      m_listener.ShowTallies();
      Assert.AreEqual(5, m_listener.Creates, "Should be 5 creates");
      Assert.AreEqual(5, m_listener.Updates, "Should be 5 updates");
      Assert.AreEqual(0, m_listener.Invalidates, "Should be 0 invalidates");
      Assert.AreEqual(5, m_listener.Destroys, "Should be 5 destroys");
      Assert.AreEqual(false, m_writer.IsWriterInvoked, "Writer should not be invoked");
    }

    public void RegisterAllKeysRN()
    {
      Region region = CacheHelper.GetVerifyRegion(RegionName);
      region.RegisterAllKeys(false, null, false, false);
    }

    public void CallOp()
    {
      o_region = new RegionOperation(RegionName);
      o_region.PutOp(5, null);
      Thread.Sleep(1000); // let the events reach at other end.
      o_region.PutOp(5, null);
      Thread.Sleep(1000);
      o_region.InvalidateOp(5, null);
      Thread.Sleep(1000);
      o_region.DestroyOp(5, null);
      Thread.Sleep(1000);
    }

    void runThinClientListenerWriterTest(bool pool, bool locator)
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

      m_client1.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client1, no-ack, no-cache,  no-listener and no-writer");
      m_client1.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        false, false, false, pool, locator);

      m_client2.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client2 , no-ack, no-cache, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        false, true, true, pool, locator);
      m_client2.Call(RegisterAllKeys, new string[] { RegionName });

      m_client1.Call(CallOp);

      m_client2.Call(ValidateListenerWriterWithNBSTrue);

      m_client1.Call(CacheHelper.CloseCache);

      m_client2.Call(CacheHelper.CloseCache);

      CacheHelper.StopJavaServer(1);

      CacheHelper.SetupJavaServers(pool && locator, "cacheserver.xml");
      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, no-cache,  no-listener and no-writer");
      m_client1.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        false, false, false, pool, locator);

      Util.Log("Creating region in client2 , no-ack, no-cache, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        false, true, true, pool, locator);

      m_client3.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client2 , no-ack, no-cache, with listener and writer");
      m_client3.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
        false, true, true, pool, locator);

      m_client2.Call(RegisterAllKeysRN);

      m_client3.Call(RegisterAllKeysRN);

      m_client1.Call(CallOp);

      m_client2.Call(ValidateEvents);

      m_client3.Call(ValidateEvents);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);
      m_client3.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);

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
    public void ThinClientListenerWriterTest()
    {
      runThinClientListenerWriterTest(false, false); //region config
      runThinClientListenerWriterTest(true, false); //pool with server endpoints
      runThinClientListenerWriterTest(true, true); //pool with locator
    }
  }
}

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
  using GemStone.GemFire.Cache.UnitTests.NewAPI;
  using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientListenerWriter : ThinClientRegionSteps
  {
    private TallyWriter<object, object> m_writer;
    private TallyListener<object, object> m_listener;
    RegionOperation o_region;

    private UnitProcess m_client1, m_client2, m_client3;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
    }

    public void CreateRegion(string locators,
      bool caching, bool listener, bool writer)
    {
      if (listener)
      {
        m_listener = new TallyListener<object, object>();
      }
      else
      {
        m_listener = null;
      }
      Region region = null;
      region = CacheHelper.CreateTCRegion_Pool<object, object>(RegionName, true, caching,
        m_listener, locators, "__TESTPOOL1_", true);

      if (writer)
      {
        m_writer = new TallyWriter<object, object>();
        AttributesMutator<object, object> at = region.AttributesMutator;
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
      Assert.AreEqual(20, m_listener.Invalidates, "Should be 20 invalidates");
      Assert.AreEqual(5, m_listener.Destroys, "Should be 5 destroys");
      Assert.AreEqual(false, m_writer.IsWriterInvoked, "Writer Should not be invoked");
    }

    public void ValidateGetEvents(int creates, int updates)
    {
      Thread.Sleep(1000);
      m_listener.ShowTallies();
      Assert.AreEqual(creates, m_listener.Creates, "Incorrect creates");
      Assert.AreEqual(updates, m_listener.Updates, "Incorrect updates");
    }

    public void ValidateListenerWriterWithNBSTrue()
    {
      Thread.Sleep(5000);
      m_listener.ShowTallies();

      Assert.AreEqual(10, m_listener.Creates, "Should be 10 creates");
      Assert.AreEqual(10, m_listener.Updates, "Should be 10 updates");
      Assert.AreEqual(0, m_listener.Invalidates, "Should be 0 invalidates");
      Assert.AreEqual(5, m_listener.Destroys, "Should be 5 destroys");
      Assert.AreEqual(false, m_writer.IsWriterInvoked, "Writer should not be invoked");
    }

    public void RegisterAllKeysRN()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      region.GetSubscriptionService().RegisterAllKeys(false, null, false, false);
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

    public void PutOp(string key, string value)
    {
      Util.Log("PutOp started");
      o_region = new RegionOperation(RegionName);
      Region r = o_region.Region;
      r[key] = value;
      Thread.Sleep(1000); // let the events reach at other end.
      Util.Log("PutOp finished");
    }

    public void InvalidateOp(string key)
    {
      Util.Log("InvalidateOp started");
      o_region = new RegionOperation(RegionName);
      Region r = o_region.Region;
      r.GetLocalView().Invalidate(key);
      Thread.Sleep(1000); // let the events reach at other end.
      Util.Log("InvalidateOp finished");
    }

    public void GetOp(string key)
    {
      Util.Log("GetOp started");
      o_region = new RegionOperation(RegionName);
      Region r = o_region.Region;
      Object val  = r[key];
      Thread.Sleep(1000); // let the events reach at other end.
      Util.Log("GetOp finished");
    }

    public void registerPdxType8()
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
    }
    void runThinClientListenerWriterTest()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client1, no-ack, no-cache,  no-listener and no-writer");
      m_client1.Call(CreateRegion, CacheHelper.Locators,
        false, false, false);

      m_client2.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client2 , no-ack, no-cache, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Locators,
        false, true, true);

      m_client1.Call(registerPdxType8);
      m_client2.Call(registerPdxType8);

      m_client2.Call(RegisterAllKeys, new string[] { RegionName });

      m_client1.Call(CallOp);

      m_client2.Call(ValidateListenerWriterWithNBSTrue);

      m_client1.Call(CacheHelper.CloseCache);

      m_client2.Call(CacheHelper.CloseCache);

      CacheHelper.StopJavaServer(1);

      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, no-cache,  no-listener and no-writer");
      m_client1.Call(CreateRegion, CacheHelper.Locators,
        false, false, false);

      Util.Log("Creating region in client2 , no-ack, no-cache, with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Locators,
        false, true, true);

      m_client3.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client2 , no-ack, no-cache, with listener and writer");
      m_client3.Call(CreateRegion, CacheHelper.Locators,
        false, true, true);

      m_client1.Call(registerPdxType8);
      m_client2.Call(registerPdxType8);

      m_client2.Call(RegisterAllKeysRN);

      m_client3.Call(RegisterAllKeysRN);

      m_client1.Call(CallOp);

      m_client2.Call(ValidateEvents);

      m_client3.Call(ValidateEvents);

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);
      m_client3.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);

      /*  Bug #381   */
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      Util.Log("Creating region in client1, no-ack, no-cache,  no-listener and no-writer");
      m_client1.Call(CreateRegion,CacheHelper.Locators,
        false, false, false);

      Util.Log("Creating region in client2 , with listener and writer");
      m_client2.Call(CreateRegion, CacheHelper.Locators,
        true, true, true);

      m_client3.Call(CacheHelper.InitClient);
      Util.Log("Creating region in client3 , with listener and writer");
      m_client3.Call(CreateRegion, CacheHelper.Locators,
        true, true, true);

      m_client2.Call(RegisterAllKeysRN);
      m_client1.Call(PutOp, "Key-1", "Value-1");
      m_client2.Call(GetOp, "Key-1");
      m_client2.Call(ValidateGetEvents, 0, 1);
      Util.Log("ValidateGetEvents 1 done. ");

      m_client3.Call(RegisterAllKeysRN);
      m_client3.Call(GetOp, "Key-1");
      m_client3.Call(ValidateGetEvents, 1, 0);
      Util.Log("ValidateGetEvents 2 done. ");

      m_client2.Call(PutOp, "Key-2", "Value-2");
      m_client2.Call(InvalidateOp, "Key-2");
      m_client2.Call(GetOp, "Key-2");
      m_client2.Call(ValidateGetEvents, 1, 2);
      Util.Log("ValidateGetEvents 3 done. ");

      m_client1.Call(CacheHelper.Close);
      m_client2.Call(CacheHelper.Close);
      m_client3.Call(CacheHelper.Close);

      CacheHelper.StopJavaServer(1);

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
    public void ThinClientListenerWriterTest()
    {
      runThinClientListenerWriterTest();
    }
  }
}

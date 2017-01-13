//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  using GIRegion = GemStone.GemFire.Cache.Generic.IRegion<string, string>;

  class ThinClientTallyLoader : TallyLoader<string, string>
  {
    // Note this just returns default(TVal) hence null or empty string, use
    // GetLoadCount() to get the load count int value.
    public override string Load(GIRegion region, string key, object callbackArg)
    {
      base.Load(region, key, callbackArg);
      int loadCount = GetLoadCount();
      if (key != null)
      {
        Util.Log(Util.LogLevel.Debug,
          "Putting the value ({0}) for local region clients only.",
          loadCount);
        region[key] = loadCount.ToString();
      }
      return loadCount.ToString();
    }
  }

  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("generics")]
  public class AttributesMutatorTests : ThinClientRegionSteps
  {
    private UnitProcess m_client1, m_client2;
    private const string Key = "one";
    private const int Val = 1;
    private const int TimeToLive = 5;
    private const string PeerRegionName = "PEER1";
    TallyListener<string, string> m_reg1Listener1, m_reg1Listener2;
    TallyListener<string, string> m_reg2Listener1, m_reg2Listener2;
    TallyListener<string, string> m_reg3Listener1, m_reg3Listener2;
    TallyLoader<string, string> m_reg1Loader1, m_reg1Loader2;
    TallyLoader<string, string> m_reg2Loader1, m_reg2Loader2;
    TallyLoader<string, string> m_reg3Loader1, m_reg3Loader2;
    TallyWriter<string, string> m_reg1Writer1, m_reg1Writer2;
    TallyWriter<string, string> m_reg2Writer1, m_reg2Writer2;
    TallyWriter<string, string> m_reg3Writer1, m_reg3Writer2;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    protected override string ExtraPropertiesFile
    {
      get
      {
        return null;
      }
    }

    #region Private methods

    private void SetCacheListener(string regionName, ICacheListener<string, string> listener)
    {
      GIRegion region = CacheHelper.GetVerifyRegion<string, string>(regionName);
      AttributesMutator<string, string> attrMutator = region.AttributesMutator;
      attrMutator.SetCacheListener(listener);
    }

    private void SetCacheLoader(string regionName, ICacheLoader<string, string> loader)
    {
      GIRegion region = CacheHelper.GetVerifyRegion<string, string>(regionName);
      AttributesMutator<string, string> attrMutator = region.AttributesMutator;
      attrMutator.SetCacheLoader(loader);
    }

    private void SetCacheWriter(string regionName, ICacheWriter<string, string> writer)
    {
      GIRegion region = CacheHelper.GetVerifyRegion<string, string>(regionName);
      AttributesMutator<string, string> attrMutator = region.AttributesMutator;
      attrMutator.SetCacheWriter(writer);
    }

    private void RegisterKeys()
    {
      GIRegion region0 = CacheHelper.GetRegion<string, string>(m_regionNames[0]);
      GIRegion region1 = CacheHelper.GetRegion<string, string>(m_regionNames[1]);
      string cKey1 = m_keys[1];
      string cKey2 = m_keys[3];
      region0.GetSubscriptionService().RegisterKeys(new string[] { cKey1 });
      region1.GetSubscriptionService().RegisterKeys(new string[] { cKey2 });
    }

    #endregion

    #region Public methods invoked by the tests

    public void StepOneCallbacks()
    {
      m_reg1Listener1 = new TallyListener<string, string>();
      m_reg2Listener1 = new TallyListener<string, string>();
      m_reg1Loader1 = new ThinClientTallyLoader();
      m_reg2Loader1 = new ThinClientTallyLoader();
      m_reg1Writer1 = new TallyWriter<string, string>();
      m_reg2Writer1 = new TallyWriter<string, string>();

      SetCacheListener(RegionNames[0], m_reg1Listener1);
      SetCacheLoader(RegionNames[0], m_reg1Loader1);
      SetCacheWriter(RegionNames[0], m_reg1Writer1);

      SetCacheListener(RegionNames[1], m_reg2Listener1);
      SetCacheLoader(RegionNames[1], m_reg2Loader1);
      SetCacheWriter(RegionNames[1], m_reg2Writer1);

      RegisterKeys();
      m_reg3Listener1 = new TallyListener<string, string>();
      //m_reg3Loader1 = new TallyLoader<string, string>();
      m_reg3Loader1 = new ThinClientTallyLoader();
      m_reg3Writer1 = new TallyWriter<string, string>();
      AttributesFactory<string, string> af = new AttributesFactory<string, string>();

      GIRegion region = CacheHelper.CreateRegion<string, string>(PeerRegionName,
        af.CreateRegionAttributes());

      SetCacheListener(PeerRegionName, m_reg3Listener1);
      SetCacheLoader(PeerRegionName, m_reg3Loader1);
      SetCacheWriter(PeerRegionName, m_reg3Writer1);

      // Call the loader and writer
      Assert.IsNotNull(GetEntry(RegionNames[0], m_keys[0]),
        "Found null value.");
      Assert.IsNotNull(GetEntry(RegionNames[1], m_keys[0]),
        "Found null value.");
      Assert.IsNotNull(GetEntry(PeerRegionName, m_keys[0]),
              "Found null value.");

      CreateEntry(PeerRegionName, m_keys[1], m_vals[1]);
    }

    public void StepTwoCallbacks()
    {
      AttributesFactory<string, string> af = new AttributesFactory<string, string>();

      GIRegion region = CacheHelper.CreateRegion<string, string>(PeerRegionName,
        af.CreateRegionAttributes());

      CreateEntry(RegionNames[0], m_keys[1], m_vals[1]);
      CreateEntry(RegionNames[1], m_keys[3], m_vals[3]);

      SetCacheLoader(RegionNames[0], new ThinClientTallyLoader());
      SetCacheLoader(RegionNames[1], new ThinClientTallyLoader());

      Assert.IsNotNull(GetEntry(RegionNames[0], m_keys[0]),
        "Found null value in region0.");
      Assert.IsNotNull(GetEntry(RegionNames[1], m_keys[0]),
        "Found null value in region1.");

    }

    public void StepThreeCallbacks()
    {
      Assert.AreEqual(2, m_reg1Listener1.ExpectCreates(2),
        "Two creation events were expected for region1.");
      Assert.AreEqual(2, m_reg2Listener1.ExpectCreates(2),
        "Two creation events were expected for region2.");

      Assert.AreEqual(0, m_reg1Listener1.ExpectUpdates(0),
        "No update event was expected for region1.");
      Assert.AreEqual(0, m_reg2Listener1.ExpectUpdates(0),
        "No update event was expected for region2.");

      Assert.AreEqual(1, m_reg1Loader1.ExpectLoads(1),
        "One loader event was expected for region1.");
      Assert.AreEqual(1, m_reg2Loader1.ExpectLoads(1),
        "One loader event was expected for region2.");

      Assert.AreEqual(1, m_reg1Writer1.ExpectCreates(1),
        "One writer create event was expected for region1.");
      Assert.AreEqual(1, m_reg2Writer1.ExpectCreates(1),
        "One writer create event was expected for region2.");
    }

    public void StepFourCallbacks()
    {
      UpdateEntry(m_regionNames[0], m_keys[1], m_nvals[1], true);
      UpdateEntry(m_regionNames[1], m_keys[3], m_nvals[3], true);
    }

    public void StepFiveCallbacks()
    {
      Assert.AreEqual(1, m_reg1Listener1.Updates,
        "One update event was expected for region.");
      Assert.AreEqual(1, m_reg2Listener1.Updates,
        "One update event was expected for region.");

      m_reg1Listener2 = new TallyListener<string, string>();
      m_reg2Listener2 = new TallyListener<string, string>();
      m_reg1Loader2 = new ThinClientTallyLoader();
      m_reg2Loader2 = new ThinClientTallyLoader();
      m_reg1Writer2 = new TallyWriter<string, string>();
      m_reg2Writer2 = new TallyWriter<string, string>();

      SetCacheListener(RegionNames[0], m_reg1Listener2);
      SetCacheLoader(RegionNames[0], m_reg1Loader2);
      SetCacheWriter(RegionNames[0], m_reg1Writer2);

      SetCacheListener(RegionNames[1], m_reg2Listener2);
      SetCacheLoader(RegionNames[1], m_reg2Loader2);
      SetCacheWriter(RegionNames[1], m_reg2Writer2);

      m_reg3Listener2 = new TallyListener<string, string>();
      //m_reg3Loader2 = new TallyLoader<string, string>();
      m_reg3Loader2 = new ThinClientTallyLoader();
      m_reg3Writer2 = new TallyWriter<string, string>();

      SetCacheListener(PeerRegionName, m_reg3Listener2);
      SetCacheLoader(PeerRegionName, m_reg3Loader2);
      SetCacheWriter(PeerRegionName, m_reg3Writer2);

      // Force a fresh key get to trigger the new loaders
      Assert.IsNotNull(GetEntry(RegionNames[0], m_keys[2]),
        "Found null value.");
      Assert.IsNotNull(GetEntry(RegionNames[1], m_keys[2]),
        "Found null value.");
    }



    #endregion

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    [Test]
    public void CreateAndVerifyExpiry()
    {
      try
      {
        IRegion<string, int> region = CacheHelper.CreateLocalRegionWithETTL<string, int>(RegionName,
          ExpirationAction.LocalInvalidate, TimeToLive);

        GemStone.GemFire.Cache.Generic.RegionAttributes<string, int> newAttrs = region.Attributes;
        int ttl = newAttrs.EntryTimeToLive;
        Assert.AreEqual(TimeToLive, ttl);

        region[Key] = Val;

        // countdown begins... it is ttl so access should not play into it..
        Thread.Sleep(2000); // sleep for some time, expect value to still be there.
        int res = region[Key];
        Assert.IsNotNull(res);
        Assert.AreEqual(Val, res);
        Thread.Sleep(6000); // sleep for 6 more seconds, expect value to be invalid.
        bool containsKey = region.ContainsValueForKey(Key);
        Assert.IsFalse(containsKey, "The region should not contain the key");
      }
      finally
      {
        CacheHelper.Close();
      }
    }

    [Test]
    public void Callbacks()
    {
      CacheHelper.SetupJavaServers("cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      try
      {
        m_client1.Call(CreateTCRegions, RegionNames, CacheHelper.Locators, true);
        m_client1.Call(StepOneCallbacks);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames, CacheHelper.Locators, true);
        m_client2.Call(StepTwoCallbacks);
        Util.Log("StepTwo complete.");

        m_client1.Call(StepThreeCallbacks);
        Util.Log("StepThree complete.");

        m_client2.Call(StepFourCallbacks);
        Util.Log("StepFour complete.");

        m_client1.Call(StepFiveCallbacks);
        Util.Log("StepFive complete.");
      }
      finally
      {
        CacheHelper.StopJavaServer(1);
        Util.Log("Cacheserver 1 stopped.");
        m_client2.Call(DestroyRegions);
        CacheHelper.StopJavaLocator(1);
        CacheHelper.ClearEndpoints();
      }
    }
  }
}

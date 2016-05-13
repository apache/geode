//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  class ThinClientTallyLoader : TallyLoader
  {
    public override IGFSerializable Load(Region region, ICacheableKey key,
      IGFSerializable helper)
    {
      int loadValue = ((CacheableInt32)base.Load(region, key, helper)).Value;
      CacheableString returnValue = new CacheableString(loadValue.ToString());
      if (key != null && region.Attributes.Endpoints != null)
      {
        Util.Log(Util.LogLevel.Debug,
          "Putting the value ({0}) for local region clients only.",
          returnValue.Value);
        region.Put(key, returnValue);
      }
      return returnValue;
    }
  }

  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class AttributesMutatorTests : ThinClientRegionSteps
  {
    private UnitProcess m_client1, m_client2;
    private const string Key = "one";
    private const int Val = 1;
    private const int TimeToLive = 5;
    private const string PeerRegionName = "PEER1";
    TallyListener m_reg1Listener1, m_reg1Listener2;
    TallyListener m_reg2Listener1, m_reg2Listener2;
    TallyListener m_reg3Listener1, m_reg3Listener2;
    TallyLoader m_reg1Loader1, m_reg1Loader2;
    TallyLoader m_reg2Loader1, m_reg2Loader2;
    TallyLoader m_reg3Loader1, m_reg3Loader2;
    TallyWriter m_reg1Writer1, m_reg1Writer2;
    TallyWriter m_reg2Writer1, m_reg2Writer2;
    TallyWriter m_reg3Writer1, m_reg3Writer2;

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

    private void SetCacheListener(string regionName, ICacheListener listener)
    {
      Region region = CacheHelper.GetVerifyRegion(regionName);
      AttributesMutator attrMutator = region.GetAttributesMutator();
      attrMutator.SetCacheListener(listener);
    }

    private void SetCacheLoader(string regionName, ICacheLoader loader)
    {
      Region region = CacheHelper.GetVerifyRegion(regionName);
      AttributesMutator attrMutator = region.GetAttributesMutator();
      attrMutator.SetCacheLoader(loader);
    }

    private void SetCacheWriter(string regionName, ICacheWriter writer)
    {
      Region region = CacheHelper.GetVerifyRegion(regionName);
      AttributesMutator attrMutator = region.GetAttributesMutator();
      attrMutator.SetCacheWriter(writer);
    }

    private void RegisterKeys()
    {
      Region region0 = CacheHelper.GetRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetRegion(m_regionNames[1]);
      CacheableKey cKey1 = m_keys[1];
      CacheableKey cKey2 = m_keys[3];
      region0.RegisterKeys(new CacheableKey[] { cKey1 });
      region1.RegisterKeys(new CacheableKey[] { cKey2 });
    }

    #endregion

    #region Public methods invoked by the tests

    public void StepOneCallbacks()
    {
      m_reg1Listener1 = new TallyListener();
      m_reg2Listener1 = new TallyListener();
      m_reg1Loader1 = new ThinClientTallyLoader();
      m_reg2Loader1 = new ThinClientTallyLoader();
      m_reg1Writer1 = new TallyWriter();
      m_reg2Writer1 = new TallyWriter();

      SetCacheListener(RegionNames[0], m_reg1Listener1);
      SetCacheLoader(RegionNames[0], m_reg1Loader1);
      SetCacheWriter(RegionNames[0], m_reg1Writer1);

      SetCacheListener(RegionNames[1], m_reg2Listener1);
      SetCacheLoader(RegionNames[1], m_reg2Loader1);
      SetCacheWriter(RegionNames[1], m_reg2Writer1);

      RegisterKeys();
      m_reg3Listener1 = new TallyListener();
      m_reg3Loader1 = new TallyLoader();
      m_reg3Writer1 = new TallyWriter();
      AttributesFactory af = new AttributesFactory();

      af.SetScope(ScopeType.Local);
      Region region = CacheHelper.CreateRegion(PeerRegionName,
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
      AttributesFactory af = new AttributesFactory();

      af.SetScope(ScopeType.Local);
      Region region = CacheHelper.CreateRegion(PeerRegionName,
        af.CreateRegionAttributes());

      CreateEntry(RegionNames[0], m_keys[1], m_vals[1]);
      CreateEntry(RegionNames[1], m_keys[3], m_vals[3]);

      Assert.IsNotNull(GetEntry(RegionNames[0], m_keys[0]),
        "Found null value.");
      Assert.IsNotNull(GetEntry(RegionNames[1], m_keys[0]),
        "Found null value.");

    }

    public void StepThreeCallbacks()
    {
      Assert.AreEqual(2, m_reg1Listener1.Creates,
        "Two creation events were expected for region.");
      Assert.AreEqual(2, m_reg2Listener1.Creates,
        "Two creation events were expected for region.");

      Assert.AreEqual(0, m_reg1Listener1.Updates,
        "No update event was expected for region.");
      Assert.AreEqual(0, m_reg2Listener1.Updates,
        "No update event was expected for region.");

      Assert.AreEqual(1, m_reg1Loader1.Loads,
        "One loader event was expected for region.");
      Assert.AreEqual(1, m_reg2Loader1.Loads,
        "One loader event was expected for region.");

      Assert.AreEqual(1, m_reg1Writer1.Creates,
        "Two writer create events were expected for region.");
      Assert.AreEqual(1, m_reg2Writer1.Creates,
        "Two writer create events were expected for region.");
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

      m_reg1Listener2 = new TallyListener();
      m_reg2Listener2 = new TallyListener();
      m_reg1Loader2 = new ThinClientTallyLoader();
      m_reg2Loader2 = new ThinClientTallyLoader();
      m_reg1Writer2 = new TallyWriter();
      m_reg2Writer2 = new TallyWriter();

      SetCacheListener(RegionNames[0], m_reg1Listener2);
      SetCacheLoader(RegionNames[0], m_reg1Loader2);
      SetCacheWriter(RegionNames[0], m_reg1Writer2);

      SetCacheListener(RegionNames[1], m_reg2Listener2);
      SetCacheLoader(RegionNames[1], m_reg2Loader2);
      SetCacheWriter(RegionNames[1], m_reg2Writer2);

      m_reg3Listener2 = new TallyListener();
      m_reg3Loader2 = new TallyLoader();
      m_reg3Writer2 = new TallyWriter();

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
        AttributesFactory af = new AttributesFactory();
        af.SetEntryTimeToLive(ExpirationAction.LocalInvalidate, TimeToLive);
        af.SetScope(ScopeType.Local);
        Region region = CacheHelper.CreateRegion(RegionName,
          af.CreateRegionAttributes());

        RegionAttributes newAttrs = region.Attributes;
        int ttl = newAttrs.EntryTimeToLive;
        Assert.AreEqual(TimeToLive, ttl);

        region.Put(Key, Val);

        // countdown begins... it is ttl so access should not play into it..
        Thread.Sleep(2000); // sleep for some time, expect value to still be there.
        CacheableInt32 res = region.Get(Key) as CacheableInt32;
        Assert.IsNotNull(res);
        Assert.AreEqual(Val, res.Value);
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
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      try
      {
        m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
        m_client1.Call(StepOneCallbacks);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
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
        CacheHelper.ClearEndpoints();
      }
    }
  }
}

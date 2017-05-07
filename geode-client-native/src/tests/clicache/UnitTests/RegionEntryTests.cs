//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("unicast_only")]
  public class RegionEntryTests : UnitTests
  {
    private const string hostName = "REGIONENTRYTEST";
    private const string regionName = "TESTREGIONENTRY_ROOT_REGION";
    private Region region;

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
      CacheHelper.InitName(hostName, hostName);
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      try
      {
        CacheHelper.Close();
      }
      finally
      {
        base.EndTests();
      }
    }

    public void TestEntries(Region region, int num)
    {
      string regionName = region.Name;
      Util.Log("Creating {0} entries in Region {1}", num, regionName);

      for (int i = 0; i < num; i++)
      {
        region.Create(regionName + ": " + i.ToString(),
          regionName + ": value of " + i.ToString());
      }
      ICacheableKey[] cKeys = region.GetKeys();
      IGFSerializable[] cValues = region.GetValues();
      Assert.AreEqual(num, cKeys.Length, "Number of keys in region is incorrect.");
      Assert.AreEqual(num, cValues.Length, "Number of values in region is incorrect.");

      foreach (ICacheableKey key in cKeys)
      {
        region.LocalInvalidate(key);
      }
      cKeys = region.GetKeys();
      cValues = region.GetValues();
      Assert.AreEqual(num, cKeys.Length, "Number of keys in region is incorrect after invalidate.");
      Assert.AreEqual(0, cValues.Length, "Number of values in region is incorrect after invalidate.");

      foreach (ICacheableKey key in cKeys)
      {
        region.LocalDestroy(key);
      }
      cKeys = region.GetKeys();
      cValues = region.GetValues();
      Assert.AreEqual(0, cKeys.Length, "Number of keys in region is incorrect after destroy.");
      Assert.AreEqual(0, cValues.Length, "Number of values in region is incorrect after destroy.");
    }

    [Test]
    public void RegionEntryFunction()
    {
      CacheHelper.CreatePlainRegion(regionName);
      region = CacheHelper.GetVerifyRegion(regionName);

      TestEntries(region, 10);
      TestEntries(region, 100);
      TestEntries(region, 10000);

      region.LocalDestroyRegion();
    }
  }
}

//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class RegionMapTests : UnitTests
  {
    protected override ClientBase[] GetClients()
    {
      return null;
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

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    [Test]
    public void RegionLRULastTen()
    {
      ICacheableKey[] cKeys;

      Region region = CacheHelper.CreateLRURegion(Util.CurrentTestName, 10);
      // put more than 10 items... verify limit is held.
      for (int i = 1; i <= 10; i++)
      {
        region.Put(i, "value of " + i.ToString());
        cKeys = region.GetKeys();
        Assert.AreEqual(i, cKeys.Length, "Incorrect number of entries.");
      }
      for (int i = 11; i <= 20; i++)
      {
        region.Put(i, "value of " + i.ToString());
        cKeys = region.GetKeys();
        CacheHelper.ShowKeys(cKeys);
        Assert.AreEqual(10, cKeys.Length, "Expected 10 entries.");
      }

      CacheableInt32 cKey;
      // verify it is the last 10 keys..
      cKeys = region.GetKeys();
      Assert.AreEqual(10, cKeys.Length, "Expected 10 entries.");
      int total = 0;
      int expected = 0;
      for (int i = 11; i <= 20; i++)
      {
        cKey = cKeys[i - 11] as CacheableInt32;
        expected += i;
        total += cKey.Value;
      }
      Assert.AreEqual(expected, total, "Checksum mismatch.");
    }

    [Test]
    public void RegionNoLRU()
    {
      ICacheableKey[] cKeys;
      AttributesFactory af = new AttributesFactory();
      af.SetScope(ScopeType.Local);
      RegionAttributes rattrs = af.CreateRegionAttributes();
      Region region = CacheHelper.CreateRegion(Util.CurrentTestName, rattrs);
      for (int i = 1; i <= 20; i++)
      {
        region.Put(i, "value of " + i.ToString());
        cKeys = region.GetKeys();
        CacheHelper.ShowKeys(cKeys);
        Assert.AreEqual(i, cKeys.Length, "Incorrect number of entries.");
      }
    }

    [Test]
    public void RegionLRULocal()
    {
      ICacheableKey[] cKeys;

      Region region = CacheHelper.CreateLRURegion(Util.CurrentTestName, 10);
      // put more than 10 items... verify limit is held.
      // TODO make this local scope and re-increase the iterations... would also like to time it.
      for (int i = 1; i <= 1000; i++)
      {
        region.Put(i, "value of " + i);
        cKeys = region.GetKeys();
        Assert.AreEqual((i <= 10 ? i : 10), cKeys.Length, "Incorrect number of entries.");
      }
    }

    [Test]
    public void RecentlyUsedBit()
    {
      // Put twenty items in region. LRU is set to 10.
      // So 10 through 19 should be in region (started at 0)
      // get 15 so it is marked recently used.
      // put 9 more...  check that 15 was skipped for eviction.
      // put 1 more...  15 should then have been evicted.
      ICacheableKey[] cKeys;

      Region region = CacheHelper.CreateLRURegion(Util.CurrentTestName, 10);
      // put more than 10 items... verify limit is held.
      for (int i = 1; i <= 20; i++)
      {
        region.Put(i, "value of " + i.ToString());
      }
      CacheableString cVal = region.Get(16) as CacheableString;
      Assert.IsNotNull(cVal, "expected to find key 16 in cache.");
      for (int i = 21; i <= 35; i++)
      {
        region.Put(i, "value of " + i.ToString());
        cKeys = region.GetKeys();
        CacheHelper.ShowKeys(cKeys);
        Assert.AreEqual(10, cKeys.Length, "Expected 10 entries.");
      }
      Assert.IsTrue(region.ContainsKey(16), "expected to find key 16 in cache.");

      region.Put(36, "value of 36");
      cKeys = region.GetKeys();
      CacheHelper.ShowKeys(cKeys);
      Assert.AreEqual(10, cKeys.Length, "Expected 10 entries.");

      Assert.IsFalse(region.ContainsKey(16), "16 should have been evicted.");
    }

    [Test]
    public void EmptiedMap()
    {
      ICacheableKey[] cKeys;

      Region region = CacheHelper.CreateLRURegion(Util.CurrentTestName, 10);
      for (int i = 1; i <= 10; i++)
      {
        region.Put(i, "value of " + i.ToString());
        cKeys = region.GetKeys();
        Assert.AreEqual(i, cKeys.Length, "Incorrect number of entries.");
      }
      for (int i = 1; i <= 10; i++)
      {
        region.Destroy(i);
        Util.Log("Removed key {0}", i);
      }
      cKeys = region.GetKeys();
      Assert.AreEqual(0, cKeys.Length, "Expected 0 entries.");
      for (int i = 20; i <= 40; i++)
      {
        region.Put(i, "value of " + i.ToString());
      }
      cKeys = region.GetKeys();
      Assert.AreEqual(10, cKeys.Length, "Expected 10 entries.");
    }
  }
}

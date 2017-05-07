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

  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ExpirationTests : UnitTests
  {
    #region Private members

    private Region m_region = null;
    private CacheableString m_key = new CacheableString("KeyA - 1");
    private CacheableString m_value = new CacheableString("Value - AAAAAAA");
    private string m_regionName = "R";

    #endregion

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
      Properties config = new Properties();
      CacheHelper.InitConfig(config);
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

    #region Private functions

    private int GetNumOfEntries()
    {
      try
      {
        IGFSerializable[] keys = m_region.GetKeys();
        if (keys != null)
        {
          return keys.Length;
        }
      }
      catch (RegionDestroyedException)
      {
      }
      return 0;
    }

    private void DoNPuts(int n)
    {
      for (int index = 0; index < n; index++)
      {
        m_region.Put(string.Format("KeyA - {0}", index + 1), m_value);
      }
    }

    private void Do1Put()
    {
      m_region.Put(m_key, m_value);
    }

    private void SetupRegion(uint entryTTL, uint entryIdleTimeout,
      uint regionTTL, uint regionIdleTimeout)
    {
      AttributesFactory afact = new AttributesFactory();
      const ExpirationAction action = ExpirationAction.Destroy;

      afact.SetEntryTimeToLive(action, entryTTL);
      afact.SetEntryIdleTimeout(action, entryIdleTimeout);
      afact.SetRegionTimeToLive(action, regionTTL);
      afact.SetRegionIdleTimeout(action, regionIdleTimeout);
      afact.SetScope(ScopeType.Local);
      RegionAttributes attrs = afact.CreateRegionAttributes();
      m_region = CacheHelper.CreateRegion(m_regionName, attrs);
    }

    private void PutKeyTouch(int sleepSecs)
    {
      Do1Put();
      Thread.Sleep(sleepSecs * 1000);
      m_region.Get(m_key);
    }

    private void PutNKeysNoExpire(int numKeys, int sleepSecs)
    {
      int n;

      DoNPuts(numKeys);
      Thread.Sleep(sleepSecs * 1000);
      if (numKeys > 0)
      {
        n = GetNumOfEntries();
        Assert.AreEqual(numKeys, n, "Expected " + numKeys + " entries");
      }
    }

    private void PutNKeysExpire(int numKeys, int sleepSecs)
    {
      int n;

      DoNPuts(numKeys);
      Thread.Sleep(sleepSecs * 1000);
      n = GetNumOfEntries();
      Assert.AreEqual(0, n, "Expected 0 entry");
    }

    private void CheckRegion(bool expectedDead, int sleepSecs)
    {
      if (sleepSecs > 0)
      {
        Thread.Sleep(sleepSecs * 1000);
      }
      if (expectedDead)
      {
        Assert.IsTrue(m_region.IsDestroyed, "Expected {0} to be dead",
          m_regionName);
      }
      else
      {
        Assert.IsFalse(m_region.IsDestroyed, "Expected {0} to be alive",
          m_regionName);
      }
    }

    #endregion

    /*
    [Test]
    public void KeyDestroy()
    {
      m_regionName = "RT1";
      SetupRegion(0, 0, 0, 0);
      PutKeyTouch(5);
      IGFSerializable val = m_region.Get(m_key);
      m_region.Destroy(m_key);
      val = m_region.Get(m_key);
      CheckRegion(false, 0);

      m_regionName = "RT2";
      SetupRegion(0, 0, 0, 0);
      PutNKeysNoExpire(5, 1);
      m_region.DestroyRegion();
      CheckRegion(true, 0);

      m_regionName = "RT3";
      SetupRegion(0, 0, 0, 0);
      PutKeyTouch(5);
      val = m_region.Get(m_key);
      CheckRegion(false, 0);
      m_region.DestroyRegion();
      try
      {
        val = m_region.Get(m_key);
        Util.Log("The key fetched has value: {0}", val);
        Assert.Fail("The region should have been destroyed.");
      }
      catch (RegionDestroyedException)
      {
      }
      CheckRegion(true, 0);
    }
     * */

    [Test]
    public void KeyExpiration()
    {
      m_regionName = "R1";
      SetupRegion(0, 0, 0, 0);
      PutNKeysNoExpire(100, 10);
      CheckRegion(false, 0);

      m_regionName = "R2";
      SetupRegion(20, 2, 0, 0);
      PutNKeysNoExpire(1, 5);
      CheckRegion(false, 0);

      m_regionName = "R3";
      SetupRegion(5, 0, 0, 0);
      PutNKeysExpire(1, 10);
      CheckRegion(false, 0);

      m_regionName = "R4";
      SetupRegion(0, 5, 0, 0);
      PutKeyTouch(2);
      Thread.Sleep(3000);
      Assert.AreEqual(1, GetNumOfEntries(), "Expected 1 entry");
      Thread.Sleep(5000);
      Assert.AreEqual(0, GetNumOfEntries(), "Expected 0 entry");
      CheckRegion(false, 0);

      m_regionName = "R5";
      SetupRegion(10, 0, 0, 0);
      PutKeyTouch(5);
      PutNKeysNoExpire(0, 6);
      CheckRegion(false, 0);

      m_regionName = "R6";
      SetupRegion(5, 0, 0, 0);
      PutNKeysExpire(1, 6);
      CheckRegion(false, 0);

      m_regionName = "R7";
      SetupRegion(0, 5, 0, 0);
      PutKeyTouch(2);
      m_region.Destroy(m_key);
      CheckRegion(false, 5);

      m_regionName = "R8";
      SetupRegion(6, 3, 0, 0);
      PutNKeysNoExpire(1, 4);
      Thread.Sleep(4000);
      Assert.AreEqual(0, GetNumOfEntries(), "ttl is over so it should be 0");
      CheckRegion(false, 5);
    }

    [Test]
    public void RegionExpiration()
    {
      m_regionName = "RR1";
      SetupRegion(0, 0, 20, 2);
      CheckRegion(false, 5);

      m_regionName = "RR2";
      SetupRegion(0, 0, 5, 0);
      PutNKeysNoExpire(1, 2);
      DoNPuts(1);
      CheckRegion(true, 7);

      m_regionName = "RR3";
      SetupRegion(0, 0, 0, 5);
      PutNKeysNoExpire(1, 2);
      DoNPuts(1);
      CheckRegion(true, 10);

      m_regionName = "RR4";
      SetupRegion(0, 0, 0, 8);
      PutKeyTouch(5);
      Thread.Sleep(5000);
      Assert.AreEqual(1, GetNumOfEntries(), "Expected 1 entry");
      CheckRegion(false, 0);

      m_regionName = "RR5";
      SetupRegion(0, 0, 10, 0);
      Do1Put();
      CheckRegion(true, 12);

      m_regionName = "RR6";
      SetupRegion(0, 0, 6, 3);
      Thread.Sleep(4000);
      Do1Put();
      CheckRegion(false, 4);
      CheckRegion(true, 4);
    }

    [Test]
    public void RegionAndKeyExpiration()
    {
      /*
      m_regionName = "RK1";
      SetupRegion(5, 0, 0, 0);
      PutNKeysNoExpire(1, 2);
      m_region.DestroyRegion();
      CheckRegion(true, 0);

      m_regionName = "RK2";
      SetupRegion(0, 5, 0, 0);
      PutNKeysNoExpire(1, 1);
      m_region.DestroyRegion();
      CheckRegion(true, 6);
       * */

      m_regionName = "RK3";
      SetupRegion(6, 0, 0, 12);
      PutNKeysExpire(1, 10);
      CheckRegion(true, 11);

      m_regionName = "RK4";
      SetupRegion(0, 4, 0, 7);
      PutNKeysNoExpire(1, 3);
      IGFSerializable val = m_region.Get(m_key);
      CheckRegion(false, 5);
      CheckRegion(true, 5);
    }
  }
}

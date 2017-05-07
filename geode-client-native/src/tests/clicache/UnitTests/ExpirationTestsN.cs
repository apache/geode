//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;
using System.Collections.Generic;
#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ExpirationTests : UnitTests
  {
    #region Private members

    private IRegion<object, object> m_region = null;
    private string m_key = "KeyA - 1";
    private string m_value = "Value - AAAAAAA";
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
      Properties<string, string> config = new Properties<string, string>();
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
        ICollection<object> keys = m_region.GetLocalView().Keys;

        if (keys != null)
        {
          return keys.Count;
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
        m_region[string.Format("KeyA - {0}", index + 1)] = m_value;
      }
    }

    private void Do1Put()
    {
      m_region[m_key] = m_value;
    }

    private void SetupRegion(uint entryTTL, uint entryIdleTimeout,
      uint regionTTL, uint regionIdleTimeout)
    {
      const ExpirationAction action = ExpirationAction.Destroy;

      RegionFactory rf = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.LOCAL);

      rf.SetEntryTimeToLive(action, entryTTL);
      rf.SetEntryIdleTimeout(action, entryIdleTimeout);
      rf.SetRegionTimeToLive(action, regionTTL);
      rf.SetRegionIdleTimeout(action, regionIdleTimeout);

      CacheHelper.Init();
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(m_regionName);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", m_regionName);
      }
      m_region = rf.Create<object, object>(m_regionName);
      Assert.IsNotNull(m_region, "IRegion<object, object> was not created.");

    }
    private void PutKeyTouch(int sleepSecs)
    {
      Do1Put();
      Thread.Sleep(sleepSecs * 1000);
      object val = m_region[m_key];
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
      m_region.GetLocalView().Remove(m_key);
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
      object val = m_region[m_key];
      CheckRegion(false, 5);
      CheckRegion(true, 5);
    }
  }
}

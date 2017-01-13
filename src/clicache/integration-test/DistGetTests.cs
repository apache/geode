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
  public class DistGetTests : UnitTests
  {
    private const string RootRegion = "DistGet";
    private const string SomeDistRegion = "SomeDistReg";
    private const string GetRegion = "GetWithInvalid";
    private const string GetILRegion = "GetWithInvalid_IL";

    private const int InvBeginKey = 2006;
    private const int InvEndKey = 2260;
    private const int Inv2BeginKey = 4006;
    private const int Inv2EndKey = 4260;

    private Region m_region;
    private UnitProcess m_dataHolder, m_getter, m_invalidOne, m_invalidTwo;

    protected override ClientBase[] GetClients()
    {
      m_dataHolder = new UnitProcess();
      m_getter = new UnitProcess();
      m_invalidOne = new UnitProcess();
      m_invalidTwo = new UnitProcess();
      return new ClientBase[] { m_dataHolder, m_getter, m_invalidOne, m_invalidTwo };
    }

    #region Functions used by the tests

    public void Puts()
    {
      m_region = CacheHelper.CreateDistRegion(RootRegion,
        ScopeType.DistributedAck, SomeDistRegion, 10);
      Util.Log("Beginning puts.");
      m_region.Put("findme", "hello");

      CacheableString cRes = m_region.Get("findme") as CacheableString;
      Assert.AreEqual("hello", cRes.Value);
    }

    public void FindItems()
    {
      m_region = CacheHelper.CreateDistRegion(RootRegion,
        ScopeType.DistributedAck, SomeDistRegion, 10);

      Util.Log("Created second process region.");

      CacheableString cKey = new CacheableString("findme");
      CacheableString cRes = m_region.Get(cKey) as CacheableString;
      Assert.AreEqual("hello", cRes.Value);
      Util.Log("Received value for findme: {0}", cRes.Value);

      m_region.LocalInvalidateRegion();
      Util.Log("invalidated region");
      Assert.IsTrue(m_region.ContainsKey(cKey));
      Assert.IsFalse(m_region.ContainsValueForKey(cKey));
      Util.Log("passed invalid assertions.");

      cRes = m_region.Get(cKey) as CacheableString;
      Util.Log("get completed.");
      Assert.AreEqual("hello", cRes.Value);
      Util.Log("Received value for findme: {0}", cRes.Value);
    }

    public void MakeDataTwo(string regionName)
    {
      m_region = CacheHelper.CreateILRegion(regionName, true, true, null);
      CacheableInt32 cKey;
      for (int i = InvBeginKey; i <= InvEndKey; i++)
      {
        cKey = new CacheableInt32(i);
        m_region.Put(cKey, cKey);
      }
    }

    public void Join(string regionName)
    {
      m_region = CacheHelper.CreateILRegion(regionName, true, true, null);
      CacheableInt32 cVal;
      for (int i = InvBeginKey; i <= InvEndKey; i++)
      {
        cVal = m_region.Get(i) as CacheableInt32;
        Assert.IsNotNull(cVal);
        Assert.AreEqual(i, cVal.Value);
      }
      m_region.LocalInvalidateRegion();
    }

    public void CheckNotValid(string regionName)
    {
      m_region = CacheHelper.CreateILRegion(regionName, true, true, null);
      CacheableInt32 cVal;
      for (int i = InvBeginKey; i <= InvEndKey; i++)
      {
        cVal = m_region.Get(i) as CacheableInt32;
        Assert.IsNotNull(cVal);
        Assert.AreEqual(i, cVal.Value);
      }
      for (int i = InvBeginKey; i <= InvEndKey; i++)
      {
        m_region.Put(i, -i);
      }
      for (int i = InvBeginKey; i <= InvEndKey; i++)
      {
        cVal = m_region.Get(i) as CacheableInt32;
        Assert.IsNotNull(cVal);
        Assert.AreEqual(-i, cVal.Value);
      }
    }

    public void PutKeys(int start, int end, int factor, bool invalidate)
    {
      for (int i = start; i <= end; i++)
      {
        m_region.Put(i, i * factor);
        if (invalidate)
        {
          m_region.LocalInvalidate(i);
        }
      }
    }

    public void CheckKeys(int start, int end, int factor, bool invalidate, bool netSearch)
    {
      CacheableInt32 cKey, cVal;
      for (int i = start; i <= end; i++)
      {
        cKey = new CacheableInt32(i);
        if (netSearch)
        {
          Assert.IsFalse(m_region.ContainsKey(cKey));
        }
        else
        {
          Assert.IsTrue(m_region.ContainsValueForKey(cKey));
        }
        cVal = m_region.Get(cKey) as CacheableInt32;
        Assert.IsNotNull(cVal);
        Assert.AreEqual(i * factor, cVal.Value);
        if (invalidate)
        {
          m_region.LocalInvalidate(cKey);
        }
      }
    }

    #endregion

    [Test]
    public void DistReg()
    {
      m_dataHolder.Call(Puts);
      m_getter.Call(FindItems);
    }

    [Test]
    public void GetWithInvalid()
    {
      m_dataHolder.Call(MakeDataTwo, GetRegion);
      m_invalidOne.Call(Join, GetRegion);
      m_invalidTwo.Call(Join, GetRegion);
      m_getter.Call(CheckNotValid, GetRegion);

      m_invalidTwo.Call(CheckKeys, InvBeginKey, InvEndKey, -1, false, false);
      m_invalidOne.Call(CheckKeys, InvBeginKey, InvEndKey, -1, false, false);
    }
  }
}

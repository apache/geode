/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading;

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.DUnitFramework;

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

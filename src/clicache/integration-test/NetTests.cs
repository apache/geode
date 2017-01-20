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
  public class NetTests : UnitTests
  {
    private const string TestRegion = "TestRegion";
    private const string TestRegionWrite = "TestRegionWrite";
    private const int NumEntries = 200;

    private Region m_region;
    private Region m_netWriteRegion;

    private TallyLoader m_ldr = new TallyLoader();
    private TallyWriter m_lwr = new TallyWriter();

    private UnitProcess m_client1, m_client2, m_client3;

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
    }

    #region Functions invoked by the tests

    public void DoGets(Region region, int num)
    {
      for(int i = 0; i < num; i++)
      {
        CacheableInt32 val = region.Get(i) as CacheableInt32;
        Assert.AreEqual(i, val.Value);
      }
    }

    public void CreateRegionWithTallyLoader(ScopeType scope)
    {
      AttributesFactory af = new AttributesFactory();
      af.SetCacheLoader(m_ldr);
      af.SetScope(scope);
      af.SetCachingEnabled(true);
    
      m_region = CacheHelper.CreateRegion(TestRegion,
        af.CreateRegionAttributes());
    }

    public void CreateRegionAndGetNEntries(int num)
    {
      CacheHelper.CreateDistribRegion(TestRegion, false, true);
      m_region = CacheHelper.GetVerifyRegion(TestRegion);
      DoGets(m_region, num);
      IGFSerializable[] arr = m_region.GetKeys();
      Assert.AreEqual(num, arr.Length);
    }

    public void VerifyLoaderCallsAfterGets(int num)
    {
      Assert.AreEqual(num, m_ldr.Loads);
      Util.Log("Calling doGets for verify");
      //Thread.Sleep(2000);
      //doGets(m_region, load);
      IGFSerializable[] arr = m_region.GetKeys();
      Assert.AreEqual(num, arr.Length);
    }

    public void RegionThreeLoadEntries(int num)
    {
      AttributesFactory af = new AttributesFactory();
      af.SetScope(ScopeType.Local);
      af.SetCacheLoader(m_ldr);
      m_region = CacheHelper.CreateRegion(TestRegion, af.CreateRegionAttributes());
      m_ldr.Reset();
      Thread.Sleep(100);
      DoGets(m_region, num);
      Assert.AreEqual(num, m_ldr.Loads);
      IGFSerializable[] arr = m_region.GetKeys();
      Assert.AreEqual(num, arr.Length);
    }

    public void CreateRegionWithTallyWriter(ScopeType scope)
    {
      AttributesFactory af = new AttributesFactory();
      af.SetCacheWriter(m_lwr);
      af.SetScope(scope);
      af.SetCachingEnabled(true);
  
      m_netWriteRegion = CacheHelper.CreateRegion(TestRegionWrite,
        af.CreateRegionAttributes());
    }

    public void RegionTwoCreateEntries(int num)
    {
      CacheHelper.CreateDistribRegion(TestRegionWrite, false, true);
      m_netWriteRegion = CacheHelper.GetVerifyRegion(TestRegionWrite);
      Thread.Sleep(100);
      TestCreateEntryActions(num);
    }

    public void TestCreateEntryActions(int num)
    {
      for (int i = 0; i < num; i++)
      {
        CacheableInt32 key = new CacheableInt32(i);
        m_netWriteRegion.Put(key, key);
      }
    }

    public void TestUpdateActions(int num)
    {
      for (int i = 0; i < num; i++)
      {
        CacheableInt32 key = new CacheableInt32(i);
        m_netWriteRegion.Put(key, key);
      }
    }

    public void VerifyWriterCallsAfterCreate(int num)
    {
      Assert.AreEqual(num, m_lwr.Creates);
    }

    public void VerifyWriterCallsAfterUpdates(int num)
    {
      Assert.AreEqual(num, m_lwr.Creates);
      Assert.AreEqual(num, m_lwr.Updates);
    }

    #endregion

    [Test]
    public void LoaderTest()
    {
      m_client1.Call(CreateRegionWithTallyLoader, ScopeType.DistributedNoAck);
      m_client2.Call(CreateRegionAndGetNEntries, NumEntries);
      m_client1.Call(VerifyLoaderCallsAfterGets, NumEntries);
      m_client3.Call(RegionThreeLoadEntries, NumEntries);
    }

    [Test]
    public void WriterTest()
    {
      m_client1.Call(CreateRegionWithTallyWriter, ScopeType.DistributedNoAck);
      m_client2.Call(RegionTwoCreateEntries, NumEntries);
      m_client1.Call(VerifyWriterCallsAfterCreate, NumEntries);
      m_client2.Call(TestUpdateActions, NumEntries);
      m_client1.Call(VerifyWriterCallsAfterUpdates, NumEntries);
    }
  }
}

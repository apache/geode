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

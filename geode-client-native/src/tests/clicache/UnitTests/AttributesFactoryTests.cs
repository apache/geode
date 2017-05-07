//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Reflection;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class AttributesFactoryTests : UnitTests
  {
    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    [Test]
    public void InvalidTCRegionAttributes()
    {
      Properties config = new Properties();
      CacheHelper.InitConfig(config);
      Region region;
      RegionAttributes attrs;
      AttributesFactory af = new AttributesFactory();
      af.SetScope(ScopeType.Local);
      af.SetEndpoints("bass:1234");
      attrs = af.CreateRegionAttributes();
      try
      {
        region = CacheHelper.CreateRegion("region1", attrs);
        Assert.Fail(
          "LOCAL scope is incompatible with a native client region");
      }
      catch (UnsupportedOperationException)
      {
        Util.Log("Got expected UnsupportedOperationException for " +
          "LOCAL scope for native client region");
      }

      af.SetScope(ScopeType.Local);
      af.SetClientNotificationEnabled(true);
      attrs = af.CreateRegionAttributes();
      try
      {
        region = CacheHelper.CreateRegion("region2", attrs);
        Assert.Fail(
          "LOCAL scope is incompatible with clientNotificationEnabled");
      }
      catch (UnsupportedOperationException)
      {
        Util.Log("Got expected UnsupportedOperationException for " +
          "clientNotificationEnabled for non native client region");
      }

      // Checks for HA regions

      CacheHelper.CloseCache();
      af.SetScope(ScopeType.Local);
      af.SetEndpoints("bass:3434");
      af.SetClientNotificationEnabled(false);
      attrs = af.CreateRegionAttributes();
      try
      {
        region = CacheHelper.CreateRegion("region2", attrs);
        Assert.Fail(
          "LOCAL scope is incompatible with a native client HA region");
      }
      catch (UnsupportedOperationException)
      {
        Util.Log("Got expected UnsupportedOperationException for " +
          "LOCAL scope for native client region");
      }

      af.SetScope(ScopeType.Local);
      af.SetEndpoints("none");
      af.SetClientNotificationEnabled(false);
      attrs = af.CreateRegionAttributes();
      region = CacheHelper.CreateRegion("region1", attrs);
      Util.Log("C++ local region created with HA cache specification.");
    }
  }
}

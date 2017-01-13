//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

//using System;
//using System.Reflection;

//#pragma warning disable 618

//namespace GemStone.GemFire.Cache.UnitTests.NewAPI
//{
//  using NUnit.Framework;
//  using GemStone.GemFire.DUnitFramework;
// // using GemStone.GemFire.Cache; 
//  using GemStone.GemFire.Cache.Generic;
//  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

//  [TestFixture]
//  [Category("group1")]
//  [Category("unicast_only")]
//  [Category("generics")]
//  public class AttributesFactoryTests : UnitTests
//  {
//    protected override ClientBase[] GetClients()
//    {
//      return null;
//    }

//    [Test]
//    public void InvalidTCRegionAttributes()
//    {
//      Properties<string, string> config = new Properties<string, string>();
//      CacheHelper.InitConfig(config);
//      IRegion<object, object> region;
//      RegionAttributes<object, object> attrs;
//      AttributesFactory<object, object> af = new AttributesFactory<object, object>();
//      af.SetScope(ScopeType.Local);
//      af.SetEndpoints("bass:1234");
//      attrs = af.CreateRegionAttributes();
//      region = CacheHelper.CreateRegion<object, object>("region1", attrs);
//      try
//      {
//       IRegion<Object, Object> localRegion = region.GetLocalView();
//        Assert.Fail(
//          "LOCAL scope is incompatible with a native client region");
//      }
//      catch (UnsupportedOperationException)
//      {
//        Util.Log("Got expected UnsupportedOperationException for " +
//          "LOCAL scope for native client region");
//      }

//      af.SetScope(ScopeType.Local);
//      af.SetClientNotificationEnabled(true);
//      attrs = af.CreateRegionAttributes();
//      try
//      {
//        region = CacheHelper.CreateRegion<object, object>("region2", attrs);
//        Assert.Fail(
//          "LOCAL scope is incompatible with clientNotificationEnabled");
//      }
//      catch (UnsupportedOperationException)
//      {
//        Util.Log("Got expected UnsupportedOperationException for " +
//          "clientNotificationEnabled for non native client region");
//      }

//      // Checks for HA regions

//      CacheHelper.CloseCache();
//      af.SetScope(ScopeType.Local);
//      af.SetEndpoints("bass:3434");
//      af.SetClientNotificationEnabled(false);
//      attrs = af.CreateRegionAttributes();
//      try
//      {
//        region = CacheHelper.CreateRegion<object, object>("region2", attrs);
//        Assert.Fail(
//          "LOCAL scope is incompatible with a native client HA region");
//      }
//      catch (UnsupportedOperationException)
//      {
//        Util.Log("Got expected UnsupportedOperationException for " +
//          "LOCAL scope for native client region");
//      }

//      af.SetScope(ScopeType.Local);
//      af.SetEndpoints("none");
//      af.SetClientNotificationEnabled(false);
//      attrs = af.CreateRegionAttributes();
//      region = CacheHelper.CreateRegion<object, object>("region1", attrs);
//      Util.Log("C++ local region created with HA cache specification.");
//    }
//  }
//}

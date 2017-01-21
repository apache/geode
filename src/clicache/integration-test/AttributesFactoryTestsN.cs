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

//using System;
//using System.Reflection;

//#pragma warning disable 618

//namespace Apache.Geode.Client.UnitTests
//{
//  using NUnit.Framework;
//  using Apache.Geode.DUnitFramework;
// // using Apache.Geode.Client; 
//  using Apache.Geode.Client;
//  //using Region = Apache.Geode.Client.IRegion<Object, Object>;

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

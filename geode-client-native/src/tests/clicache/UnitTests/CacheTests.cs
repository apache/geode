//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.IO;
using System.Threading;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

    [TestFixture]
    [Category("group4")]
    [Category("unicast_only")]
    [Category("deprecated")]
    public class CacheTests : UnitTests
    {
        private DistributedSystem m_dsys = null;
        private const string DSYSName = "CacheTest";

        protected override ClientBase[] GetClients()
        {
            return null;
        }

        [TestFixtureSetUp]
        public override void InitTests()
        {
            base.InitTests();
            Util.Log("Creating DistributedSytem with name = {0}", DSYSName);
            CacheHelper.ConnectName(DSYSName);
            m_dsys = CacheHelper.DSYS;
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

        [Test]
        public void CppCache()
        {
            string host_name = "CPPCACHE_TEST";
            const UInt32 totalSubRegions = 3;
            string regionName = "CPPCACHE_ROOT_REGION";
            string subRegionName1 = "CPPCACHE_SUB_REGION1";
            string subRegionName2 = "CPPCACHE_SUB_REGION2";
            string subRegionName21 = "CPPCACHE_SUB_REGION21";
            const char SPTOR = '/';
            Cache cache;
            Region region;

            Util.Log("Creating Cache with name = '" + host_name + "' and unitialized system");
            try
            {
                cache = CacheFactory.Create(host_name, null);
                Assert.Fail("IllegalArgumentException should have occurred when initializing a Cache to null DistributedSystem!");
            }
            catch (IllegalArgumentException ex)
            {
                Util.Log("Got an expected exception in creating uninitialized Cache: " + ex);
            }

            Util.Log("Creating Cache with name = " + host_name);
            cache = CacheFactory.Create(host_name, m_dsys);
            Util.Log("Creating DUPLICATE Cache with name = " + host_name);
            Cache cacheDup;
            try
            {
                cacheDup = CacheFactory.Create(host_name, m_dsys);
                Assert.Fail("CacheExistsException should have occurred when initializing a duplicate Cache.");
            }
            catch (CacheExistsException ex)
            {
                Util.Log("Got an expected exception in creating duplicate Cache: " + ex);
            }

            AttributesFactory attrFac = new AttributesFactory();
            RegionAttributes rAttr;
            attrFac.SetScope(ScopeType.Local);
            Util.Log("Creating RegionAttributes");
            rAttr = attrFac.CreateRegionAttributes();

            Util.Log("Creating Region with name = " + regionName);
            region = cache.CreateRegion(regionName, rAttr);

            Util.Log("Creating Sub Region with name = " + subRegionName1);
            Region subRegion1 = region.CreateSubRegion(subRegionName1, rAttr);

            Util.Log("Create Sub Region with name = " + subRegionName2);
            Region subRegion2 = region.CreateSubRegion(subRegionName2, rAttr);

            Util.Log("Create Sub Region with name = '" + subRegionName21 +
              "' inside region with name = " + subRegionName2);
            Region subRegion21 = subRegion2.CreateSubRegion(subRegionName21, rAttr);

            Region[] subRegions = region.SubRegions(true);
            Util.Log("The number of sub-regions is: " + subRegions.Length);
            Assert.AreEqual(totalSubRegions, subRegions.Length, "Number of sub-regions does not match.");
            Util.Log("The sub-regions are: ");
            foreach (Region rg in subRegions)
            {
                Util.Log("\t" + rg.Name);
            }

            Region[] rootRegions = cache.RootRegions();
            Util.Log("The number of root regions in the cache is: " + rootRegions.Length);
            Util.Log("The root regions are: ");
            foreach (Region rg in rootRegions)
            {
                Util.Log("\t" + rg.Name);
            }
            subRegionName1 = regionName + SPTOR + subRegionName1;
            subRegionName2 = regionName + SPTOR + subRegionName2;
            subRegionName21 = subRegionName2 + SPTOR + subRegionName21;

            Util.Log("Sub-region1: " + subRegionName1);
            Util.Log("Sub-region2: " + subRegionName2);
            Util.Log("Sub-region21: " + subRegionName21);

            #region Try a simple put/get in the region

            region.Put("key1", "value1");
            CacheableString getValue1 = region.Get("key1") as CacheableString;
            Assert.IsNotNull(getValue1, "Value for 'key1' should not be null.");
            // Sleep for sometime and try again.
            Thread.Sleep(1000);
            getValue1 = region.Get("key1") as CacheableString;
            Assert.IsNotNull(getValue1, "Value for 'key1' should not be null after sleep.");

            #endregion

            string[] allRegions = new string[] { regionName, subRegionName1, subRegionName2, subRegionName21 };
            foreach (string rgName in allRegions)
            {
                Util.Log("Finding region: " + rgName);
                region = cache.GetRegion(rgName);
                Assert.IsNotNull(region, "Did not find the region with name = " + rgName);
            }
            string notExist = "/NotExistentRegion";
            Util.Log("Finding region: " + notExist);
            region = cache.GetRegion(notExist);
            Assert.IsNull(region, "Found the non-existent region with name = " + notExist);

            cache.Close();
        }

    }
}

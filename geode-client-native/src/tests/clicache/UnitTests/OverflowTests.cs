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
  public class OverflowTests : UnitTests
  {
    private DistributedSystem m_dsys = null;
    private Region m_region;
    private const string DSYSName = "OverflowTest";
    private const string BDBDir = "BDB";
    private const string BDBDirEnv = "BDBEnv";

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
      CacheHelper.InitName(DSYSName, DSYSName);
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

    [TearDown]
    public override void EndTest()
    {
      base.EndTest();
    }

    #region Private functions used by the tests

    private Region CreateOverflowRegion(string regionName)
    {
      AttributesFactory attrsFact = new AttributesFactory();
      attrsFact.SetScope(ScopeType.Local);
      attrsFact.SetCachingEnabled(true);
      attrsFact.SetLruEntriesLimit(20);
      attrsFact.SetInitialCapacity(1000);
      attrsFact.SetDiskPolicy(DiskPolicyType.Overflows);
      Properties bdbProperties = new Properties();
      bdbProperties.Insert("CacheSizeGb", "0");
      bdbProperties.Insert("CacheSizeMb", "512");
      bdbProperties.Insert("PageSize", "65536");
      bdbProperties.Insert("MaxFileSize", "512000000");
      bdbProperties.Insert("PersistenceDirectory", BDBDir);
      bdbProperties.Insert("EnvironmentDirectory", BDBDirEnv);
      attrsFact.SetPersistenceManager("BDBImpl",
        "createBDBInstance", bdbProperties);

      return CacheHelper.CreateRegion(regionName,
        attrsFact.CreateRegionAttributes());
    }

    // Testing for attibute validation.
    private void ValidateAttributes(Region region)
    {
      RegionAttributes regAttr = region.Attributes;
      int initialCapacity = regAttr.InitialCapacity;
      Assert.AreEqual(1000, initialCapacity, "Expected initial capacity to be 1000");
      Assert.AreEqual(DiskPolicyType.Overflows, regAttr.DiskPolicy,
        "Expected Action to be overflow to disk");
    }

    private string GetBDBFileName(string bdbDir, string regionName, string subRegionName)
    {
      if (subRegionName == null)
      {
        return string.Format("{0}/{1}_{2}/_{3}/file_0.db",
          bdbDir, Util.HostName, Util.PID, regionName);
      }
      else
      {
        return string.Format("{0}/{1}_{2}/_{3}_{4}/file_0.db",
          bdbDir, Util.HostName, Util.PID, regionName, subRegionName);
      }
    }

    private Region CreateSubRegion(Region region, string subRegionName)
    {
      AttributesFactory attrsFact = new AttributesFactory(region.Attributes);
      Properties bdbProperties = new Properties();
      bdbProperties.Insert("CacheSizeGb", "0");
      bdbProperties.Insert("CacheSizeMb", "512");
      bdbProperties.Insert("PageSize", "65536");
      bdbProperties.Insert("MaxFileSize", "512000000");
      bdbProperties.Insert("PersistenceDirectory", BDBDir);
      bdbProperties.Insert("EnvironmentDirectory", BDBDirEnv);
      attrsFact.SetPersistenceManager("BDBImpl",
        "createBDBInstance", bdbProperties);
      attrsFact.SetScope(ScopeType.Local);
      Region subRegion = region.CreateSubRegion(subRegionName,
        attrsFact.CreateRegionAttributes());
      Assert.IsNotNull(subRegion, "Expected region to be non null");
      Assert.IsTrue(File.Exists(GetBDBFileName(BDBDir, region.Name,
        subRegionName)), "Persistence file is not present");
      DoNput(subRegion, 1000);
      DoNget(subRegion, 1000);
      return subRegion;
    }

    private void DoNput(Region region, int num)
    {
      CacheableString cVal = new CacheableString(new string('A', 1024));
      for (int i = 0; i < num; i++)
      {
        Util.Log("Putting key = key-{0}", i);
        region.Put("key-" + i.ToString(), cVal);
      }
    }

    private void DoNget(Region region, int num)
    {
      CacheableString cVal;
      string expectVal = new string('A', 1024);
      for (int i = 0; i < num; i++)
      {
        cVal = region.Get("key-" + i.ToString()) as CacheableString;
        Util.Log("Getting key = key-{0}", i);
        Assert.IsNotNull(cVal, "Key[key-{0}] not found.", i);
        Assert.AreEqual(expectVal, cVal.Value, "Did not find the expected value.");
      }
    }

    private bool IsOverflowed(IGFSerializable cVal)
    {
      //Util.Log("IsOverflowed:: value is: {0}; type is: {1}", cVal.ToString(), cVal.GetType());
      return cVal.ToString() == "CacheableToken::OVERFLOWED";
    }

    private void CheckOverflowToken(Region region, int num, int lruLimit)
    {
      IGFSerializable cVal;
      ICacheableKey[] cKeys = region.GetKeys();

      Assert.AreEqual(num, cKeys.Length, "Number of keys does not match.");
      int count = 0;
      foreach (CacheableKey cKey in cKeys)
      {
        RegionEntry entry = region.GetEntry(cKey);
        cVal = entry.Value;
        if (IsOverflowed(cVal))
        {
          count++;
        }
      }
      Assert.AreEqual(0, count, "Number of overflowed entries should be zero");
    }

    private void CheckNumOfEntries(Region region, int lruLimit)
    {
      IGFSerializable[] cVals = region.GetValues();
      Assert.AreEqual(lruLimit, cVals.Length, "Number of values does not match.");
    }

    private void TestGetOp(Region region, int num)
    {
      DoNput(region, num);

      ICacheableKey[] cKeys = region.GetKeys();
      IGFSerializable cVal;

      Assert.AreEqual(num, cKeys.Length, "Number of keys does not match.");
      foreach (ICacheableKey cKey in cKeys)
      {
        RegionEntry entry = region.GetEntry(cKey);
        cVal = entry.Value;
        if (IsOverflowed(cVal))
        {
          cVal = region.Get(cKey);
          Assert.IsFalse(IsOverflowed(cVal), "Overflow token even after a Region.Get");
        }
      }
    }

    private void TestEntryDestroy(Region region)
    {
      ICacheableKey[] cKeys = region.GetKeys();
      for (int i = 50; i < 60; i++)
      {
        try
        {
          region.LocalDestroy(cKeys[i]);
        }
        catch (Exception ex)
        {
          Util.Log("Entry missing for {0}. Exception: {1}", cKeys[i], ex.ToString());
        }
      }
      cKeys = region.GetKeys();
      Assert.AreEqual(90, cKeys.Length, "Number of keys is not correct.");
    }

    #endregion

    [Test]
    public void OverflowPutGet()
    {
      m_region = CreateOverflowRegion("OverFlowRegion");
      ValidateAttributes(m_region);

      // put some values into the cache.
      DoNput(m_region, 100);
      CheckNumOfEntries(m_region, 100);

      // check whether value get evicted and token gets set as overflow
      CheckOverflowToken(m_region, 100, 20);
      // do some gets... printing what we find in the cache.
      DoNget(m_region, 100);
      TestEntryDestroy(m_region);
      TestGetOp(m_region, 100);

      // test to verify same region repeatedly to ensure that the persistece 
      // files are created and destroyed correctly

      Region subRegion;
      for (int i = 0; i < 10; i++)
      {
        subRegion = CreateSubRegion(m_region, "SubRegion");
        subRegion.DestroyRegion();
        Assert.IsTrue(subRegion.IsDestroyed, "Expected region to be destroyed");
        Assert.IsFalse(File.Exists(GetBDBFileName(BDBDir, m_region.Name,
        "SubRegion")), "Persistence file present after region destroy");
      }
    }

    [Test]
    public void XmlCacheCreationWithOverflow()
    {
      Cache cache = null;
      Region region1;
      Region region2;
      Region[] rootRegions;
      Region[] subRegions;
      /*string host_name = "XML_CACHE_CREATION_TEST";*/
      const UInt32 totalSubRegionsRoot1 = 2;
      const UInt32 totalRootRegions = 2;

      try
      {
        CacheHelper.CloseCache();
        Util.Log("Creating cache with the configurations provided in valid_overflowAttr.xml");
        string cachePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "valid_overflowAttr.xml";
        cache = CacheFactory.CreateCacheFactory().Set("cache-xml-file", cachePath).Create();
        Util.Log("Successfully created the cache.");
        rootRegions = cache.RootRegions();
        Assert.IsNotNull(rootRegions);
        Assert.AreEqual(totalRootRegions, rootRegions.Length);

        Util.Log("Root regions in Cache: ");
        foreach (Region rg in rootRegions)
        {
          Util.Log('\t' + rg.Name);
        }

        region1 = rootRegions[0];
        subRegions = region1.SubRegions(true);
        Assert.IsNotNull(subRegions);
        Assert.AreEqual(subRegions.Length, totalSubRegionsRoot1);

        Util.Log("SubRegions for the root region: ");
        foreach (Region rg in subRegions)
        {
          Util.Log('\t' + rg.Name);
        }

        Util.Log("Testing if the nesting of regions is correct...");
        region2 = rootRegions[1];
        subRegions = region2.SubRegions(true);
        string childName;
        string parentName;
        foreach (Region rg in subRegions)
        {
          childName = rg.Name;
          Region parent = rg.ParentRegion;
          parentName = parent.Name;
          if (childName == "SubSubRegion221")
          {
            Assert.AreEqual("SubRegion22", parentName);
          }
        }

        RegionAttributes attrs = region1.Attributes;
        //Util.Log("Attributes of root region Root1 are: ");

        ScopeType scopeST = attrs.Scope;
        Assert.AreEqual(ScopeType.Local, scopeST);

        bool cachingEnabled = attrs.CachingEnabled;
        Assert.IsTrue(cachingEnabled);

        uint lruEL = attrs.LruEntriesLimit;
        Assert.AreEqual(35, lruEL);

        int concurrency = attrs.ConcurrencyLevel;
        Assert.AreEqual(10, concurrency);

        int initialCapacity = attrs.InitialCapacity;
        Assert.AreEqual(25, initialCapacity);

        int regionIdleTO = attrs.RegionIdleTimeout;
        Assert.AreEqual(20, regionIdleTO);

        ExpirationAction action1 = attrs.RegionIdleTimeoutAction;
        Assert.AreEqual(ExpirationAction.Destroy, action1);

        DiskPolicyType type = attrs.DiskPolicy;
        Assert.AreEqual(DiskPolicyType.Overflows, type);
        string persistenceDir, maxFileSize;

        string lib = attrs.PersistenceLibrary;
        string libFun = attrs.PersistenceFactory;
        Util.Log(" persistence library1 = " + lib);
        Util.Log(" persistence function1 = " + libFun);
        Properties pconfig = attrs.PersistenceProperties;
        Assert.IsNotNull(pconfig, "Persistence properties should not be null for root1.");
        persistenceDir = pconfig.Find("PersistenceDirectory");
        maxFileSize = pconfig.Find("MaxFileSize");
        Assert.IsNotNull(persistenceDir, "Persistence directory should not be null.");
        Assert.AreNotEqual(0, persistenceDir.Length, "Persistence directory should not be empty.");
        Assert.IsNotNull(maxFileSize, "Persistence MaxFileSize should not be null.");
        Assert.AreNotEqual(0, maxFileSize.Length, "Persistence MaxFileSize should not be empty.");

        Util.Log("****Attributes of Root1 are correctly set****");

        RegionAttributes attrs2 = region2.Attributes;
        string lib2 = attrs2.PersistenceLibrary;
        string libFun2 = attrs2.PersistenceFactory;
        Util.Log(" persistence library2 = " + lib2);
        Util.Log(" persistence function2 = " + libFun2);
        Properties pconfig2 = attrs2.PersistenceProperties;
        Assert.IsNotNull(pconfig2, "Persistence properties should not be null for root2.");
        persistenceDir = pconfig2.Find("PersistenceDirectory");
        maxFileSize = pconfig2.Find("MaxFileSize");
        Assert.IsNotNull(persistenceDir, "Persistence directory should not be null.");
        Assert.AreNotEqual(0, persistenceDir.Length, "Persistence directory should not be empty.");
        Assert.IsNotNull(maxFileSize, "Persistence MaxFileSize should not be null.");
        Assert.AreNotEqual(0, maxFileSize.Length, "Persistence MaxFileSize should not be empty.");

        Util.Log("****Attributes of Root2 are correctly set****");

        region1.DestroyRegion(null);
        region2.DestroyRegion(null);

        if (!cache.IsClosed)
        {
          cache.Close();
        }

        ////////////////////////////testing of cache.xml completed///////////////////


        Util.Log("Create cache with the configurations provided in the invalid_overflowAttr1.xml.");

        Util.Log("Non existent XML; exception should be thrown");

        try
        {
          cachePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "non-existent.xml";
          cache = CacheFactory.CreateCacheFactory().Set("cache-xml-file", cachePath).Create();
          Assert.Fail("Creation of cache with non-existent.xml should fail!");
        }
        catch (CacheXmlException ex)
        {
          Util.Log("Expected exception with non-existent.xml: {0}", ex);
        }

        Util.Log("This is a well-formed xml....attributes not provided for persistence manager. exception should be thrown");

        try
        {
          cachePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_overflowAttr1.xml";
          cache = CacheFactory.CreateCacheFactory().Set("cache-xml-file", cachePath).Create();
          Assert.Fail("Creation of cache with invalid_overflowAttr1.xml should fail!");
        }
        catch (IllegalStateException ex)
        {
          Util.Log("Expected exception with invalid_overflowAttr1.xml: {0}", ex);
        }

        ///////////////testing of invalid_overflowAttr1.xml completed///////////////////

        Util.Log("Create cache with the configurations provided in the invalid_overflowAttr2.xml.");
        Util.Log("This is a well-formed xml....attribute values is not provided for persistence library name......should throw an exception");

        try
        {
          cachePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_overflowAttr2.xml";
          cache = CacheFactory.CreateCacheFactory().Set("cache-xml-file", cachePath).Create();
          Assert.Fail("Creation of cache with invalid_overflowAttr2.xml should fail!");
        }
        catch (CacheXmlException ex)
        {
          Util.Log("Expected exception with invalid_overflowAttr2.xml: {0}", ex);
        }

        ///////////////testing of invalid_overflowAttr2.xml completed///////////////////

        Util.Log("Create cache with the configurations provided in the invalid_overflowAttr3.xml.");
        Util.Log("This is a well-formed xml....but region-attributes for persistence invalid......should throw an exception");

        try
        {
          cachePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_overflowAttr3.xml";
          cache = CacheFactory.CreateCacheFactory().Set("cache-xml-file", cachePath).Create();
          Assert.Fail("Creation of cache with invalid_overflowAttr3.xml should fail!");
        }
        catch (CacheXmlException ex)
        {
          Util.Log("Expected exception with invalid_overflowAttr3.xml: {0}", ex);
        }

        ///////////////testing of invalid_overflowAttr3.xml completed///////////////////
      }
      catch (Exception ex)
      {
        Assert.Fail("Caught exception: {0}", ex);
      }
      finally
      {
        if (cache != null && !cache.IsClosed)
        {
          cache.Close();
        }
      }
    }
  }
}

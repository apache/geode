//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using System.Diagnostics;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("generics")]

  public class OverflowTests : UnitTests
  {
    private DistributedSystem m_dsys = null;

    private const string DSYSName = "OverflowTest";

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
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

    [SetUp]
    public void StartTest()
    {
      CacheHelper.Init();
    }

    [TearDown]
    public override void EndTest()
    {
      CacheHelper.Close();
      base.EndTest();
    }

    #region Private functions used by the tests

    private IRegion<object, object> CreateOverflowRegion(string regionName, string libraryName, string factoryFunctionName)
    {

      RegionFactory rf = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.LOCAL);
      rf.SetCachingEnabled(true);
      rf.SetLruEntriesLimit(20);
      rf.SetInitialCapacity(1000);
      rf.SetDiskPolicy(DiskPolicyType.Overflows);

      Properties<string, string> sqliteProperties = new Properties<string, string>();
      sqliteProperties.Insert("PageSize", "65536");
      sqliteProperties.Insert("MaxFileSize", "512000000");
      String sqlite_dir = "SqLiteDir" + Process.GetCurrentProcess().Id.ToString();
      sqliteProperties.Insert("PersistenceDirectory", sqlite_dir);

      rf.SetPersistenceManager(libraryName, factoryFunctionName, sqliteProperties);

      CacheHelper.Init();
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(regionName);
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> {0} was not destroyed.", regionName);
      }
      region = rf.Create<object, object>(regionName);
      Assert.IsNotNull(region, "IRegion<object, object> was not created.");

      return region;

    }

    // Testing for attibute validation.
    private void ValidateAttributes(IRegion<object, object> region)
    {
      GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> regAttr = region.Attributes;
      int initialCapacity = regAttr.InitialCapacity;
      Assert.AreEqual(1000, initialCapacity, "Expected initial capacity to be 1000");
      Assert.AreEqual(DiskPolicyType.Overflows, regAttr.DiskPolicy,
        "Expected Action to be overflow to disk");
    }


    private string GetSqLiteFileName(string sqliteDir, string regionName)
    {
      return Path.Combine(Directory.GetCurrentDirectory(), sqliteDir, regionName, regionName + ".db");
    }

    private IRegion<object, object> CreateSubRegion(IRegion<object, object> region, string subRegionName, string libraryName, string factoryFunctionName)
    {
      AttributesFactory<object, object> attrsFact = new AttributesFactory<object, object>(region.Attributes);
      Properties<string, string> sqliteProperties = new Properties<string, string>();
      sqliteProperties.Insert("PageSize", "65536");
      sqliteProperties.Insert("MaxPageCount", "512000000");
      String sqlite_dir = "SqLiteDir" + Process.GetCurrentProcess().Id.ToString();
      sqliteProperties.Insert("PersistenceDirectory", sqlite_dir);
      attrsFact.SetPersistenceManager(libraryName, factoryFunctionName, sqliteProperties);
      IRegion<object, object> subRegion = region.CreateSubRegion(subRegionName,
        attrsFact.CreateRegionAttributes());
      Assert.IsNotNull(subRegion, "Expected region to be non null");
      Assert.IsTrue(File.Exists(GetSqLiteFileName(sqlite_dir, subRegionName)), "Persistence file is not present");
      DoNput(subRegion, 50);
      DoNget(subRegion, 50);
      return subRegion;
    }

    private void DoNput(IRegion<object, object> region, int num)
    {
      //CacheableString cVal = new CacheableString(new string('A', 1024));
      string cVal = new string('A', 1024);
      for (int i = 0; i < num; i++)
      {
        Util.Log("Putting key = key-{0}", i);
        region["key-" + i.ToString()] = cVal;
      }
    }

    private void DoNget(IRegion<object, object> region, int num)
    {
      string cVal;
      string expectVal = new string('A', 1024);
      for (int i = 0; i < num; i++)
      {
        cVal = region["key-" + i.ToString()] as string;
        Util.Log("Getting key = key-{0}", i);
        Assert.IsNotNull(cVal, "Key[key-{0}] not found.", i);
        Assert.AreEqual(expectVal, cVal, "Did not find the expected value.");
      }
    }

    private bool IsOverflowed(/*IGFSerializable*/ object cVal)
    {
      //Util.Log("IsOverflowed:: value is: {0}; type is: {1}", cVal.ToString(), cVal.GetType());
      return cVal.ToString() == "CacheableToken::OVERFLOWED";
    }

    private void CheckOverflowToken(IRegion<object, object> region, int num, int lruLimit)
    {
      //IGFSerializable cVal;
      //ICacheableKey[] cKeys = region.GetKeys();

      object cVal;
      ICollection<object> cKeys = region.GetLocalView().Keys;
      Assert.AreEqual(num, cKeys.Count, "Number of keys does not match.");
      int count = 0;
      foreach (object cKey in cKeys)
      {
        RegionEntry<object, object> entry = region.GetEntry(cKey);
        cVal = entry.Value;
        if (IsOverflowed(cVal))
        {
          count++;
        }
      }
      Assert.AreEqual(0, count, "Number of overflowed entries should be zero");
    }

    private void CheckNumOfEntries(IRegion<object, object> region, int lruLimit)
    {
      //ICollection<object> cVals = region.GetLocalView().Values;
      ICollection<object> cVals = region.Values;
      Assert.AreEqual(lruLimit, cVals.Count, "Number of values does not match.");
    }

    private void TestGetOp(IRegion<object, object> region, int num)
    {
      DoNput(region, num);

      ICollection<object> cKeys = region.GetLocalView().Keys;
      object cVal;

      Assert.AreEqual(num, cKeys.Count, "Number of keys does not match.");
      foreach (object cKey in cKeys)
      {
        RegionEntry<object, object> entry = region.GetEntry(cKey);
        cVal = entry.Value;
        if (IsOverflowed(cVal))
        {
          cVal = region[cKey];
          Assert.IsFalse(IsOverflowed(cVal), "Overflow token even after a Region.Get");
        }
      }
    }

    private void TestEntryDestroy(IRegion<object, object> region)
    {
      //ICollection<object> cKeys = region.Keys;
      ICollection<object> cKeys = region.GetLocalView().Keys;
      string[] arrKeys = new string[cKeys.Count];
      cKeys.CopyTo(arrKeys, 0);

      for (int i = 50; i < 60; i++)
      {
        try
        {
          region.GetLocalView().Remove(arrKeys[i]);
        }
        catch (Exception ex)
        {
          Util.Log("Entry missing for {0}. Exception: {1}", arrKeys[i], ex.ToString());
        }
      }
      cKeys = region.GetLocalView().Keys;

      Assert.AreEqual(90, cKeys.Count, "Number of keys is not correct.");
    }

    #endregion

    [Test]
    public void OverflowPutGet()
    {
      IRegion<object, object> region = CreateOverflowRegion("OverFlowRegion", "SqLiteImpl", "createSqLiteInstance");
      ValidateAttributes(region);

      //Console.WriteLine("TEST-2");
      // put some values into the cache.
      DoNput(region, 100);
      //Console.WriteLine("TEST-2.1 All PUts Donee");

      CheckNumOfEntries(region, 100);

      //Console.WriteLine("TEST-3");
      // check whether value get evicted and token gets set as overflow
      CheckOverflowToken(region, 100, 20);
      // do some gets... printing what we find in the cache.
      DoNget(region, 100);
      TestEntryDestroy(region);
      TestGetOp(region, 100);

      //Console.WriteLine("TEST-4");
      // test to verify same region repeatedly to ensure that the persistece 
      // files are created and destroyed correctly

      IRegion<object, object> subRegion;
      String sqlite_dir = "SqLiteDir" + Process.GetCurrentProcess().Id.ToString();
      for (int i = 0; i < 1; i++)
      {
        subRegion = CreateSubRegion(region, "SubRegion", "SqLiteImpl", "createSqLiteInstance");
        subRegion.DestroyRegion();
        Assert.IsTrue(subRegion.IsDestroyed, "Expected region to be destroyed");
        Assert.IsFalse(File.Exists(GetSqLiteFileName(sqlite_dir, "SubRegion")), "Persistence file present after region destroy");
      }
      //Console.WriteLine("TEST-5");
    }

    [Test]
    public void OverflowPutGetManaged()
    {
      IRegion<object, object> region = CreateOverflowRegion("OverFlowRegion", "Gemstone.Gemfire.Plugins.SqLite",
        "Gemstone.Gemfire.Plugins.SqLite.SqLiteImpl<System.Object,System.Object>.Create");
      ValidateAttributes(region);

      //Console.WriteLine("TEST-2");
      // put some values into the cache.
      DoNput(region, 100);
      //Console.WriteLine("TEST-2.1 All PUts Donee");

      CheckNumOfEntries(region, 100);

      //Console.WriteLine("TEST-3");
      // check whether value get evicted and token gets set as overflow
      CheckOverflowToken(region, 100, 20);
      // do some gets... printing what we find in the cache.
      DoNget(region, 100);
      TestEntryDestroy(region);
      TestGetOp(region, 100);

      //Console.WriteLine("TEST-4");
      // test to verify same region repeatedly to ensure that the persistece 
      // files are created and destroyed correctly

      IRegion<object, object> subRegion;
      String sqlite_dir = "SqLiteDir" + Process.GetCurrentProcess().Id.ToString();
      for (int i = 0; i < 10; i++)
      {
        subRegion = CreateSubRegion(region, "SubRegion", "Gemstone.Gemfire.Plugins.SqLite",
          "Gemstone.Gemfire.Plugins.SqLite.SqLiteImpl<System.Object,System.Object>.Create");
        subRegion.DestroyRegion();
        Assert.IsTrue(subRegion.IsDestroyed, "Expected region to be destroyed");
        Assert.IsFalse(File.Exists(GetSqLiteFileName(sqlite_dir, "SubRegion")), "Persistence file present after region destroy");
      }
      //Console.WriteLine("TEST-5");
    }

    [Test]
    public void OverflowPutGetManagedMT()
    {
      IRegion<object, object> region = CreateOverflowRegion("OverFlowRegion", "Gemstone.Gemfire.Plugins.SqLite",
        "Gemstone.Gemfire.Plugins.SqLite.SqLiteImpl<System.Object,System.Object>.Create");
      ValidateAttributes(region);

      List<Thread> threadsList = new List<Thread>();
      for (int i = 0; i < 10; i++)
      {
        Thread t = new Thread(delegate()
        {
          // put some values into the cache.
          DoNput(region, 100);
          CheckNumOfEntries(region, 100);

          // check whether value get evicted and token gets set as overflow
          CheckOverflowToken(region, 100, 20);
          // do some gets... printing what we find in the cache.
          DoNget(region, 100);
          TestEntryDestroy(region);
          TestGetOp(region, 100);
        });
        threadsList.Add(t);
        t.Start();
      }

      for (int i = 0; i < 10; i++)
      {
        threadsList[i].Join();
      }
      region.DestroyRegion();
      //Console.WriteLine("TEST-5");
    }

    [Test]
    public void OverflowPutGetManagedSetInstance()
    {
      RegionFactory rf = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.LOCAL);
      rf.SetCachingEnabled(true);
      rf.SetLruEntriesLimit(20);
      rf.SetInitialCapacity(1000);
      rf.SetDiskPolicy(DiskPolicyType.Overflows);

      Properties<string, string> sqliteProperties = new Properties<string, string>();
      sqliteProperties.Insert("PageSize", "65536");
      sqliteProperties.Insert("MaxFileSize", "512000000");
      String sqlite_dir = "SqLiteDir" + Process.GetCurrentProcess().Id.ToString();
      sqliteProperties.Insert("PersistenceDirectory", sqlite_dir);

      //rf.SetPersistenceManager(new GemStone.GemFire.Plugins.SQLite.SqLiteImpl<object, object>(), sqliteProperties);
      rf.SetPersistenceManager("SqLiteImpl", "createSqLiteInstance", sqliteProperties);

      CacheHelper.Init();
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>("OverFlowRegion");
      if ((region != null) && !region.IsDestroyed)
      {
        region.GetLocalView().DestroyRegion();
        Assert.IsTrue(region.IsDestroyed, "IRegion<object, object> OverFlowRegion was not destroyed.");
      }
      region = rf.Create<object, object>("OverFlowRegion");
      Assert.IsNotNull(region, "IRegion<object, object> was not created.");
      ValidateAttributes(region);

      //Console.WriteLine("TEST-2");
      // put some values into the cache.
      DoNput(region, 100);
      //Console.WriteLine("TEST-2.1 All PUts Donee");

      CheckNumOfEntries(region, 100);

      //Console.WriteLine("TEST-3");
      // check whether value get evicted and token gets set as overflow
      CheckOverflowToken(region, 100, 20);
      // do some gets... printing what we find in the cache.
      DoNget(region, 100);
      TestEntryDestroy(region);
      TestGetOp(region, 100);

      //Console.WriteLine("TEST-4");
      // test to verify same region repeatedly to ensure that the persistece 
      // files are created and destroyed correctly

      IRegion<object, object> subRegion;
      for (int i = 0; i < 1; i++)
      {
        subRegion = CreateSubRegion(region, "SubRegion", "Gemstone.Gemfire.Plugins.SqLite", "SqLiteImpl<object,object>.Create()");
        subRegion.DestroyRegion();
        Assert.IsTrue(subRegion.IsDestroyed, "Expected region to be destroyed");
        Assert.IsFalse(File.Exists(GetSqLiteFileName(sqlite_dir, "SubRegion")), "Persistence file present after region destroy");
      }
      //Console.WriteLine("TEST-5");
    }


    [Test]
    public void XmlCacheCreationWithOverflow()
    {
      Cache cache = null;
      IRegion<object, object> region1;
      IRegion<object, object> region2;
      IRegion<object, object> region3;
      IRegion<object, object>[] rootRegions;
      //Region<object, object>[] subRegions;
      ICollection<IRegion<object, object>> subRegions;
      /*string host_name = "XML_CACHE_CREATION_TEST";*/
      const UInt32 totalSubRegionsRoot1 = 2;
      const UInt32 totalRootRegions = 3;

      try
      {
        CacheHelper.CloseCache();
        Util.Log("Creating cache with the configurations provided in valid_overflowAttr.xml");
        string cachePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "valid_overflowAttr.xml";
        cache = CacheFactory.CreateCacheFactory().Set("cache-xml-file", cachePath).Create();
        Util.Log("Successfully created the cache.");
        rootRegions = cache.RootRegions<object, object>();
        Assert.IsNotNull(rootRegions);
        Assert.AreEqual(totalRootRegions, rootRegions.Length);

        Util.Log("Root regions in Cache: ");
        foreach (IRegion<object, object> rg in rootRegions)
        {
          Util.Log('\t' + rg.Name);
        }

        region1 = rootRegions[0];
        //subRegions = region1.SubRegions(true);
        subRegions = region1.SubRegions(true);
        Assert.IsNotNull(subRegions);
        Assert.AreEqual(subRegions.Count, totalSubRegionsRoot1);

        Util.Log("SubRegions for the root region: ");
        foreach (IRegion<object, object> rg in subRegions)
        {
          Util.Log('\t' + rg.Name);
        }

        Util.Log("Testing if the nesting of regions is correct...");
        region2 = rootRegions[1];
        subRegions = region2.SubRegions(true);
        string childName;
        string parentName;
        foreach (IRegion<object, object> rg in subRegions)
        {
          childName = rg.Name;
          IRegion<object, object> parent = rg.ParentRegion;
          parentName = parent.Name;
          if (childName == "SubSubRegion221")
          {
            Assert.AreEqual("SubRegion22", parentName);
          }
        }

        region3 = rootRegions[2];
        //subRegions = region1.SubRegions(true);
        subRegions = region3.SubRegions(true);
        Assert.IsNotNull(subRegions);
        Assert.AreEqual(subRegions.Count, totalSubRegionsRoot1);

        Util.Log("SubRegions for the root region: ");
        foreach (IRegion<object, object> rg in subRegions)
        {
          Util.Log('\t' + rg.Name);
        }

        GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> attrs = region1.Attributes;
        //Util.Log("Attributes of root region Root1 are: ");

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
        string persistenceDir, maxPageCount, pageSize;

        string lib = attrs.PersistenceLibrary;
        string libFun = attrs.PersistenceFactory;
        Util.Log(" persistence library1 = " + lib);
        Util.Log(" persistence function1 = " + libFun);
        Properties<string, string> pconfig = attrs.PersistenceProperties;
        Assert.IsNotNull(pconfig, "Persistence properties should not be null for root1.");
        persistenceDir = (string)pconfig.Find("PersistenceDirectory");
        maxPageCount = (string)pconfig.Find("MaxPageCount");
        pageSize = (string)pconfig.Find("PageSize");
        Assert.IsNotNull(persistenceDir, "Persistence directory should not be null.");
        Assert.AreNotEqual(0, persistenceDir.Length, "Persistence directory should not be empty.");
        Assert.IsNotNull(maxPageCount, "Persistence MaxPageCount should not be null.");
        Assert.AreNotEqual(0, maxPageCount.Length, "Persistence MaxPageCount should not be empty.");
        Assert.IsNotNull(pageSize, "Persistence PageSize should not be null.");
        Assert.AreNotEqual(0, pageSize.Length, "Persistence PageSize should not be empty.");
        Util.Log("****Attributes of Root1 are correctly set****");

        GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> attrs2 = region2.Attributes;
        string lib2 = attrs2.PersistenceLibrary;
        string libFun2 = attrs2.PersistenceFactory;
        Util.Log(" persistence library2 = " + lib2);
        Util.Log(" persistence function2 = " + libFun2);
        Properties<string, string> pconfig2 = attrs2.PersistenceProperties;
        Assert.IsNotNull(pconfig2, "Persistence properties should not be null for root2.");
        persistenceDir = (string)pconfig2.Find("PersistenceDirectory");
        maxPageCount = (string)pconfig2.Find("MaxPageCount");
        maxPageCount = (string)pconfig2.Find("PageSize");
        Assert.IsNotNull(persistenceDir, "Persistence directory should not be null.");
        Assert.AreNotEqual(0, persistenceDir.Length, "Persistence directory should not be empty.");
        Assert.IsNotNull(maxPageCount, "Persistence MaxPageCount should not be null.");
        Assert.AreNotEqual(0, maxPageCount.Length, "Persistence MaxPageCount should not be empty.");
        Assert.IsNotNull(pageSize, "Persistence PageSize should not be null.");
        Assert.AreNotEqual(0, pageSize.Length, "Persistence PageSize should not be empty.");

        Util.Log("****Attributes of Root2 are correctly set****");

        GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> attrs3 = region3.Attributes;
        //Util.Log("Attributes of root region Root1 are: ");

        Assert.IsTrue(attrs3.CachingEnabled);
        Assert.AreEqual(35, attrs3.LruEntriesLimit);
        Assert.AreEqual(10, attrs3.ConcurrencyLevel);
        Assert.AreEqual(25, attrs3.InitialCapacity);
        Assert.AreEqual(20, attrs3.RegionIdleTimeout);
        Assert.AreEqual(ExpirationAction.Destroy, attrs3.RegionIdleTimeoutAction);
        Assert.AreEqual(DiskPolicyType.Overflows, attrs3.DiskPolicy);

        Util.Log(" persistence library1 = " + attrs3.PersistenceLibrary);
        Util.Log(" persistence function1 = " + attrs3.PersistenceFactory);
        Properties<string, string> pconfig3 = attrs.PersistenceProperties;
        Assert.IsNotNull(pconfig3, "Persistence properties should not be null for root1.");
        Assert.IsNotNull(pconfig3.Find("PersistenceDirectory"), "Persistence directory should not be null.");
        Assert.AreNotEqual(0, pconfig3.Find("PersistenceDirectory").Length, "Persistence directory should not be empty.");
        Assert.IsNotNull(pconfig3.Find("MaxPageCount"), "Persistence MaxPageCount should not be null.");
        Assert.AreNotEqual(0, pconfig3.Find("MaxPageCount").Length, "Persistence MaxPageCount should not be empty.");
        Assert.IsNotNull(pconfig3.Find("PageSize"), "Persistence PageSize should not be null.");
        Assert.AreNotEqual(0, pconfig3.Find("PageSize"), "Persistence PageSize should not be empty.");
        Util.Log("****Attributes of Root1 are correctly set****");

        region1.DestroyRegion(null);
        region2.DestroyRegion(null);
        region3.DestroyRegion(null);

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

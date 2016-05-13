//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [Serializable]
  public class CustomPartitionResolver : IPartitionResolver
  {
    public CustomPartitionResolver()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver";
    }

    public ICacheableKey GetRoutingObject(EntryEvent key)
    {      
      string newKey = key.Key.ToString();
      int numKey = Convert.ToInt32(newKey);
      return CacheableInt32.Create(numKey + 5);
    }

    public static CustomPartitionResolver Create()
    {
      return new CustomPartitionResolver();
    }
  }

  [Serializable]
  public class CustomPartitionResolver1 : IFixedPartitionResolver
  {
    public CustomPartitionResolver1()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver1";
    }

    public ICacheableKey GetRoutingObject(EntryEvent key)
    {
      Util.Log("CustomPartitionResolver1::GetRoutingObject");
      string newKey = key.Key.ToString();
      int numKey = Convert.ToInt32(newKey);
      return CacheableInt32.Create(numKey + 5);
    }

    public string GetPartitionName(EntryEvent entryEvent, CacheableHashSet targetPartitions)
    {
      Util.Log("CustomPartitionResolver1::GetPartitionName");
      string key = entryEvent.Key.ToString();
      int numKey = Convert.ToInt32(key);
      int newkey = numKey % 6;
      if (newkey == 0) {
        return "P1";
      }
      else if (newkey == 1) {
        return "P2";
      }
      else if (newkey == 2) {
        return "P3";
      }
      else if (newkey == 3) {
        return "P4";
      }
      else if (newkey == 4) {
        return "P5";
      }
      else if (newkey == 5) {
        return "P6";
      }
      else {
        return "Invalid";
      }
    }

    public static CustomPartitionResolver1 Create()
    {
      return new CustomPartitionResolver1();
    }
  }

  [Serializable]
  public class CustomPartitionResolver2 : IFixedPartitionResolver
  {
    public CustomPartitionResolver2()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver2";
    }

    public ICacheableKey GetRoutingObject(EntryEvent key)
    {
      string newKey = key.Key.ToString();
      int numKey = Convert.ToInt32(newKey);
      return CacheableInt32.Create(numKey + 4);
    }

    public string GetPartitionName(EntryEvent entryEvent, CacheableHashSet targetPartitions)
    {
      string key = entryEvent.Key.ToString();
      int numKey = Convert.ToInt32(key);
      int newkey = numKey % 6;
      if (newkey == 0) {
        return "P1";
      }
      else if (newkey == 1) {
        return "P2";
      }
      else if (newkey == 2) {
        return "P3";
      }
      else if (newkey == 3) {
        return "P4";
      }
      else if (newkey == 4) {
        return "P5";
      }
      else if (newkey == 5) {
        return "P6";
      }
      else {
        return "Invalid";
      }
    }

    public static CustomPartitionResolver2 Create()
    {
      return new CustomPartitionResolver2();
    }
  }

  [Serializable]
  public class CustomPartitionResolver3 : IFixedPartitionResolver
  {
    public CustomPartitionResolver3()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver3";
    }

    public ICacheableKey GetRoutingObject(EntryEvent key)
    {
      string newKey = key.Key.ToString();
      int numKey = Convert.ToInt32(newKey);
      return CacheableInt32.Create(numKey % 5);
    }

    public string GetPartitionName(EntryEvent entryEvent, CacheableHashSet targetPartitions)
    {
      string key = entryEvent.Key.ToString();
      int numKey = Convert.ToInt32(key);
      int newkey = numKey % 3;
      if (newkey == 0) {
        return "P1";
      }
      else if (newkey == 1) {
        return "P2";
      }
      else if (newkey == 2) {
        return "P3";
      }      
      else {
        return "Invalid";
      }
    }

    public static CustomPartitionResolver3 Create()
    {
      return new CustomPartitionResolver3();
    }
  }

  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientRegionTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      CacheHelper.StopJavaServers();
      base.EndTests();
    }

    [TearDown]
    public override void EndTest()
    {
      try {
        m_client1.Call(DestroyRegions);
        m_client2.Call(DestroyRegions);
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
      finally {
        CacheHelper.StopJavaServers();
        CacheHelper.StopJavaLocators();
      }
      base.EndTest();
    }

    public void RegisterOtherType()
    {
      try {
        Serializable.RegisterType(OtherType.CreateDeserializable);
      }
      catch (IllegalStateException) {
        // ignored since we run multiple times for pool and non pool cases.
      }
    }

    public void DoPutsOtherTypeWithEx(OtherType.ExceptionType exType)
    {
      Region region = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      for (int keyNum = 1; keyNum <= 10; ++keyNum) {
        try {
          region.Put("key-" + keyNum, new OtherType(keyNum,
            keyNum * keyNum, exType));
          if (exType != OtherType.ExceptionType.None) {
            Assert.Fail("Expected an exception in Put");
          }
        }
        catch (GemFireIOException ex) {
          if (exType == OtherType.ExceptionType.Gemfire) {
            // Successfully changed exception back and forth
            Util.Log("Got expected exception in Put: " + ex);
          }
          else if (exType == OtherType.ExceptionType.GemfireGemfire) {
            if (ex.InnerException is CacheServerException) {
              // Successfully changed exception back and forth
              Util.Log("Got expected exception in Put: " + ex);
            }
            else {
              throw;
            }
          }
          else {
            throw;
          }
        }
        catch (CacheServerException ex) {
          if (exType == OtherType.ExceptionType.GemfireSystem) {
            if (ex.InnerException is IOException) {
              // Successfully changed exception back and forth
              Util.Log("Got expected exception in Put: " + ex);
            }
            else {
              throw;
            }
          }
          else {
            throw;
          }
        }
        catch (IOException ex) {
          if (exType == OtherType.ExceptionType.System) {
            // Successfully changed exception back and forth
            Util.Log("Got expected system exception in Put: " + ex);
          }
          else {
            throw;
          }
        }
        catch (ApplicationException ex) {
          if (exType == OtherType.ExceptionType.SystemGemfire) {
            if (ex.InnerException is CacheServerException) {
              // Successfully changed exception back and forth
              Util.Log("Got expected system exception in Put: " + ex);
            }
            else {
              throw;
            }
          }
          else if (exType == OtherType.ExceptionType.SystemSystem) {
            if (ex.InnerException is IOException) {
              // Successfully changed exception back and forth
              Util.Log("Got expected system exception in Put: " + ex);
            }
            else {
              throw;
            }
          }
          else {
            throw;
          }
        }
      }
    }

    public void RegexInterestAllStep2() //client 2 //pxr2
    {
      Util.Log("RegexInterestAllStep2 Enters.");
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      List<ICacheableKey> resultKeys = new List<ICacheableKey>();
      //CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      //CreateEntry(m_regionNames[1], m_keys[1], m_vals[1]);
      region0.RegisterAllKeys(false, resultKeys, true);
      region1.RegisterAllKeys(false, null, true);
      if (region0.Size != 1 || region1.Size != 1) {
        Assert.Fail("Expected one entry in region");
      }
      if (resultKeys.Count != 1) {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      ICacheableKey value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[0]))) {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }
      Util.Log("RegexInterestAllStep2 complete.");
    }

    public void CheckAndPutKey()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);

      IRegionService regServ = region0.RegionService;

      Cache cache = regServ as Cache;

      Assert.IsNotNull(cache);

      if (region0.ContainsKeyOnServer("keyKey01")) {
        Assert.Fail("Did not expect keyKey01 to be on Server");
      }
      region0.Put("keyKey01", "valueValue01");
      if (!region0.ContainsKeyOnServer("keyKey01")) {
        Assert.Fail("Expected keyKey01 to be on Server");
      }
    }

    public void ClearRegionStep1()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region0.RegisterAllKeys();
      CreateEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      CreateEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      if (region0.Size != 2) {
        Assert.Fail("Expected region size 2");
      }
    }
    public void ClearRegionListenersStep1()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region0.RegisterAllKeys();
      AttributesMutator attrMutator = region0.GetAttributesMutator();
      TallyListener listener = new TallyListener();
      attrMutator.SetCacheListener(listener);
      TallyWriter writer = new TallyWriter();
      attrMutator.SetCacheWriter(writer);
    }
    public void ClearRegionStep2()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region0.Clear();
      if (region0.Size != 0) {
        Assert.Fail("Expected region size 0");
      }
      Thread.Sleep(20000);
    }
    public void ClearRegionListenersStep2()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      if (region0.Size != 2) {
        Assert.Fail("Expected region size 2");
      }
    }
    public void ClearRegionListenersStep3()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      if (region0.Size != 0) {
        Assert.Fail("Expected region size 0");
      }
      RegionAttributes attr = region0.Attributes;
      TallyListener listener = attr.CacheListener as TallyListener;
      TallyWriter writer = attr.CacheWriter as TallyWriter;
      if (listener.Clears != 1) {
        Assert.Fail("Expected listener clear count 1");
      }
      if (writer.Clears != 1) {
        Assert.Fail("Expected writer clear count 1");
      }
      CreateEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      CreateEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      if (region0.Size != 2) {
        Assert.Fail("Expected region size 2");
      }
      region0.LocalClear();
      if (listener.Clears != 2) {
        Assert.Fail("Expected listener clear count 2");
      }
      if (writer.Clears != 2) {
        Assert.Fail("Expected writer clear count 2");
      }
      if (region0.Size != 0) {
        Assert.Fail("Expected region size 0");
      }
    }
    public void ClearRegionStep3()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      if (region0.Size != 2) {
        Assert.Fail("Expected region size 2");
      }
      if (!region0.ContainsKeyOnServer(m_keys[0])) {
        Assert.Fail("m_key[0] is not on Server");
      }
      if (!region0.ContainsKeyOnServer(m_keys[1])) {
        Assert.Fail("m_key[1] is not on Server");
      }
    }
    public void GetInterests()
    {
      string[] testregex = { "Key-*1", "Key-*2", "Key-*3", "Key-*4", "Key-*5", "Key-*6" };
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region0.RegisterRegex(testregex[0]);
      region0.RegisterRegex(testregex[1]);
      region0.RegisterKeys(new CacheableKey[] {
          new CacheableString(m_keys[0]) });
      region0.RegisterKeys(new CacheableKey[] {
          new CacheableString(m_keys[1]) });

      string[] regv = region0.GetInterestListRegex();
      if (regv.Length != 2) {
        Assert.Fail("regex list length is not 2");
      }
      for (int i = 0; i < regv.Length; i++) {
        Util.Log("regv[{0}]={1}", i, regv[i]);
        bool found = false;
        for (int j = 0; j < regv.Length; j++) {
          if (regv[i].Equals(testregex[j])) {
            found = true;
            break;
          }
        }
        if (!found) {
          Assert.Fail("Unexpected regex");
        }
      }

      ICacheableKey[] keyv = region0.GetInterestList();
      if (keyv.Length != 2) {
        Assert.Fail("interest list length is not 2");
      }
      for (int i = 0; i < keyv.Length; i++) {
        Util.Log("keyv[{0}]={1}", i, keyv[i].ToString());
        bool found = false;
        for (int j = 0; j < keyv.Length; j++) {
          if (keyv[i].ToString().Equals(m_keys[j])) {
            found = true;
            break;
          }
        }
        if (!found) {
          Assert.Fail("Unexpected key");
        }
      }
    }

    public void RegexInterestAllStep3(string endpoints, bool pool, bool locator)//client 2
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      region0.UnregisterAllKeys();
      region1.UnregisterAllKeys();
      region0.LocalDestroyRegion();
      region1.LocalDestroyRegion();
      List<ICacheableKey> resultKeys = new List<ICacheableKey>();
      if (pool) {
        if (locator) {
          CreateTCRegions_Pool(RegionNames, (string)null, endpoints, "__TESTPOOL1_", true);
        }
        else {
          CreateTCRegions_Pool(RegionNames, endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else {
        CreateTCRegions(RegionNames, endpoints, true);
      }
      region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      CreateEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      region0.RegisterRegex(".*", false, resultKeys, true);
      region1.RegisterRegex(".*", false, null, true);
      if (region0.Size != 1) {
        Assert.Fail("Expected one entry in region");
      }
      if (region1.Size != 1) {
        Assert.Fail("Expected one entry in region");
      }
      if (resultKeys.Count != 1) {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      ICacheableKey value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[0]))) {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      Util.Log("RegexInterestAllStep3 complete.");
    }

    public void RegexInterestAllStep4()//client 1
    {
      List<ICacheableKey> resultKeys = new List<ICacheableKey>();
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region0.RegisterAllKeys(false, resultKeys, false);
      if (region0.Size != 1) {
        Assert.Fail("Expected one entry in region");
      }
      if (!region0.ContainsKey(m_keys[0])) {
        Assert.Fail("Expected region to contain the key");
      }

      if (region0.ContainsValueForKey(m_keys[0])) {
        Assert.Fail("Expected region to not contain the value");
      }
      if (resultKeys.Count != 1) {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      ICacheableKey value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[0]))) {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }

      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      resultKeys.Clear();
      region1.RegisterRegex(".*", false, resultKeys, false);

      if (region1.Size != 1) {
        Assert.Fail("Expected one entry in region");
      }

      if (!region1.ContainsKey(m_keys[2])) {
        Assert.Fail("Expected region to contain the key");
      }

      if (region1.ContainsValueForKey(m_keys[2])) {
        Assert.Fail("Expected region to not contain the value");
      }
      if (resultKeys.Count != 1) {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[2]))) {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }
      CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], false);
      CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public void RegexInterestAllStep5()//client 2
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      if (region1.Size != 2 || region0.Size != 2) {
        Assert.Fail("Expected two entry in region");
      }
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);

    }

    public void RegexInterestAllStep6()//client 1
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], false);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], false);
    }

    public void RegexInterestAllStep7() //client 2 
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1], false);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3], false);
    }

    public void RegexInterestAllStep8()//client 2
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      region0.UnregisterAllKeys();
      region1.UnregisterAllKeys();
    }

    public void RegexInterestAllStep9() //client 1
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], false);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1], false);
      UpdateEntry(m_regionNames[1], m_keys[2], m_vals[2], false);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3], false);
    }

    public void RegexInterestAllStep10() //client 2
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public void GetAll(string endpoints, bool pool, bool locator)
    {
      Util.Log("Enters GetAll");
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      List<ICacheableKey> resultKeys = new List<ICacheableKey>();
      CacheableKey key0 = m_keys[0];
      CacheableKey key1 = m_keys[1];
      resultKeys.Add(key0);
      resultKeys.Add(key1);
      region0.LocalDestroyRegion();
      region0 = null;
      if (pool) {
        if (locator) {
          region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, false, null, null, endpoints, "__TESTPOOL1_", true); //caching enable false
        }
        else {
          region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, false, null, endpoints, null, "__TESTPOOL1_", true); //caching enable false
        }
      }
      else {
        region0 = CacheHelper.CreateTCRegion(RegionNames[0], true, false, null, endpoints, true); //caching enable false
      }
      try {
        region0.GetAll(resultKeys.ToArray(), null, null, true);
        Assert.Fail("Expected IllegalArgumentException");
      }
      catch (IllegalArgumentException ex) {
        Util.Log("Got expected IllegalArgumentException" + ex.Message);
      }
      //recreate region with caching enabled true
      region0.LocalDestroyRegion();
      region0 = null;
      if (pool) {
        if (locator) {
          region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, true, null, null, endpoints, "__TESTPOOL1_", true); //caching enable true
        }
        else {
          region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, true, null, endpoints, null, "__TESTPOOL1_", true); //caching enable true
        }
      }
      else {
        region0 = CacheHelper.CreateTCRegion(RegionNames[0], true, true, null, endpoints, true); //caching enable true
      }
      Dictionary<ICacheableKey, IGFSerializable> values = new Dictionary<ICacheableKey, IGFSerializable>();
      Dictionary<ICacheableKey, Exception> exceptions = new Dictionary<ICacheableKey, Exception>();
      resultKeys.Clear();
      try {
        region0.GetAll(resultKeys.ToArray(), values, exceptions);
        Assert.Fail("Expected IllegalArgumentException");
      }
      catch (IllegalArgumentException ex) {
        Util.Log("Got expected IllegalArgumentException" + ex.Message);
      }
      resultKeys.Add(key0);
      resultKeys.Add(key1);
      try {
        region0.GetAll(resultKeys.ToArray(), null, null, false);
        Assert.Fail("Expected IllegalArgumentException");
      }
      catch (IllegalArgumentException ex) {
        Util.Log("Got expected IllegalArgumentException" + ex.Message);
      }
      region0.GetAll(resultKeys.ToArray(), values, exceptions);
      if (values.Count != 2) {
        Assert.Fail("Expected 2 values");
      }
      if (exceptions.Count != 0) {
        Assert.Fail("No exception expected");
      }
      try {
        CacheableString val0 = (CacheableString)values[(resultKeys[0])];
        CacheableString val1 = (CacheableString)values[(resultKeys[1])];
        if (!(val0.ToString().Equals(m_nvals[0])) || !(val1.ToString().Equals(m_nvals[1]))) {
          Assert.Fail("Got unexpected value");
        }
      }
      catch (Exception ex) {
        Assert.Fail("Key should have been found" + ex.Message);
      }
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      CacheableKey key2 = m_keys[2];
      CacheableKey key3 = m_keys[3];
      region1.LocalInvalidate(key2);
      resultKeys.Clear();
      resultKeys.Add(key2);
      resultKeys.Add(key3);
      values.Clear();
      exceptions.Clear();
      region1.GetAll(resultKeys.ToArray(), values, exceptions, true);
      if (values.Count != 2) {
        Assert.Fail("Expected 2 values");
      }
      if (exceptions.Count != 0) {
        Assert.Fail("Expected no exception");
      }
      try {
        CacheableString val2 = (CacheableString)values[(resultKeys[0])];
        CacheableString val3 = (CacheableString)values[(resultKeys[1])];
        if (!(val2.ToString().Equals(m_nvals[2])) || !(val3.ToString().Equals(m_vals[3]))) {
          Assert.Fail("Got unexpected value");
        }
      }
      catch (Exception ex) {
        Assert.Fail("Key should have been found" + ex.Message);
      }
      if (region1.Size != 2) {
        Assert.Fail("Expected 2 entry in the region");
      }
      RegionEntry[] regionEntry = region1.GetEntries(false);//Not of subregions
      if (regionEntry.Length != 2) {
        Assert.Fail("Should have two values in the region");
      }
      VerifyEntry(RegionNames[1], m_keys[2], m_nvals[2], true);
      VerifyEntry(RegionNames[1], m_keys[3], m_vals[3], true);
      region1.LocalInvalidate(key3);
      values = null;
      exceptions.Clear();
      region1.GetAll(resultKeys.ToArray(), values, exceptions, true);
      if (region1.Size != 2) {
        Assert.Fail("Expected 2 entry in the region");
      }
      regionEntry = region1.GetEntries(false);
      if (regionEntry.Length != 2) {
        Assert.Fail("Should have two values in the region");
      }
      VerifyEntry(RegionNames[1], m_keys[2], m_nvals[2], true);
      VerifyEntry(RegionNames[1], m_keys[3], m_nvals[3], true);
      Util.Log("Exits GetAll");
    }

    private void UpdateEntry(string p, string p_2, string p_3)
    {
      throw new Exception("The method or operation is not implemented.");
    }

    public void PutAllStep3()//client 1
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      List<ICacheableKey> resultKeys = new List<ICacheableKey>();
      CacheableKey key0 = m_keys[0];
      CacheableKey key1 = m_keys[1];
      resultKeys.Add(key0);
      resultKeys.Add(key1);
      region0.RegisterKeys(resultKeys.ToArray());
      Util.Log("Step three completes");
    }

    public void SingleHopGetAllTask()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      CacheableKey[] array = new CacheableKey[100];
      for (int y = 0; y < 100; y++) {
        Util.Log("put:{0}", y);
        region0.Put((CacheableKey)new CacheableInt32(y), new CacheableInt32(y));
        array[y] = (CacheableKey)new CacheableInt32(y);
      }
           
      Dictionary<ICacheableKey, IGFSerializable> values = new Dictionary<ICacheableKey, IGFSerializable>();
      Dictionary<ICacheableKey, Exception> exceptions = new Dictionary<ICacheableKey, Exception>();
      region0.GetAll(array, values, exceptions, true);
      int length = array.Length;
      Util.Log("GetAll array count is {0}", length);
      Assert.AreEqual(100, length);

      int z = values.Count;
      for (int w = 0; w < z; w++) {
        z = values.Count;        
        CacheableInt32 q = (CacheableInt32)values[new CacheableInt32(w)];
        Util.Log("actual value:{0} expected value: {1} ", q, w);
        Assert.AreEqual(q, new CacheableInt32(w));        
      }

    }

    public void PutAllStep4() //client 2
    {
      CacheableHashMap map0 = new CacheableHashMap();
      CacheableHashMap map1 = new CacheableHashMap();
      CacheableKey key0 = m_keys[0];
      CacheableKey key1 = m_keys[1];
      CacheableString val0 = new CacheableString(m_vals[0]);
      CacheableString val1 = new CacheableString(m_vals[1]);
      map0.Add(key0, val0);
      map0.Add(key1, val1);

      CacheableKey key2 = m_keys[2];
      CacheableKey key3 = m_keys[3];
      CacheableString val2 = new CacheableString(m_vals[2]);
      CacheableString val3 = new CacheableString(m_vals[3]);
      map1.Add(key2, val2);
      map1.Add(key3, val3);
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      region0.PutAll(map0);
      region1.PutAll(map1);
      Util.Log("Put All Complets");
    }

    public void PutAllStep5() //client 1
    {
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[0], m_keys[1]);

      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);

      DoNetsearch(m_regionNames[1], m_keys[2], m_vals[2], true);
      DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], true);

      Util.Log("StepFive complete.");
    }

    public void PutAllStep6() //client 2
    {
      CacheableHashMap map0 = new CacheableHashMap();
      CacheableHashMap map1 = new CacheableHashMap();
      CacheableKey key0 = m_keys[0];
      CacheableKey key1 = m_keys[1];
      CacheableString val0 = new CacheableString(m_nvals[0]);
      CacheableString val1 = new CacheableString(m_nvals[1]);
      map0.Add(key0, val0);
      map0.Add(key1, val1);

      CacheableKey key2 = m_keys[2];
      CacheableKey key3 = m_keys[3];
      CacheableString val2 = new CacheableString(m_nvals[2]);
      CacheableString val3 = new CacheableString(m_nvals[3]);
      map1.Add(key2, val2);
      map1.Add(key3, val3);
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      region0.PutAll(map0);
      region1.PutAll(map1);
      Util.Log("Step6 Complets");
    }

    public void PutAllStep7() //client 1
    {
      //Region0 is changed at client 1 because keys[0] and keys[1] were registered.PutAllStep3
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      // region1 is not changed at client beacuse no regsiter interest.
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);
      Util.Log("PutAllStep7 complete.");
    }

    void runDistOps(bool pool, bool locator)
    {
      Util.Log("Starting iteration for pool " + pool + ", locator " + locator);

      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateNonExistentRegion, (string)null, CacheHelper.Locators, pool);
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
              (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateNonExistentRegion, CacheHelper.Endpoints, (string)null, pool);
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
              CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateNonExistentRegion, CacheHelper.Endpoints, (string)null, pool);
        m_client1.Call(CreateTCRegions, RegionNames,
            CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      m_client1.Call(CheckServerKeys);
      m_client1.Call(StepFive, true);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSix, true);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runDistOps2(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml", "cacheserver3.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        Util.Log("Cacheserver 3 started.");
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml", "cacheserver2.xml", "cacheserver3.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServer(3, "GFECS3");
        Util.Log("Cacheserver 3 started.");
      }

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
            CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFive, true);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSix, true);
      Util.Log("StepSix complete.");
      m_client1.Call(GetAll, CacheHelper.Endpoints, pool, locator);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");
      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCheckPutGet(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }

      Util.Log("Cacheserver 1 started.");

      PutGetTests putGetTest = new PutGetTests();

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("Client 1 (pool locators) regions created");
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("Client 2 (pool locators) regions created");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("Client 1 (pool endpoints) regions created");
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("Client 2 (pool endpoints) regions created");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("Client 1 regions created");
        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("Client 2 regions created");
      }

      m_client1.Call(putGetTest.SetRegion, RegionNames[0]);
      m_client2.Call(putGetTest.SetRegion, RegionNames[0]);

      CacheableHelper.RegisterBuiltins();

      putGetTest.TestAllKeyValuePairs(m_client1, m_client2,
        RegionNames[0], true, pool);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }
    
    void runCheckPutGetWithAppDomain(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }

      Util.Log("Cacheserver 1 started.");

      PutGetTests putGetTest = new PutGetTests();

      m_client1.Call(InitializeAppDomain);

      if (pool)
      {
        if (locator)
        {
          m_client1.Call(CreateTCRegions_Pool_AD, RegionNames,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false, false);
          Util.Log("Client 1 (pool locators) regions created");
        }
        else
        {
          m_client1.Call(CreateTCRegions_Pool_AD, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false, false);
          Util.Log("Client 1 (pool endpoints) regions created");
        }
      }
      
      m_client1.Call(SetRegionAD, RegionNames[0]);
      //m_client2.Call(putGetTest.SetRegion, RegionNames[0]);

     // CacheableHelper.RegisterBuiltins();

      //putGetTest.TestAllKeyValuePairs(m_client1, m_client2,
        //RegionNames[0], true, pool);
      m_client1.Call(TestAllKeyValuePairsAD, RegionNames[0], true, pool);
      m_client1.Call(CloseCacheAD);
      Util.Log("Client 1 closed");
      
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runPartitionResolver(bool pool, bool locator)
    {
      if (pool && locator) {
      CacheHelper.SetupJavaServers(true, "cacheserver1_pr.xml",
          "cacheserver2_pr.xml", "cacheserver3_pr.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        Util.Log("Cacheserver 3 started.");
      }
      else {       
        CacheHelper.SetupJavaServers(false, "cacheserver1_pr.xml",
          "cacheserver2_pr.xml", "cacheserver3_pr.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServer(3, "GFECS3");
        Util.Log("Cacheserver 3 started.");
      }      
       
      PutGetTests putGetTest = new PutGetTests();
      // Create and Add partition resolver to the regions.
      CustomPartitionResolver cpr = CustomPartitionResolver.Create();

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool2, RegionNames,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false, cpr);
          Util.Log("Client 1 (pool locators) regions created");
          m_client2.Call(CreateTCRegions_Pool2, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false, cpr);
          Util.Log("Client 2 (pool locators) regions created");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool2, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false, cpr);
          Util.Log("Client 1 (pool endpoints) regions created");
          m_client2.Call(CreateTCRegions_Pool2, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false, cpr);
          Util.Log("Client 2 (pool endpoints) regions created");
        }
      }             
      m_client1.Call(putGetTest.SetRegion, RegionNames[1]);
      m_client2.Call(putGetTest.SetRegion, RegionNames[1]);
      putGetTest.DoPRSHPartitionResolverTasks(m_client1, m_client2, RegionNames[1]);

      m_client1.Call(putGetTest.SetRegion, RegionNames[0]);
      m_client2.Call(putGetTest.SetRegion, RegionNames[0]);
      putGetTest.DoPRSHPartitionResolverTasks(m_client1, m_client2, RegionNames[0]);
    
      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFixedPartitionResolver(bool pool, bool locator)
    {
      if (pool && locator) {
      CacheHelper.SetupJavaServers(true, "cacheserver1_fpr.xml",
          "cacheserver2_fpr.xml", "cacheserver3_fpr.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        Util.Log("Cacheserver 3 started.");
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver1_fpr.xml",
          "cacheserver2_fpr.xml", "cacheserver3_fpr.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServer(3, "GFECS3");
        Util.Log("Cacheserver 3 started.");
      }      
       
      PutGetTests putGetTest = new PutGetTests();
      // Create and Add partition resolver to the regions.
      CustomPartitionResolver1 cpr1 = CustomPartitionResolver1.Create();
      CustomPartitionResolver2 cpr2 = CustomPartitionResolver2.Create();
      CustomPartitionResolver3 cpr3 = CustomPartitionResolver3.Create();

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool1, PartitionRegion1,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false, cpr1);
          Util.Log("Client 1 (pool locators) PartitionRegion1 created");

          m_client1.Call(CreateTCRegions_Pool1, PartitionRegion2,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false, cpr2);
          Util.Log("Client 1 (pool locators) PartitionRegion2 created");

          m_client1.Call(CreateTCRegions_Pool1, PartitionRegion3,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false, cpr3);
          Util.Log("Client 1 (pool locators) PartitionRegion3 created");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool1, PartitionRegion1,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false, cpr1);
          Util.Log("Client 1 (pool endpoints) PartitionRegion1 created");

          m_client1.Call(CreateTCRegions_Pool1, PartitionRegion2,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false, cpr2);
          Util.Log("Client 1 (pool endpoints) PartitionRegion2 created");

          m_client1.Call(CreateTCRegions_Pool1, PartitionRegion3,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false, cpr3);
          Util.Log("Client 1 (pool endpoints) PartitionRegion3 created");
        }
      }
      m_client1.Call(putGetTest.SetRegion, PartitionRegion1);
      putGetTest.DoPRSHFixedPartitionResolverTasks(m_client1, PartitionRegion1);

      m_client1.Call(putGetTest.SetRegion, PartitionRegion2);
      putGetTest.DoPRSHFixedPartitionResolverTasks(m_client1, PartitionRegion2);

      m_client1.Call(putGetTest.SetRegion, PartitionRegion3);
      putGetTest.DoPRSHFixedPartitionResolverTasks(m_client1, PartitionRegion3);
    
      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCheckPut(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver_hashcode.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver_hashcode.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }

      Util.Log("Cacheserver 1 started.");

      PutGetTests putGetTest = new PutGetTests();

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("Client 1 (pool locators) regions created");
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("Client 2 (pool locators) regions created");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("Client 1 (pool endpoints) regions created");
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("Client 2 (pool endpoints) regions created");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("Client 1 regions created");
        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("Client 2 regions created");
      }

      m_client1.Call(putGetTest.SetRegion, RegionNames[0]);
      m_client2.Call(putGetTest.SetRegion, RegionNames[0]);

      CacheableHelper.RegisterBuiltins();
      putGetTest.TestAllKeys(m_client1, m_client2, RegionNames[0]);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCheckNativeException(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(RegisterOtherType);

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");
      }

      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.None);
      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.Gemfire);
      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.System);
      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.GemfireGemfire);
      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.GemfireSystem);
      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.SystemGemfire);
      m_client1.Call(DoPutsOtherTypeWithEx, OtherType.ExceptionType.SystemSystem);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailover(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml", "cacheserver2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
              CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
            CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      if (pool && locator) {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      m_client1.Call(StepFiveFailover);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSix, false);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runNotification(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames, CacheHelper.Endpoints, true);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames, CacheHelper.Endpoints, true);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(RegisterAllKeysR0WithoutValues);
      m_client1.Call(RegisterAllKeysR1WithoutValues);

      m_client2.Call(RegisterAllKeysR0WithoutValues);
      m_client2.Call(RegisterAllKeysR1WithoutValues);

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFive, true);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixNotify, false);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenNotify, false);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEightNotify, false);
      Util.Log("StepEight complete.");

      m_client1.Call(StepNineNotify, false);
      Util.Log("StepNine complete.");

      m_client2.Call(StepTen);
      Util.Log("StepTen complete.");

      m_client1.Call(StepEleven);
      Util.Log("StepEleven complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailover2(bool pool, bool locators)
    {
      // This test is for client failover with client notification.
      if (pool && locators) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml", "cacheserver2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locators) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(RegisterAllKeysR0WithoutValues);
      m_client1.Call(RegisterAllKeysR1WithoutValues);

      m_client2.Call(RegisterAllKeysR0WithoutValues);
      m_client2.Call(RegisterAllKeysR1WithoutValues);

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      if (pool && locators) {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      m_client1.Call(CheckServerKeys);
      m_client1.Call(StepFive, false);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixNotify, false);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenNotify, false);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEightNotify, false);
      Util.Log("StepEight complete.");

      m_client1.Call(StepNineNotify, false);
      Util.Log("StepNine complete.");

      m_client2.Call(StepTen);
      Util.Log("StepTen complete.");

      m_client1.Call(StepEleven);
      Util.Log("StepEleven complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locators) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailover3(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true,
          "cacheserver.xml", "cacheserver2.xml", "cacheserver3.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false,
          "cacheserver.xml", "cacheserver2.xml", "cacheserver3.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
            CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      if (pool && locator) {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        Util.Log("Cacheserver 3 started.");
      }
      else {
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StartJavaServer(3, "GFECS3");
        Util.Log("Cacheserver 3 started.");
      }

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      m_client1.Call(StepFive, false);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSix, false);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");
      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailoverInterestAll(bool pool, bool locator)
    {
      runFailoverInterestAll(pool, locator, false);
    }

    void runFailoverInterestAll(bool pool, bool locator, bool ssl)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC", null, ssl);
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, ssl);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
             (string)null, CacheHelper.Locators, "__TESTPOOL1_", true, ssl);
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
           CacheHelper.Endpoints, true);
      }

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      if (pool) {
        if (locator) {
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
             (string)null, CacheHelper.Locators, "__TESTPOOL1_", true, ssl);
        }
        else {
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
             CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else {
        m_client2.Call(CreateTCRegions, RegionNames,
           CacheHelper.Endpoints, true);
      }
      Util.Log("CreateTCRegions complete.");

      m_client2.Call(RegexInterestAllStep2);
      Util.Log("RegexInterestAllStep2 complete.");

      if (pool && locator) {
        m_client2.Call(RegexInterestAllStep3, CacheHelper.Locators, pool, locator);
      }
      else {
        m_client2.Call(RegexInterestAllStep3, CacheHelper.Endpoints, pool, locator);
      }
      Util.Log("RegexInterestAllStep3 complete.");

      m_client1.Call(RegexInterestAllStep4);
      Util.Log("RegexInterestAllStep4 complete.");

      m_client2.Call(RegexInterestAllStep5);
      Util.Log("RegexInterestAllStep5 complete.");

      if (pool && locator) {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, ssl);
      }
      else {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }

      CacheHelper.StopJavaServer(1); //failover happens
      Util.Log("Cacheserver 2 started and failover forced");

      m_client1.Call(RegexInterestAllStep6);
      Util.Log("RegexInterestAllStep6 complete.");

      System.Threading.Thread.Sleep(5000); // sleep to let updates arrive

      m_client2.Call(RegexInterestAllStep7);
      Util.Log("RegexInterestAllStep7 complete.");

      m_client2.Call(RegexInterestAllStep8);
      Util.Log("RegexInterestAllStep8 complete.");

      m_client1.Call(RegexInterestAllStep9);
      Util.Log("RegexInterestAllStep9 complete.");

      m_client2.Call(RegexInterestAllStep10);
      Util.Log("RegexInterestAllStep10 complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1, true, ssl);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runPutAll(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
      }

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);  //Client Notification true for client 1
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", true);  //Cleint notification true for client 2
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);  //Client Notification true for client 1
          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", true);  //Cleint notification true for client 2
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);  //Client Notification true for client 1
        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);  //Cleint notification true for client 2
      }
      Util.Log("Region creation complete.");

      m_client1.Call(PutAllStep3);
      Util.Log("PutAllStep3 complete.");

      m_client2.Call(PutAllStep4);
      Util.Log("PutAllStep4 complete.");

      m_client1.Call(PutAllStep5);
      Util.Log("PutAllStep5 complete.");

      m_client2.Call(PutAllStep6);
      Util.Log("PutAllStep6 complete.");

      m_client1.Call(PutAllStep7);
      Util.Log("PutAllStep7 complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRemoveOps(bool pool, bool locator)
    {
      Util.Log("Starting iteration for pool " + pool + ", locator " + locator);

      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
              (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
              CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames,
            CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
        Util.Log("StepTwo complete.");
      }

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client1.Call(RemoveStepFive);
      Util.Log("RemoveStepFive complete.");

      m_client2.Call(RemoveStepSix);
      Util.Log("RemoveStepSix complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRemoveOps1(bool pool, bool locator)
    {
      Util.Log("Starting iteration for pool " + pool + ", locator " + locator);

      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver1_expiry.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver1_expiry.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames2,
              (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool locators) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames2,
            (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool locators) complete.");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames2,
              CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepOne (pool endpoints) complete.");

          m_client2.Call(CreateTCRegions_Pool, RegionNames2,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("StepTwo (pool endpoints) complete.");
        }
      }
      else {
        m_client1.Call(CreateTCRegions, RegionNames2,
            CacheHelper.Endpoints, false);
        Util.Log("StepOne complete.");

        m_client2.Call(CreateTCRegions, RegionNames2,
          CacheHelper.Endpoints, false);
        Util.Log("StepTwo complete.");
      }

      m_client2.Call(RemoveStepEight);
      Util.Log("RemoveStepEight complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void CheckPrSingleHopForGetAllTask(bool pool, bool locator)
    {
      if (pool && locator) {
        CacheHelper.SetupJavaServers(true, "cacheserver1_pr.xml",
          "cacheserver1_pr.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator 1 started.");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      }
      else {
        CacheHelper.SetupJavaServers(false, "cacheserver1_pr.xml",
          "cacheserver1_pr.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
      }

      if (pool) {
        if (locator) {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
           (string)null, CacheHelper.Locators, "__TESTPOOL1_", false);
          Util.Log("Client 1 (pool locators) regions created");
        }
        else {
          m_client1.Call(CreateTCRegions_Pool, RegionNames,
            CacheHelper.Endpoints, (string)null, "__TESTPOOL1_", false);
          Util.Log("Client 1 (pool endpoints) regions created");
        }
      }      
      Util.Log("Region creation complete.");

      m_client1.Call(SingleHopGetAllTask);
      Util.Log("SingleHopGetAllTask complete.");      

      m_client1.Call(Close);
      Util.Log("Client 1 closed");      

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator) {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    #region Tests

    [Test]    
    public void CheckKeyOnServer()
    {
      CacheHelper.SetupJavaServers("cacheserver.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, false);
      Util.Log("StepOne of region creation complete.");

      m_client1.Call(CheckAndPutKey);
      Util.Log("Check for ContainsKeyOnServer complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    [Test]
    public void RegionClearTest()
    {
      CacheHelper.SetupJavaServers("cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
      Util.Log("client 1 StepOne of region creation complete.");
      m_client2.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
      Util.Log("client 2 StepOne of region creation complete.");

      m_client1.Call(ClearRegionListenersStep1);
      m_client2.Call(ClearRegionStep1);
      m_client1.Call(ClearRegionListenersStep2);
      m_client2.Call(ClearRegionStep2);
      m_client1.Call(ClearRegionListenersStep3);
      m_client2.Call(ClearRegionStep3);
      Util.Log("StepTwo of check for RegionClearTest complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }
    [Test]
    public void GetInterestsOnClient()
    {
      CacheHelper.SetupJavaServers("cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions, RegionNames,
          CacheHelper.Endpoints, true);
      Util.Log("StepOne of region creation complete.");

      m_client1.Call(GetInterests);
      Util.Log("StepTwo of check for GetInterestsOnClient complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }
  
    [Test]
    public void DistOps()
    {
      runDistOps(false, false); // region config
      runDistOps(true, false); // pool with server endpoints
      runDistOps(true, true); // pool with locators
    }

    [Test]
    public void DistOps2()
    {
      runDistOps2(false, false); // region config
      runDistOps2(true, false); // pool with server endpoints
      runDistOps2(true, true); // pool with locators
    }

    [Test]
    public void Notification()
    {
      runNotification(false, false); // region config
      runNotification(true, false); // pool with server endpoints
      runNotification(true, true); // pool with locators
    }

    [Test]
    public void CheckPutGet()
    {
      runCheckPutGet(false, false); // region config
      runCheckPutGet(true, false); // pool with server endpoints
      runCheckPutGet(true, true); // pool with locators
    }

    
    [Test]
    public void CheckPutGetWithAppDomain()
    {
      //runCheckPutGet(false, false); // region config
     // runCheckPutGet(true, false); // pool with server endpoints
      runCheckPutGetWithAppDomain(true, true); // pool with locators
    }

    [Test]
    public void JavaHashCode()
    {
      runCheckPut(false, false); // region config
      runCheckPut(true, false); // pool with server endpoints
      runCheckPut(true, true); // pool with locators
    }

    [Test]
    public void CheckPartitionResolver()
    {
      runPartitionResolver(true, false); // pool with server endpoints
      runPartitionResolver(true, true); // pool with locators
    }

    [Test]
    public void CheckFixedPartitionResolver()
    {
      runFixedPartitionResolver(true, false); // pool with server endpoints
      runFixedPartitionResolver(true, true); // pool with locators
    }

    [Test]
    public void CheckNativeException()
    {
      runCheckNativeException(false, false); // region config
      runCheckNativeException(true, false); // pool with server endpoints
      runCheckNativeException(true, true); // pool with locators
    }

    [Test]
    public void Failover()
    {
      runFailover(false, false); // region config
      runFailover(true, false); // pool with server endpoints
      runFailover(true, true); // pool with locators
    }
    
    [Test]
    public void Failover2()
    {
      runFailover2(false, false); // region config
      runFailover2(true, false); // pool with server endpoints
      runFailover2(true, true); // pool with locators
    }

    [Test]
    public void Failover3()
    {
      runFailover3(false, false); // region config
      runFailover3(true, false); // pool with server endpoints
      runFailover3(true, true); // pool with locators
    }

    [Test]
    public void FailOverInterestAll()
    {
      runFailoverInterestAll(false, false); // region config
      runFailoverInterestAll(true, false); // pool with server endpoints
      runFailoverInterestAll(true, true); // pool with locators
    }

    [Test]
    public void FailOverInterestAllWithSSL()
    {
      // SSL CAN ONLY BE ENABLED WITH LOCATORS THUS REQUIRING POOLS.
      runFailoverInterestAll(true, true, true); // pool with locators with ssl
    }

    [Test]
    public void PutAll()
    {
      runPutAll(false, false); // region config
      runPutAll(true, false); // pool with server endpoints
      runPutAll(true, true); // pool with locators
    }

    [Test]
    public void RemoveOps()
    {
      runRemoveOps(false, false); // region config
      runRemoveOps(true, false); // pool with server endpoints
      runRemoveOps(true, true); // pool with locators
    }

    [Test]
    public void RemoveOps1()
    {
      runRemoveOps1(false, false); // region config
      runRemoveOps1(true, false); // pool with server endpoints
      runRemoveOps1(true, true); // pool with locators
    }

    [Test]
    public void Bug717GetAll()
    {
      CheckPrSingleHopForGetAllTask(true, false); // pool with server endpoints
      CheckPrSingleHopForGetAllTask(true, true); // pool with locators
    }
    #endregion
  }
}

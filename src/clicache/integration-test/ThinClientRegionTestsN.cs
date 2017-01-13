//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Collections;
using System.Collections.ObjectModel;
using System.IO;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [Serializable]
  public class CustomPartitionResolver<TValue> : IPartitionResolver<int, TValue>
  {
    public CustomPartitionResolver()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver";
    }

    public Object GetRoutingObject(EntryEvent<int, TValue> key)
    {
      Util.Log("CustomPartitionResolver::GetRoutingObject");
      return key.Key + 5;
    }

    public static CustomPartitionResolver<TValue> Create()
    {
      return new CustomPartitionResolver<TValue>();
    }
  }

 [Serializable]
  public class CustomPartitionResolver1<TValue> : IFixedPartitionResolver<int, TValue>
  {
    public CustomPartitionResolver1()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver1";
    }

    public Object GetRoutingObject(EntryEvent<int, TValue> key)
    {
      Util.Log("CustomPartitionResolver1::GetRoutingObject");
      int nkey = key.Key;

      return nkey + 5;
    }

    public static CustomPartitionResolver1<TValue> Create()
    {
      return new CustomPartitionResolver1<TValue>();
    }

    public string GetPartitionName(EntryEvent<int, TValue> entryEvent)
    {
      Util.Log("CustomPartitionResolver1::GetPartitionName");
      int newkey = entryEvent.Key % 6;
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
  }

  [Serializable]
  public class CustomPartitionResolver2<TValue> : IFixedPartitionResolver<int, TValue>
  {
    public CustomPartitionResolver2()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver2";
    }

    public Object GetRoutingObject(EntryEvent<int, TValue> key)
    {
      Util.Log("CustomPartitionResolver2::GetRoutingObject");
      return key.Key + 4;
    }

    public static CustomPartitionResolver2<TValue> Create()
    {
      return new CustomPartitionResolver2<TValue>();
    }

    public string GetPartitionName(EntryEvent<int, TValue> entryEvent)
    {
      Util.Log("CustomPartitionResolver2::GetPartitionName");
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
  }

  [Serializable]
  public class CustomPartitionResolver3<TValue> : IFixedPartitionResolver<int, TValue>
  {
    public CustomPartitionResolver3()
    {
    }

    public string GetName()
    {
      return (string)"CustomPartitionResolver3";
    }

    public Object GetRoutingObject(EntryEvent<int, TValue> key)
    {
      Util.Log("CustomPartitionResolver3::GetRoutingObject");
      return key.Key % 5;
    }

    public static CustomPartitionResolver3<TValue> Create()
    {
      return new CustomPartitionResolver3<TValue>();
    }

    public string GetPartitionName(EntryEvent<int, TValue> entryEvent)
    {
      Util.Log("CustomPartitionResolver3::GetPartitionName");
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
  }

  public class TradeKey : ICacheableKey
  {
    public int m_id;
    public int m_accountid;

    public TradeKey()
    {
    }

    public TradeKey(int id)
    {
      m_id = id;
      m_accountid = 1 + id;
    }

    public TradeKey(int id, int accId)
    {
      m_id = id;
      m_accountid = accId;
    }

    public IGFSerializable FromData(DataInput input)
    {
      m_id = input.ReadInt32();
      m_accountid = input.ReadInt32();
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_id);
      output.WriteInt32(m_accountid);
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x04;
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        objectSize += (UInt32)sizeof(Int32);
        objectSize += (UInt32)sizeof(Int32);
        return objectSize;
      }
    }

    public static IGFSerializable CreateDeserializable()
    {
      return new TradeKey();
    }

    public bool Equals(ICacheableKey other)
    {
      if (other == null)
        return false;
      TradeKey bc = other as TradeKey;
      if (bc == null)
        return false;

      if (bc == this)
        return true;

      if (bc.m_id == this.m_id)
      {
        return true;
      }
      return false;
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
  }

  [Serializable]
  public class TradeKeyResolver : IPartitionResolver<TradeKey, Object>
  {
    public TradeKeyResolver()
    {
    }

    public string GetName()
    {
      return (string)"TradeKeyResolver";
    }

    public Object GetRoutingObject(EntryEvent<TradeKey, Object> key)
    {
      Util.Log("TradeKeyResolver::GetRoutingObject");
      TradeKey tkey = (TradeKey)key.Key;
      Util.Log("TradeKeyResolver::GetRoutingObject done {0} ", tkey.m_id + 5);
      return tkey.m_id + 5;
    }

    public static TradeKeyResolver Create()
    {
      return new TradeKeyResolver();
    }
  }

  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("generics")]
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
      try
      {
        Serializable.RegisterTypeGeneric(OtherType.CreateDeserializable);
      }
      catch (IllegalStateException)
      {
        // ignored since we run multiple times for pool and non pool cases.
      }
    }

    public void DoPutsOtherTypeWithEx(OtherType.ExceptionType exType)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      for (int keyNum = 1; keyNum <= 10; ++keyNum)
      {
        try
        {
          region["key-" + keyNum] = new OtherType(keyNum, keyNum * keyNum, exType);
          if (exType != OtherType.ExceptionType.None)
          {
            Assert.Fail("Expected an exception in Put");
          }
        }
        catch (GemFireIOException ex)
        {
          if (exType == OtherType.ExceptionType.Gemfire)
          {
            // Successfully changed exception back and forth
            Util.Log("Got expected exception in Put: " + ex);
          }
          else if (exType == OtherType.ExceptionType.GemfireGemfire)
          {
            if (ex.InnerException is CacheServerException)
            {
              // Successfully changed exception back and forth
              Util.Log("Got expected exception in Put: " + ex);
            }
            else
            {
              throw;
            }
          }
          else
          {
            throw;
          }
        }
        catch (CacheServerException ex)
        {
          if (exType == OtherType.ExceptionType.GemfireSystem)
          {
            if (ex.InnerException is IOException)
            {
              // Successfully changed exception back and forth
              Util.Log("Got expected exception in Put: " + ex);
            }
            else
            {
              throw;
            }
          }
          else
          {
            throw;
          }
        }
        catch (IOException ex)
        {
          if (exType == OtherType.ExceptionType.System)
          {
            // Successfully changed exception back and forth
            Util.Log("Got expected system exception in Put: " + ex);
          }
          else
          {
            throw;
          }
        }
        catch (ApplicationException ex)
        {
          if (exType == OtherType.ExceptionType.SystemGemfire)
          {
            if (ex.InnerException is CacheServerException)
            {
              // Successfully changed exception back and forth
              Util.Log("Got expected system exception in Put: " + ex);
            }
            else
            {
              throw;
            }
          }
          else if (exType == OtherType.ExceptionType.SystemSystem)
          {
            if (ex.InnerException is IOException)
            {
              // Successfully changed exception back and forth
              Util.Log("Got expected system exception in Put: " + ex);
            }
            else
            {
              throw;
            }
          }
          else
          {
            throw;
          }
        }
      }
    }

    public void RegexInterestAllStep2() //client 2 //pxr2
    {
      Util.Log("RegexInterestAllStep2 Enters.");
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      List<object> resultKeys = new List<object>();
      //CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      //CreateEntry(m_regionNames[1], m_keys[1], m_vals[1]);
      region0.GetSubscriptionService().RegisterAllKeys(false, resultKeys, true);
      region1.GetSubscriptionService().RegisterAllKeys(false, null, true);
      if (region0.Count != 1 || region1.Count != 1)
      {
        Assert.Fail("Expected one entry in region");
      }
      if (resultKeys.Count != 1)
      {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      object value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[0])))
      {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }
      Util.Log("RegexInterestAllStep2 complete.");
    }

    public void CheckAndPutKey()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      IRegionService regServ = region0.RegionService;

      Cache cache = regServ as Cache;

      Assert.IsNotNull(cache);

      if (region0.ContainsKey("keyKey01"))
      {
        Assert.Fail("Did not expect keyKey01 to be on Server");
      }
      region0["keyKey01"] = "valueValue01";
      if (!region0.ContainsKey("keyKey01"))
      {
        Assert.Fail("Expected keyKey01 to be on Server");
      }
    }

    public void ClearRegionStep1()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region0.GetSubscriptionService().RegisterAllKeys();
      CreateEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      CreateEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      if (region0.Count != 2)
      {
        Assert.Fail("Expected region size 2");
      }
    }
    public void ClearRegionListenersStep1()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region0.GetSubscriptionService().RegisterAllKeys();
      AttributesMutator<object, object> attrMutator = region0.AttributesMutator;
      TallyListener<object, object> listener = new TallyListener<object, object>();
      attrMutator.SetCacheListener(listener);
      TallyWriter<object, object> writer = new TallyWriter<object, object>();
      attrMutator.SetCacheWriter(writer);
    }
    
    public void ClearRegionStep2()
    {
      //Console.WriteLine("IRegion<object, object> Name = {0}", m_regionNames[0]);

      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      if (region0 == null)
      {
        Console.WriteLine("Region0 is Null");
      }
      else
      {
        //Console.WriteLine("NIL:Before clear call");
        region0.Clear();
        //Console.WriteLine("NIL:After clear call");

        if (region0.Count != 0)
        {
          Assert.Fail("Expected region size 0");
        }
      }
      Thread.Sleep(20000);
    }

    public void ClearRegionListenersStep2()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      if (region0.Count != 2)
      {
        Assert.Fail("Expected region size 2");
      }
    }

    public void ClearRegionListenersStep3()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      if (region0.Count != 0)
      {
        Assert.Fail("Expected region size 0");
      }
      GemStone.GemFire.Cache.Generic.RegionAttributes<object, object> attr = region0.Attributes;
      TallyListener<object, object> listener = attr.CacheListener as TallyListener<object, object>;
      TallyWriter<object, object> writer = attr.CacheWriter as TallyWriter<object, object>;
      if (listener.Clears != 1)
      {
        Assert.Fail("Expected listener clear count 1");
      }
      if (writer.Clears != 1)
      {
        Assert.Fail("Expected writer clear count 1");
      }
      CreateEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      CreateEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      if (region0.Count != 2)
      {
        Assert.Fail("Expected region size 2");
      }
      region0.GetLocalView().Clear();
      if (listener.Clears != 2)
      {
        Assert.Fail("Expected listener clear count 2");
      }
      if (writer.Clears != 2)
      {
        Assert.Fail("Expected writer clear count 2");
      }
      if (region0.Count != 0)
      {
        Assert.Fail("Expected region size 0");
      }
    }

    public void ClearRegionStep3()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      if (region0.Count != 2)
      {
        Assert.Fail("Expected region size 2");
      }
      
      if (!region0.ContainsKey(m_keys[0]))
      {
        Assert.Fail("m_key[0] is not on Server");
      }
      if (!region0.ContainsKey(m_keys[1]))
      {
        Assert.Fail("m_key[1] is not on Server");
      }
    }

    public void GetInterests()
    {
      string[] testregex = { "Key-*1", "Key-*2", "Key-*3", "Key-*4", "Key-*5", "Key-*6" };
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region0.GetSubscriptionService().RegisterRegex(testregex[0]);
      region0.GetSubscriptionService().RegisterRegex(testregex[1]);
      ICollection<object> myCollection1 = new Collection<object>();
      myCollection1.Add((object)m_keys[0]);
      region0.GetSubscriptionService().RegisterKeys(myCollection1);

      ICollection<object> myCollection2 = new Collection<object>();
      myCollection2.Add((object)m_keys[1]);
      region0.GetSubscriptionService().RegisterKeys(myCollection2);

      ICollection<string> regvCol = region0.GetSubscriptionService().GetInterestListRegex();
      string[] regv = new string[regvCol.Count];
      regvCol.CopyTo(regv, 0);

      if (regv.Length != 2)
      {
        Assert.Fail("regex list length is not 2");
      }
      for (int i = 0; i < regv.Length; i++)
      {
        Util.Log("regv[{0}]={1}", i, regv[i]);
        bool found = false;
        for (int j = 0; j < regv.Length; j++)
        {
          if (regv[i].Equals(testregex[j]))
          {
            found = true;
            break;
          }
        }
        if (!found)
        {
          Assert.Fail("Unexpected regex");
        }
      }

      ICollection<object>  keyvCol = region0.GetSubscriptionService().GetInterestList();
      string[] keyv = new string[keyvCol.Count];
      keyvCol.CopyTo(keyv, 0);

      if (keyv.Length != 2)
      {
        Assert.Fail("interest list length is not 2");
      }
      for (int i = 0; i < keyv.Length; i++)
      {
        Util.Log("keyv[{0}]={1}", i, keyv[i].ToString());
        bool found = false;
        for (int j = 0; j < keyv.Length; j++)
        {
          if (keyv[i].ToString().Equals(m_keys[j]))
          {
            found = true;
            break;
          }
        }
        if (!found)
        {
          Assert.Fail("Unexpected key");
        }
      }
    }

    public void RegexInterestAllStep3(string locators)
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      region0.GetSubscriptionService().UnregisterAllKeys();
      region1.GetSubscriptionService().UnregisterAllKeys();
      region0.GetLocalView().DestroyRegion();
      region1.GetLocalView().DestroyRegion();
      List<object> resultKeys = new List<object>();
      CreateTCRegions_Pool(RegionNames, locators, "__TESTPOOL1_", true);
      region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      CreateEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      region0.GetSubscriptionService().RegisterRegex(".*", false, resultKeys, true);
      region1.GetSubscriptionService().RegisterRegex(".*", false, null, true);
      if (region0.Count != 1)
      {
        Assert.Fail("Expected one entry in region");
      }
      if (region1.Count != 1)
      {
        Assert.Fail("Expected one entry in region");
      }
      if (resultKeys.Count != 1)
      {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      object value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[0])))
      {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      Util.Log("RegexInterestAllStep3 complete.");
    }

    public void RegexInterestAllStep4()
    {
      List<object> resultKeys = new List<object>();
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region0.GetSubscriptionService().RegisterAllKeys(false, resultKeys, false);
      if (region0.Count != 1)
      {
        Assert.Fail("Expected one entry in region");
      }
      if (!region0.ContainsKey(m_keys[0]))
      {
        Assert.Fail("Expected region to contain the key");
      }

      if (region0.ContainsValueForKey(m_keys[0]))
      {
        Assert.Fail("Expected region to not contain the value");
      }
      if (resultKeys.Count != 1)
      {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      object value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[0])))
      {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }

      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      resultKeys.Clear();
      region1.GetSubscriptionService().RegisterRegex(".*", false, resultKeys, false);

      if (region1.Count != 1)
      {
        Assert.Fail("Expected one entry in region");
      }

      if (!region1.ContainsKey(m_keys[2]))
      {
        Assert.Fail("Expected region to contain the key");
      }

      if (region1.ContainsValueForKey(m_keys[2]))
      {
        Assert.Fail("Expected region to not contain the value");
      }
      if (resultKeys.Count != 1)
      {
        Assert.Fail("Expected one key from registerAllKeys");
      }
      value = resultKeys[0];
      if (!(value.ToString().Equals(m_keys[2])))
      {
        Assert.Fail("Unexpected key from RegisterAllKeys");
      }
      CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], false);
      CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public void RegexInterestAllStep5()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      if (region1.Count != 2 || region0.Count != 2)
      {
        Assert.Fail("Expected two entry in region");
      }
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);

    }

    public void RegexInterestAllStep6()
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

    public void RegexInterestAllStep8()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      region0.GetSubscriptionService().UnregisterAllKeys();
      region1.GetSubscriptionService().UnregisterAllKeys();
    }

    public void RegexInterestAllStep9() 
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], false);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1], false);
      UpdateEntry(m_regionNames[1], m_keys[2], m_vals[2], false);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3], false);
    }

    public void RegexInterestAllStep10() 
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    //public void GetAll(string endpoints, bool pool, bool locator)
    //{
    //  Util.Log("Enters GetAll");
    //  IRegion<object, object> region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
    //  List<ICacheableKey> resultKeys = new List<ICacheableKey>();
    //  CacheableKey key0 = m_keys[0];
    //  CacheableKey key1 = m_keys[1];
    //  resultKeys.Add(key0);
    //  resultKeys.Add(key1);
    //  region0.LocalDestroyRegion();
    //  region0 = null;
    //  if (pool) {
    //    if (locator) {
    //      region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, false, null, null, endpoints, "__TESTPOOL1_", true); //caching enable false
    //    }
    //    else {
    //      region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, false, null, endpoints, null, "__TESTPOOL1_", true); //caching enable false
    //    }
    //  }
    //  else {
    //    region0 = CacheHelper.CreateTCRegion(RegionNames[0], true, false, null, endpoints, true); //caching enable false
    //  }
    //  try {
    //    region0.GetAll(resultKeys.ToArray(), null, null, true);
    //    Assert.Fail("Expected IllegalArgumentException");
    //  }
    //  catch (IllegalArgumentException ex) {
    //    Util.Log("Got expected IllegalArgumentException" + ex.Message);
    //  }
    //  //recreate region with caching enabled true
    //  region0.LocalDestroyRegion();
    //  region0 = null;
    //  if (pool) {
    //    if (locator) {
    //      region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, true, null, null, endpoints, "__TESTPOOL1_", true); //caching enable true
    //    }
    //    else {
    //      region0 = CacheHelper.CreateTCRegion_Pool(RegionNames[0], true, true, null, endpoints, null, "__TESTPOOL1_", true); //caching enable true
    //    }
    //  }
    //  else {
    //    region0 = CacheHelper.CreateTCRegion(RegionNames[0], true, true, null, endpoints, true); //caching enable true
    //  }
    //  Dictionary<ICacheableKey, IGFSerializable> values = new Dictionary<ICacheableKey, IGFSerializable>();
    //  Dictionary<ICacheableKey, Exception> exceptions = new Dictionary<ICacheableKey, Exception>();
    //  resultKeys.Clear();
    //  try {
    //    region0.GetAll(resultKeys.ToArray(), values, exceptions);
    //    Assert.Fail("Expected IllegalArgumentException");
    //  }
    //  catch (IllegalArgumentException ex) {
    //    Util.Log("Got expected IllegalArgumentException" + ex.Message);
    //  }
    //  resultKeys.Add(key0);
    //  resultKeys.Add(key1);
    //  try {
    //    region0.GetAll(resultKeys.ToArray(), null, null, false);
    //    Assert.Fail("Expected IllegalArgumentException");
    //  }
    //  catch (IllegalArgumentException ex) {
    //    Util.Log("Got expected IllegalArgumentException" + ex.Message);
    //  }
    //  region0.GetAll(resultKeys.ToArray(), values, exceptions);
    //  if (values.Count != 2) {
    //    Assert.Fail("Expected 2 values");
    //  }
    //  if (exceptions.Count != 0) {
    //    Assert.Fail("No exception expected");
    //  }
    //  try {
    //    CacheableString val0 = (CacheableString)values[(resultKeys[0])];
    //    CacheableString val1 = (CacheableString)values[(resultKeys[1])];
    //    if (!(val0.ToString().Equals(m_nvals[0])) || !(val1.ToString().Equals(m_nvals[1]))) {
    //      Assert.Fail("Got unexpected value");
    //    }
    //  }
    //  catch (Exception ex) {
    //    Assert.Fail("Key should have been found" + ex.Message);
    //  }
    //  IRegion<object, object> region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
    //  CacheableKey key2 = m_keys[2];
    //  CacheableKey key3 = m_keys[3];
    //  region1.LocalInvalidate(key2);
    //  resultKeys.Clear();
    //  resultKeys.Add(key2);
    //  resultKeys.Add(key3);
    //  values.Clear();
    //  exceptions.Clear();
    //  region1.GetAll(resultKeys.ToArray(), values, exceptions, true);
    //  if (values.Count != 2) {
    //    Assert.Fail("Expected 2 values");
    //  }
    //  if (exceptions.Count != 0) {
    //    Assert.Fail("Expected no exception");
    //  }
    //  try {
    //    CacheableString val2 = (CacheableString)values[(resultKeys[0])];
    //    CacheableString val3 = (CacheableString)values[(resultKeys[1])];
    //    if (!(val2.ToString().Equals(m_nvals[2])) || !(val3.ToString().Equals(m_vals[3]))) {
    //      Assert.Fail("Got unexpected value");
    //    }
    //  }
    //  catch (Exception ex) {
    //    Assert.Fail("Key should have been found" + ex.Message);
    //  }
    //  if (region1.Size != 2) {
    //    Assert.Fail("Expected 2 entry in the region");
    //  }
    //  RegionEntry[] regionEntry = region1.GetEntries(false);//Not of subregions
    //  if (regionEntry.Length != 2) {
    //    Assert.Fail("Should have two values in the region");
    //  }
    //  VerifyEntry(RegionNames[1], m_keys[2], m_nvals[2], true);
    //  VerifyEntry(RegionNames[1], m_keys[3], m_vals[3], true);
    //  region1.LocalInvalidate(key3);
    //  values = null;
    //  exceptions.Clear();
    //  region1.GetAll(resultKeys.ToArray(), values, exceptions, true);
    //  if (region1.Size != 2) {
    //    Assert.Fail("Expected 2 entry in the region");
    //  }
    //  regionEntry = region1.GetEntries(false);
    //  if (regionEntry.Length != 2) {
    //    Assert.Fail("Should have two values in the region");
    //  }
    //  VerifyEntry(RegionNames[1], m_keys[2], m_nvals[2], true);
    //  VerifyEntry(RegionNames[1], m_keys[3], m_nvals[3], true);
    //  Util.Log("Exits GetAll");
    //}

    //private void UpdateEntry(string p, string p_2, string p_3)
    //{
    //  throw new Exception("The method or operation is not implemented.");
    //}

    public void PutAllStep3()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      List<object> resultKeys = new List<object>();
      object key0 = m_keys[0];
      object key1 = m_keys[1];
      resultKeys.Add(key0);
      resultKeys.Add(key1);
      region0.GetSubscriptionService().RegisterKeys(resultKeys.ToArray());
      Util.Log("Step three completes");
    }

    public void PutAllStep4() 
    {
      Dictionary<object, object> map0 = new Dictionary<object, object>();
      Dictionary<object, object> map1 = new Dictionary<object, object>();
      object key0 = m_keys[0];
      object key1 = m_keys[1];
      string val0 = m_vals[0];
      string val1 = m_vals[1];
      map0.Add(key0, val0);
      map0.Add(key1, val1);

      object key2 = m_keys[2];
      object key3 = m_keys[3];
      string val2 = m_vals[2];
      string val3 = m_vals[3];
      map1.Add(key2, val2);
      map1.Add(key3, val3);
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      region0.PutAll(map0);
      region1.PutAll(map1);
      Util.Log("Put All Complets");
    }

    public void PutAllStep5() 
    {
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[0], m_keys[1]);

      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);

      DoNetsearch(m_regionNames[1], m_keys[2], m_vals[2], true);
      DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], true);

      Util.Log("StepFive complete.");
    }

    public void PutAllStep6() 
    {
      Dictionary<object, object> map0 = new Dictionary<object, object>();
      Dictionary<object, object> map1 = new Dictionary<object, object>();
      
      object key0 = m_keys[0];
      object key1 = m_keys[1];
      string val0 = m_nvals[0];
      string val1 = m_nvals[1];
      map0.Add(key0, val0);
      map0.Add(key1, val1);

      object key2 = m_keys[2];
      object key3 = m_keys[3];
      string val2 = m_nvals[2];
      string val3 = m_nvals[3];
      map1.Add(key2, val2);
      map1.Add(key3, val3);
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
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

    public virtual void RemoveAllStep1()
    {
      CreateEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      CreateEntry(m_regionNames[0], m_keys[2], m_vals[2]);

      CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
      CreateEntry(m_regionNames[1], m_keys[4], m_vals[4]);
      CreateEntry(m_regionNames[1], m_keys[5], m_vals[5]);
      Util.Log("RemoveAllStep1 complete.");
    }

    public virtual void RemoveAllStep2()
    {
      IRegion<object, object> reg0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> reg1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      ICollection<object> keys0 = new List<object>();
      ICollection<object> keys1 = new List<object>();
      for (int i = 0; i < 3; i++)
      {
        keys0.Add(m_keys[i]);
        keys1.Add(m_keys[i + 3]);
      }

      //try remove all
      reg0.RemoveAll(keys0);
      reg1.RemoveAll(keys1);
      Util.Log("RemoveAllStep2 complete.");
    }

    public virtual void RemoveAllStep3()
    {
      VerifyDestroyed(m_regionNames[0], m_keys[0]);
      VerifyDestroyed(m_regionNames[0], m_keys[1]);
      VerifyDestroyed(m_regionNames[0], m_keys[2]);

      VerifyDestroyed(m_regionNames[1], m_keys[3]);
      VerifyDestroyed(m_regionNames[1], m_keys[4]);
      VerifyDestroyed(m_regionNames[1], m_keys[5]);
      
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      Assert.AreEqual(region0.Count, 0, "Remove all should remove the entries specified");
      Assert.AreEqual(region1.Count, 0, "Remove all should remove the entries specified");
      Util.Log("RemoveAllStep3 complete.");
    }

    public virtual void RemoveAllSingleHopStep()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      ICollection<object> keys = new Collection<object>();
      for (int y = 0; y < 1000; y++)
      {
        Util.Log("put:{0}", y);
        region0[y] = y;
        keys.Add(y);
      }
      region0.RemoveAll(keys);
      Assert.AreEqual(0, region0.Count);
      Util.Log("RemoveAllSingleHopStep completed");
    }

    void runDistOps()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateNonExistentRegion, CacheHelper.Locators);
      m_client1.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runDistOps2()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml", "cacheserver3.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");
      CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
      Util.Log("Cacheserver 3 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFive, true);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSix, true);
      Util.Log("StepSix complete.");
      //m_client1.Call(GetAll, pool, locator);

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCheckPutGet()
    {
      CacheHelper.SetupJavaServers(true, "cacheServer_pdxreadserialized.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      PutGetTests putGetTest = new PutGetTests();

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 1 (pool locators) regions created");
      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 2 (pool locators) regions created");

      m_client1.Call(putGetTest.SetRegion, RegionNames[0]);
      m_client2.Call(putGetTest.SetRegion, RegionNames[0]);

      long dtTicks = DateTime.Now.Ticks;
      CacheableHelper.RegisterBuiltins(dtTicks);

      putGetTest.TestAllKeyValuePairs(m_client1, m_client2,
        RegionNames[0], true, dtTicks);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCheckPutGetWithAppDomain()
    {
      CacheHelper.SetupJavaServers(true, "cacheServer_pdxreadserialized.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      PutGetTests putGetTest = new PutGetTests();

      m_client1.Call(InitializeAppDomain);
      long dtTime = DateTime.Now.Ticks;
      m_client1.Call(CreateTCRegions_Pool_AD, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, false, dtTime);
      Util.Log("Client 1 (pool locators) regions created");

      m_client1.Call(SetRegionAD, RegionNames[0]);
      //m_client2.Call(putGetTest.SetRegion, RegionNames[0]);

      // CacheableHelper.RegisterBuiltins();

      //putGetTest.TestAllKeyValuePairs(m_client1, m_client2,
      //RegionNames[0], true, pool);
      m_client1.Call(TestAllKeyValuePairsAD, RegionNames[0], true, dtTime);
      //m_client1.Call(CloseCacheAD);
      Util.Log("Client 1 closed");

      CacheHelper.CloseCache();

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void putGetTest()
    { 
      
    }

    void runPdxAppDomainTest(bool caching, bool readPdxSerialized)
    {
      CacheHelper.SetupJavaServers(true, "cacheServer_pdxreadserialized.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

     
      m_client1.Call(InitializeAppDomain);

      m_client1.Call(CreateTCRegions_Pool_AD2, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, false, caching, readPdxSerialized);
      Util.Log("Client 1 (pool locators) regions created");

      m_client1.Call(SetRegionAD, RegionNames[0]);

      m_client1.Call(pdxPutGetTest, caching, readPdxSerialized);
      m_client1.Call(pdxGetPutTest, caching, readPdxSerialized);
      //m_client2.Call(putGetTest.SetRegion, RegionNames[0]);

      // CacheableHelper.RegisterBuiltins();

      //putGetTest.TestAllKeyValuePairs(m_client1, m_client2,
      //RegionNames[0], true, pool);
     // m_client1.Call(TestAllKeyValuePairsAD, RegionNames[0], true, pool);
      m_client1.Call(CloseCacheAD);

      Util.Log("Client 1 closed");

      //CacheHelper.CloseCache();

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runPartitionResolver()
    {
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

      PutGetTests putGetTest = new PutGetTests();
      // Create and Add partition resolver to the regions.
      //CustomPartitionResolver<object> cpr = CustomPartitionResolver<object>.Create();

      m_client1.Call(CreateTCRegions_Pool2_WithPartitionResolver, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, true);
      Util.Log("Client 1 (pool locators) regions created");
      m_client2.Call(CreateTCRegions_Pool2_WithPartitionResolver, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false, true);
      Util.Log("Client 2 (pool locators) regions created");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runTradeKeyResolver()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_TradeKey.xml",
          "cacheserver2_TradeKey.xml", "cacheserver3_TradeKey.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");
      CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
      Util.Log("Cacheserver 3 started.");

      m_client1.Call(CreateTCRegion2, TradeKeyRegion, true, true, TradeKeyResolver.Create(),
        CacheHelper.Locators, true);
      Util.Log("Client 1 (pool locators) region created");          

      PutGetTests putGetTest = new PutGetTests();
      m_client1.Call(putGetTest.DoPRSHTradeResolverTasks, TradeKeyRegion);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFixedPartitionResolver()
    {
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

      PutGetTests putGetTest = new PutGetTests();

      m_client1.Call(CreateTCRegions_Pool1, PartitionRegion1,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 1 (pool locators) PartitionRegion1 created");

      m_client1.Call(CreateTCRegions_Pool1, PartitionRegion2,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 1 (pool locators) PartitionRegion2 created");

      m_client1.Call(CreateTCRegions_Pool1, PartitionRegion3,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 1 (pool locators) PartitionRegion3 created");

      m_client1.Call(putGetTest.SetRegion, PartitionRegion1);
      putGetTest.DoPRSHFixedPartitionResolverTasks(m_client1, PartitionRegion1);

      m_client1.Call(putGetTest.SetRegion, PartitionRegion2);
      putGetTest.DoPRSHFixedPartitionResolverTasks(m_client1, PartitionRegion2);

      m_client1.Call(putGetTest.SetRegion, PartitionRegion3);
      putGetTest.DoPRSHFixedPartitionResolverTasks(m_client1, PartitionRegion3);

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }    

    void runCheckPut()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_hashcode.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      PutGetTests putGetTest = new PutGetTests();

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 1 (pool locators) regions created");
      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 2 (pool locators) regions created");

      m_client1.Call(putGetTest.SetRegion, RegionNames[0]);
      m_client2.Call(putGetTest.SetRegion, RegionNames[0]);
      long dtTime = DateTime.Now.Ticks;
      CacheableHelper.RegisterBuiltinsJavaHashCode(dtTime);
      putGetTest.TestAllKeys(m_client1, m_client2, RegionNames[0], dtTime);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runCheckNativeException()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(RegisterOtherType);

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailover()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runNotification()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo (pool locators) complete.");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailover2()
    {
      // This test is for client failover with client notification.
      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(RegisterAllKeysR0WithoutValues);
      m_client1.Call(RegisterAllKeysR1WithoutValues);

      m_client2.Call(RegisterAllKeysR0WithoutValues);
      m_client2.Call(RegisterAllKeysR1WithoutValues);

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailover3()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver.xml", "cacheserver2.xml", "cacheserver3.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      Util.Log("StepFour complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");
      CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
      Util.Log("Cacheserver 3 started.");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailoverInterestAll()
    {
      runFailoverInterestAll(false);
    }
    
    void runFailoverInterestAll(bool ssl)
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml",
        "cacheserver_notify_subscription2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC", null, ssl);
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, ssl);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", true, ssl);

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", true, ssl);

      Util.Log("CreateTCRegions complete.");

      m_client2.Call(RegexInterestAllStep2);
      Util.Log("RegexInterestAllStep2 complete.");

      m_client2.Call(RegexInterestAllStep3, CacheHelper.Locators);
      Util.Log("RegexInterestAllStep3 complete.");

      m_client1.Call(RegexInterestAllStep4);
      Util.Log("RegexInterestAllStep4 complete.");

      m_client2.Call(RegexInterestAllStep5);
      Util.Log("RegexInterestAllStep5 complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, ssl);

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

      CacheHelper.StopJavaLocator(1, true, ssl);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runPutAll()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml",
        "cacheserver_notify_subscription2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);  //Client Notification true for client 1
      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);  //Cleint notification true for client 2

      Util.Log("IRegion<object, object> creation complete.");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRemoveAll()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

      m_client1.Call(RemoveAllStep1);
      Util.Log("RemoveAllStep1 complete.");

      m_client1.Call(RemoveAllStep2);
      Util.Log("RemoveAllStep2 complete.");

      m_client2.Call(RemoveAllStep3);
      Util.Log("RemoveAllStep3 complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRemoveAllWithSingleHop()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_pr.xml",
        "cacheserver2_pr.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("Client 1 (pool locators) regions created");
      Util.Log("Region creation complete.");

      m_client1.Call(RemoveAllSingleHopStep);
      Util.Log("RemoveAllSingleHopStep complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRemoveOps()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
          CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

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

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRemoveOps1()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_expiry.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames2,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames2,
        CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepTwo (pool locators) complete.");

      m_client2.Call(RemoveStepEight);
      Util.Log("RemoveStepEight complete.");

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runIdictionaryOps()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator 1 started.");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames2,
          CacheHelper.Locators, "__TESTPOOL1_", false);
      Util.Log("StepOne (pool locators) complete.");
       
      m_client1.Call(IdictionaryRegionOperations, "DistRegionAck");
      Util.Log("IdictionaryRegionOperations complete.");

      m_client1.Call(IdictionaryRegionNullKeyOperations, "DistRegionAck");
      Util.Log("IdictionaryRegionNullKeyOperations complete."); 

      m_client1.Call(IdictionaryRegionArrayOperations, "DistRegionAck");
      Util.Log("IdictionaryRegionArrayOperations complete.");     

      m_client1.Call(Close);
      Util.Log("Client 1 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void EmptyByteArrayTest()
    {
      IRegion<int, byte[]> region = CacheHelper.GetVerifyRegion<int, byte[]>(RegionNames3[0]);
      
      IRegionService regServ = region.RegionService;

      Cache cache = regServ as Cache;

      Assert.IsNotNull(cache);

      region[0] = new byte[0];
      Util.Log("Put empty byteArray in region");
      Assert.AreEqual(0, region[0].Length);

      region[1] = new byte[2];
      Util.Log("Put non empty byteArray in region");

      Assert.AreEqual(2, region[1].Length);
      
      region[2] = System.Text.Encoding.ASCII.GetBytes("TestString");
      Util.Log("Put string in region");

      Assert.AreEqual(0, "TestString".CompareTo(System.Text.Encoding.ASCII.GetString(region[2]).ToString()));
      Util.Log("EmptyByteArrayTest completed successfully");
    }

    #region Tests

    [Test]
    public void PutEmptyByteArrayTest()
    {
      CacheHelper.SetupJavaServers("cacheserver.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");
      m_client1.Call(CreateTCRegions_Pool, RegionNames3,
          (string)null, "__TESTPOOL1_", true);
      Util.Log("StepOne of region creation complete.");

      m_client1.Call(EmptyByteArrayTest);
      Util.Log("EmptyByteArrayTest completed.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

    }


    [Test]
    public void CheckKeyOnServer()
    {
      CacheHelper.SetupJavaServers("cacheserver.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
          (string)null, "__TESTPOOL1_", true);
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

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
          (string)null, "__TESTPOOL1_", true);
      Util.Log("client 1 StepOne of region creation complete.");
      m_client2.Call(CreateTCRegions_Pool, RegionNames,
          (string)null, "__TESTPOOL1_", true);
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

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
              (string)null, "__TESTPOOL1_", true);


      Util.Log("StepOne of region creation complete.");

      m_client1.Call(GetInterests);
      Util.Log("StepTwo of check for GetInterestsOnClient complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    [Test]
    public void DistOps()
    {
      runDistOps();
    }

    [Test]
    public void DistOps2()
    {
      runDistOps2();
    }

    [Test]
    public void Notification()
    {
      runNotification();
    }

    [Test]
    public void CheckPutGet()
    {
      runCheckPutGet();
    }

    [Test]
    public void CheckPutGetWithAppDomain()
    {
      runCheckPutGetWithAppDomain();
    }

    [Test]
    public void PdxAppDomainTest()
    {
      runPdxAppDomainTest(false, true); // pool with locators
      runPdxAppDomainTest(true, true); // pool with locators
      runPdxAppDomainTest(false, false); // pool with locators
      runPdxAppDomainTest(true, false); // pool with locators
    }

    [Test]
    public void JavaHashCode()
    {
      runCheckPut();
    }

    [Test]
    public void CheckPartitionResolver()
    {
      runPartitionResolver();
    }

    [Test]
    public void TradeKeyPartitionResolver()
    {
      runTradeKeyResolver();
    }

    [Test]
    public void CheckFixedPartitionResolver()
    {
      runFixedPartitionResolver();
    }

    [Test]
    public void CheckNativeException()
    {
      runCheckNativeException();
    }

    [Test]
    public void Failover()
    {
      runFailover();
    }

    [Test]
    public void Failover2()
    {
      runFailover2();
    }

    [Test]
    public void Failover3()
    {
      runFailover3();
    }

    [Test]
    public void FailOverInterestAll()
    {
      runFailoverInterestAll();
    }

    [Test]
    public void FailOverInterestAllWithSSL()
    {
      runFailoverInterestAll(true);
    }

    [Test]
    public void PutAll()
    {
      runPutAll();
    }

    [Test]
    public void RemoveAll()
    {
      runRemoveAll();
    }

    [Test]
    public void RemoveAllWithSingleHop()
    {
      runRemoveAllWithSingleHop();
    }


    [Test]
    public void RemoveOps()
    {
      runRemoveOps();
    }

    [Test]
    public void RemoveOps1()
    {
      runRemoveOps1();
    }

    [Test]
    public void IdictionaryOps()
    {
      runIdictionaryOps();
    }

    #endregion
  }
}

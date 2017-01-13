//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;
using System.Reflection;
using System.Collections;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  public abstract class DistOpsSteps : UnitTests
  {
    #region Protected utility functions used by the tests

    protected virtual void VerifyAll(string regionName, string key,
      string val, bool noKey)
    {
      VerifyAll(regionName, key, val, noKey, false, true);
    }

    protected virtual void VerifyAll(string regionName, string key,
      string val, bool noKey, bool isCreated, bool checkVal)
    {
      // Verify key and value exist in this region, in this process.
      string logStr = null;
      if (!isCreated)
      {
        if (noKey)
        {
          logStr = string.Format(
            "Verify key {0} does not exist in region {1}", key, regionName);
        }
        else if (val == null)
        {
          logStr = string.Format(
            "Verify value for key {0} does not exist in region {1}",
            key, regionName);
        }
        else
        {
          logStr = string.Format(
            "Verify value for key {0} is: {1} in region {2}",
            key, val, regionName);
        }
        Util.Log(logStr);
      }

      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      //CacheableString cKey = new CacheableString(key);
      string cKey = key;
      //Util.Log("REGION NAME = {0}  cKey = {1}  ", regionName, cKey);
      Thread.Sleep(100); // give distribution threads a chance to run

      // if the region is no ack, then we may need to wait...
      if (!isCreated)
      {
        if (!noKey)
        { // need to find the key!
          Assert.IsTrue(region.GetLocalView().ContainsKey(cKey), "Key not found in region.");
        }
        if (val != null && checkVal)
        { // need to have a value!
          Assert.IsTrue(region.GetLocalView().ContainsValueForKey(cKey), "Value not found in region.");
        }
      }

      // loop up to maxLoop times, testing condition
      int maxLoop = 1000;
      int sleepMillis = 50; // milliseconds
      int containsKeyCnt = 0;
      int containsValueCnt = 0;
      int testValueCnt = 0;

      for (int i = 1; i <= maxLoop; i++)
      {
        if (isCreated)
        {
          if (!region.GetLocalView().ContainsKey(cKey))
            containsKeyCnt++;
          else
            break;
          Assert.Less(containsKeyCnt, maxLoop, "Key has not been created in region.");
        }
        else
        {
          if (noKey)
          {
            if (region.GetLocalView().ContainsKey(cKey))
              containsKeyCnt++;
            else
              break;
            Assert.Less(containsKeyCnt, maxLoop, "Key found in region.");
          }
          if (val == null)
          {
            if (region.GetLocalView().ContainsValueForKey(cKey))
              containsValueCnt++;
            else
              break;
            Assert.Less(containsValueCnt, maxLoop, "Value found in region.");
          }
          else
          {
            string cVal = region.Get(cKey, true).ToString();
            Assert.IsNotNull(cVal, "Value should not be null.");
            if (cVal != val)
              testValueCnt++;
            else
              break;
            Assert.Less(testValueCnt, maxLoop,
              "Incorrect value found. Expected: '{0}' ; Got: '{1}'",
              val, cVal);
          }
        }
        Thread.Sleep(sleepMillis);
      }
    }

    protected virtual void VerifyInvalid(string regionName, string key)
    {
      VerifyAll(regionName, key, null, false);
    }

    protected virtual void VerifyDestroyed(string regionName, string key)
    {
      VerifyAll(regionName, key, null, true);
    }

    protected virtual void VerifyEntry(string regionName, string key,
      string val)
    {
      VerifyAll(regionName, key, val, false, false, true);
    }

    protected virtual void VerifyEntry(string regionName, string key,
      string val, bool checkVal)
    {
      VerifyAll(regionName, key, val, false, false, checkVal);
    }

    protected virtual void VerifyCreated(string regionName, string key)
    {
      VerifyAll(regionName, key, null, false, true, false);
    }

    protected virtual object GetEntry(string regionName, string key)
    {
      object val = CacheHelper.GetVerifyRegion<string, string>(regionName).Get(key, null);
      if (val != null)
      {
        Util.Log(
          "GetEntry returned value: ({0}) for key: ({1}) in region: ({2})",
          val, key, regionName);
      }
      else
      {
        Util.Log("GetEntry return NULL value for key: ({0}) in region: ({1})",
          key, regionName);
      }
      return val;
    }

    protected virtual void CreateEntry(string regionName, string key, string val)
    {
      Util.Log("Creating entry -- key: {0}  value: {1} in region {2}",
        key, val, regionName);

      // Create entry, verify entry is correct
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      if (region.GetLocalView().ContainsKey(key))
        region[key] = val;
      else
        region.Add(key, val);/*Create replaced with new API Add() */
      VerifyEntry(regionName, key, val);
    }

    protected virtual void CreateEntryWithLocatorException(string regionName, string key, string val)
    {
      bool foundException = false;
      try
      {
        CreateEntry(regionName, key, val);
      }
      catch (GemStone.GemFire.Cache.Generic.NotConnectedException ex)
      {
        if (ex.InnerException is NoAvailableLocatorsException)
        {
          Util.Log("Got expected {0}: {1}", ex.GetType().Name, ex.Message);
          foundException = true;
        }
      }
      if (!foundException)
      {
        Assert.Fail("Expected NotConnectedException with inner exception NoAvailableLocatorsException");
      }
    }

    protected virtual void UpdateEntry(string regionName, string key,
      string val, bool checkVal)
    {
      Util.Log("Updating entry -- key: {0}  value: {1} in region {2}",
        key, val, regionName);

      // Update entry, verify entry is correct
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      Assert.IsTrue(region.GetLocalView().ContainsKey(key), "Key should have been found in region.");
      if (checkVal)
      {
        Assert.IsTrue(region.ContainsValueForKey(key),
          "Value should have been found in region.");
      }
      region[key] = val;
      VerifyEntry(regionName, key, val, checkVal);
    }

    protected virtual void DoNetsearch(string regionName, string key,
      string val, bool checkNoKey)
    {
      Util.Log("Netsearching for entry -- key: {0}  " +
        "expecting value: {1} in region {2}", key, val, regionName);

      // Get entry created in Process A, verify entry is correct
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      if (checkNoKey)
      {
        Assert.IsFalse(region.GetLocalView().ContainsKey(key),
          "Key should not have been found in region.");
      }
      string checkVal = (string)region[key];
      Util.Log("Got value: {0} for key {1}, expecting {2}", checkVal, key, val);
      VerifyEntry(regionName, key, val);
    }

    protected virtual void DoCacheLoad(string regionName, string key,
      string val, bool checkNoKey)
    {
      Util.Log("Cache load for entry -- key: {0}  " +
        "expecting value: {1} in region {2}", key, val, regionName);

      // Get entry created in Process A, verify entry is correct
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      if (checkNoKey)
      {
        Assert.IsFalse(region.GetLocalView().ContainsKey(key),
          "Key should not have been found in region.");
      }
      string checkVal = null;
      try
      {
        checkVal = (string)region[key];
      }
      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException)
      {
        // expected?
        //checkVal = (string)region[key];
      }
      Util.Log("Got value: {0} for key {1}, expecting {2}", checkVal, key, val);
      //VerifyEntry(regionName, key, val);
    }

    protected virtual void InvalidateEntry(string regionName, string key)
    {
      Util.Log("Invalidating entry -- key: {0}  in region {1}",
        key, regionName);

      // Invalidate entry, verify entry is invalidated
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      Assert.IsTrue(region.GetLocalView().ContainsKey(key), "Key should have been found in region.");
      Assert.IsTrue(region.GetLocalView().ContainsValueForKey(key), "Value should have been found in region.");
      region.GetLocalView().Invalidate(key);
      VerifyInvalid(regionName, key);
    }

    protected virtual void DestroyEntry(string regionName, string key)
    {
      Util.Log("Destroying entry -- key: {0}  in region {1}",
        key, regionName);

      // Destroy entry, verify entry is destroyed
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      Assert.IsTrue(region.GetLocalView().ContainsKey(key), "Key should have been found in region.");
      region.Remove(key); //Destroy() replaced by new API Remove() 
      VerifyDestroyed(regionName, key);
    }

    #endregion

    #region Protected members

    protected string[] m_keys = { "Key-1", "Key-2", "Key-3", "Key-4",
      "Key-5", "Key-6" };
    protected string[] m_vals = { "Value-1", "Value-2", "Value-3", "Value-4",
      "Value-5", "Value-6" };
    protected string[] m_nvals = { "New Value-1", "New Value-2", "New Value-3",
      "New Value-4", "New Value-5" };

    protected string[] m_regionNames;

    #endregion

    #region Various steps for DistOps

    public virtual void CreateRegions(string[] regionNames)
    {
      CacheHelper.CreateILRegion<object, object>(regionNames[0], true, true, null);
      CacheHelper.CreateILRegion<object, object>(regionNames[1], false, true, null);
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions(string[] regionNames, string locators, bool clientNotification)
    {
      CacheHelper.CreateTCRegion_Pool<string, string>(regionNames[0], true, true,
        null, locators, "__TESTPOOL__", clientNotification);
      CacheHelper.CreateTCRegion_Pool<string, string>(regionNames[1], false, true,
        null, locators, "__TESTPOOL__", clientNotification);
      m_regionNames = regionNames;
    }
    public virtual void CreateTCRegions_Pool(string[] regionNames,
      string locators, string poolName, bool clientNotification)
    {
      CreateTCRegions_Pool(regionNames, locators, poolName, clientNotification, false);
    }

    public virtual void SetHeaplimit(int maxheaplimit, int delta)
    {
      CacheHelper.SetHeapLimit(maxheaplimit, delta);
    }

    public virtual void UnsetHeapLimit()
    {
      CacheHelper.UnsetHeapLimit();
    }

    public virtual void CreateTCRegions_Pool(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, true,
        null, locators, poolName, clientNotification, ssl, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, true,
        null, locators, poolName, clientNotification, ssl, false);
      m_regionNames = regionNames;
    }

      //CreateTCRegions_Pool_PDXWithLL
    public virtual void CreateTCRegions_Pool_PDXWithLL(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool cachingEnable)
    {
        CacheHelper.PdxReadSerialized = true;
        CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, cachingEnable,
          null, locators, poolName, clientNotification, ssl, false);
        CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, cachingEnable,
          null, locators, poolName, clientNotification, ssl, false);
        //Util.Log("CreateTCRegions_Pool_PDXWithLL PdxReadSerialized = " + CacheHelper.DCache.GetPdxReadSerialized());
        Assert.IsTrue(CacheHelper.DCache.GetPdxReadSerialized());
        m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool cachingEnable)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, cachingEnable,
        null, locators, poolName, clientNotification, ssl, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, cachingEnable,
        null, locators, poolName, clientNotification, ssl, false);
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool_PDX(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool cachingEnable)
    {
      CacheHelper.PdxIgnoreUnreadFields = true;
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, cachingEnable,
        null, locators, poolName, clientNotification, ssl, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, cachingEnable,
        null, locators, poolName, clientNotification, ssl, false);
      Assert.IsTrue(CacheHelper.DCache.GetPdxIgnoreUnreadFields());
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool_PDX2(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool cachingEnable)
    {
      CacheHelper.PdxReadSerialized = true;
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[0], true, cachingEnable,
        null, locators, poolName, clientNotification, ssl, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(regionNames[1], false, cachingEnable,
        null, locators, poolName, clientNotification, ssl, false);
      Assert.IsTrue(CacheHelper.DCache.GetPdxReadSerialized());
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool2_WithPartitionResolver(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool pr)
    {
      CreateTCRegions_Pool2<object, object>(regionNames, locators, poolName, clientNotification, false, pr);
    }

    public virtual void CreateTCRegions_Pool2<TKey, TVal>(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool pr)
    {
      CreateTCRegions_Pool2<TKey, TVal>(regionNames, locators, poolName, clientNotification, false, pr);
    }

    public virtual void CreateTCRegions_Pool2<TKey, TVal>(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool pr)
    {
      CacheHelper.CreateTCRegion_Pool2<TKey, TVal>(regionNames[0], true, true,
        null, locators, poolName, clientNotification, ssl, false, pr);
      CacheHelper.CreateTCRegion_Pool2<TKey, TVal>(regionNames[1], false, true,
        null, locators, poolName, clientNotification, ssl, false, pr);
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool1(string regionNames,
      string locators, string poolName, bool clientNotification)
    {
      CreateTCRegions_Pool1(regionNames, locators, poolName, clientNotification, false);
    }

    public virtual void CreateTCRegions_Pool1(string regionNames,
      string locators, string poolName, bool clientNotification, bool ssl)
    {
      if (regionNames.Equals("R1"))
      {
        Util.Log("R1 P1 added");
        CustomPartitionResolver1<object> pr = CustomPartitionResolver1<object>.Create();
        CacheHelper.CreateTCRegion_Pool1<object, object>(regionNames, true, true,
        null, locators, poolName, clientNotification, ssl, false, pr);
      }
      else if (regionNames.Equals("R2"))
      {
        Util.Log("R2 P2 added");
        CustomPartitionResolver2<object> pr = CustomPartitionResolver2<object>.Create();
        CacheHelper.CreateTCRegion_Pool1<object, object>(regionNames, true, true,
        null, locators, poolName, clientNotification, ssl, false, pr);
      }
      else
      {
        Util.Log("R3 P3 added");
        CustomPartitionResolver3<object> pr = CustomPartitionResolver3<object>.Create();
        CacheHelper.CreateTCRegion_Pool1<object, object>(regionNames, true, true,
        null, locators, poolName, clientNotification, ssl, false, pr);
      }
    }

    public virtual void CreateTCRegion2(string name, bool ack, bool caching,
      IPartitionResolver<TradeKey, Object> resolver, string locators, bool clientNotification)
    {
      CacheHelper.CreateTCRegion2<TradeKey, Object>(name, ack, caching, resolver,
           (string)locators, clientNotification);
    }

    private AppDomain m_firstAppDomain;
    private AppDomain m_secondAppDomain;

    private CacheHelperWrapper m_chw_forFirstAppDomain;
    private CacheHelperWrapper m_chw_forSecondAppDomain;

    private PutGetTestsAD m_putGetTests_forFirstAppDomain;
    private PutGetTestsAD m_putGetTests_forSecondAppDomain;


    public void InitializeAppDomain()
    {
      m_firstAppDomain = AppDomain.CreateDomain("FIRST_APPDOMAIN");
      m_secondAppDomain = AppDomain.CreateDomain("SECOND_APPDOMAIN");

      m_chw_forFirstAppDomain = (CacheHelperWrapper)m_firstAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.NewAPI.CacheHelperWrapper");
      m_chw_forSecondAppDomain = (CacheHelperWrapper)m_secondAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.NewAPI.CacheHelperWrapper");

      m_putGetTests_forFirstAppDomain = (PutGetTestsAD)m_firstAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.NewAPI.PutGetTestsAD");
      m_putGetTests_forSecondAppDomain = (PutGetTestsAD)m_secondAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.NewAPI.PutGetTestsAD");
    }

    public void CloseCacheAD()
    {
      try
      {
        m_chw_forFirstAppDomain.CloseCache();
      }
      catch (Exception e)
      {
        Util.Log(e.Message + e.StackTrace);
        throw e;
      }
      m_chw_forSecondAppDomain.CloseCache();

      CacheHelper.CloseCache();
    }

    //for appdomain
    public virtual void CreateTCRegions_Pool_AD(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, long dtTime)
    {
      CreateTCRegions_Pool_AD2(regionNames, locators, poolName, clientNotification, ssl, true, false);

      CacheableHelper.RegisterBuiltinsAD(dtTime);
      try
      {
        CacheHelperWrapper chw = new CacheHelperWrapper();
        chw.CreateTCRegions_Pool_AD<object, object>(regionNames, locators, poolName, clientNotification, ssl, true, false);
        //Util.LogFile = filename;
        //CacheHelper.ConnectConfig("DSNAME", null);
      }
      catch (Exception e)
      {
        Console.WriteLine(" fot exception in main process " + e.Message);
      }
    }

    public virtual void CreateTCRegions_Pool_AD2(string[] regionNames,
      string locators, string poolName, bool clientNotification, bool ssl, bool caching, bool pdxReadSerialized)
    {
      string filename = Util.DUnitLogDir + System.IO.Path.DirectorySeparatorChar + "tmp" + ".log";
      m_chw_forFirstAppDomain.SetLogFile(filename);
      m_chw_forSecondAppDomain.SetLogFile(filename);

      m_chw_forFirstAppDomain.CreateTCRegions_Pool_AD<object, object>(regionNames, locators, poolName, clientNotification, ssl, caching, pdxReadSerialized);
      //   m_chw_forFirstAppDomain.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
      //m_regionNames = regionNames;

      try
      {

        // m_chw_forSecondAppDomain.CallDistrinbutedConnect();
        m_chw_forSecondAppDomain.CreateTCRegions_Pool_AD<object, object>(regionNames, locators, poolName, clientNotification, ssl, caching, pdxReadSerialized);
        Console.WriteLine("initializing second app domain done");
      }
      catch (Exception e)
      {
        Util.Log("initializing second app domain goty exception " + e.Message);
      }
      try
      {
        //   m_chw_forSecondAppDomain.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
        Serializable.RegisterPdxType(PdxTests.PdxType.CreateDeserializable);
        CacheHelperWrapper chw = new CacheHelperWrapper();
        chw.CreateTCRegions_Pool_AD<object, object>(regionNames, locators, poolName, clientNotification, ssl, caching, pdxReadSerialized);
      }
      catch (Exception)
      {
      }
    }
    public virtual void SetRegionAD(String regionName)
    {
      m_putGetTests_forFirstAppDomain.SetRegion(regionName);
      m_putGetTests_forSecondAppDomain.SetRegion(regionName);
    }

    public virtual void pdxPutGetTest(bool caching, bool readPdxSerialized)
    {
      m_putGetTests_forFirstAppDomain.pdxPutGet(caching, readPdxSerialized);
    }

    public virtual void pdxGetPutTest(bool caching, bool readPdxSerialized)
    {
      m_putGetTests_forSecondAppDomain.pdxGetPut(caching, readPdxSerialized);
    }

    public virtual void RegisterBuiltinsAD(long dtTime)
    {
      m_chw_forFirstAppDomain.RegisterBuiltins(dtTime);
      m_chw_forSecondAppDomain.RegisterBuiltins(dtTime);
    }

    public void TestAllKeyValuePairsAD(string regionName, bool runQuery, long dtTime)
    {


      ICollection<UInt32> registeredKeyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> registeredValueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();

      m_chw_forFirstAppDomain.RegisterBuiltins(dtTime);
      m_chw_forSecondAppDomain.RegisterBuiltins(dtTime);

      foreach (UInt32 keyTypeId in registeredKeyTypeIds)
      {
        int numKeys;
        numKeys = m_putGetTests_forFirstAppDomain.InitKeys(keyTypeId, PutGetTests.NumKeys, PutGetTests.KeySize);
        numKeys = m_putGetTests_forSecondAppDomain.InitKeys(keyTypeId, PutGetTests.NumKeys, PutGetTests.KeySize);
        Type keyType = CacheableWrapperFactory.GetTypeForId(keyTypeId);

        foreach (UInt32 valueTypeId in registeredValueTypeIds)
        {
          m_putGetTests_forFirstAppDomain.InitValues(valueTypeId, numKeys, PutGetTests.ValueSize);
          m_putGetTests_forSecondAppDomain.InitValues(valueTypeId, numKeys, PutGetTests.ValueSize);
          Type valueType = CacheableWrapperFactory.GetTypeForId(valueTypeId);

          Util.Log("Starting gets/puts with keyType '{0}' and valueType '{1}'",
            keyType.Name, valueType.Name);
          // StartTimer();
          Util.Log("Running warmup task which verifies the puts.");
          PutGetStepsAD(regionName, true, runQuery);
          Util.Log("End warmup task.");
          /*LogTaskTiming(client1,
            string.Format("IRegion<object, object>:{0},Key:{1},Value:{2},KeySize:{3},ValueSize:{4},NumOps:{5}",
            regionName, keyType.Name, valueType.Name, KeySize, ValueSize, 4 * numKeys),
            4 * numKeys);
          */
          //m_chw_forFirstAppDomain.InvalidateRegion(regionName);
          // m_chw_forSecondAppDomain.InvalidateRegion(regionName);

        }
      }

      CloseCacheAD();

      CacheHelper.Close();

    }

    public void PutGetStepsAD(string regionName, bool verifyGets, bool runQuery)
    {
      if (verifyGets)
      {
        m_putGetTests_forFirstAppDomain.DoPuts();
        m_putGetTests_forFirstAppDomain.DoKeyChecksumPuts();
        m_putGetTests_forFirstAppDomain.DoValChecksumPuts();
        m_putGetTests_forSecondAppDomain.DoGetsVerify();
        //InvalidateRegion(regionName, client1);
        //m_chw_forFirstAppDomain.InvalidateRegion(regionName);
        // m_chw_forSecondAppDomain.InvalidateRegion(regionName);
        if (runQuery)
        {
          // run a query for ThinClient regions to check for deserialization
          // compability on server
          m_putGetTests_forFirstAppDomain.DoRunQuery();
        }
        m_putGetTests_forSecondAppDomain.DoPuts();
        m_putGetTests_forSecondAppDomain.DoKeyChecksumPuts();
        m_putGetTests_forSecondAppDomain.DoValChecksumPuts();
        m_putGetTests_forFirstAppDomain.DoGetsVerify();
      }
      else
      {
        m_putGetTests_forFirstAppDomain.DoPuts();
        m_putGetTests_forSecondAppDomain.DoGets();
        m_chw_forFirstAppDomain.InvalidateRegion(regionName);
        m_chw_forSecondAppDomain.InvalidateRegion(regionName);
        m_putGetTests_forSecondAppDomain.DoPuts();
        m_putGetTests_forFirstAppDomain.DoGets();
      }
      // this query invocation is primarily to delete the entries that cannot
      // be deserialized by the server
      if (runQuery)
      {
        m_putGetTests_forFirstAppDomain.DoRunQuery();
      }
    }

    public virtual void DestroyRegions()
    {
      if (m_regionNames != null)
      {
        CacheHelper.DestroyRegion<object, object>(m_regionNames[0], false, true);
        CacheHelper.DestroyRegion<object, object>(m_regionNames[1], false, true);
      }
    }

    public virtual void RegisterAllKeysR0WithoutValues()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region0.GetSubscriptionService().RegisterAllKeys(false, null, false, false);
    }

    public virtual void RegisterAllKeysR1WithoutValues()
    {
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      region1.GetSubscriptionService().RegisterAllKeys(false, null, false, false);
    }

    public virtual void StepThree()
    {
      CreateEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      CreateEntry(m_regionNames[1], m_keys[2], m_vals[2]);
    }

    public virtual void StepFour()
    {
      DoNetsearch(m_regionNames[0], m_keys[0], m_vals[0], false);
      DoNetsearch(m_regionNames[1], m_keys[2], m_vals[2], false);
      CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public virtual void StepFive(bool checkVal)
    {
      DoNetsearch(m_regionNames[0], m_keys[1], m_vals[1], false);
      DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], false);
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], checkVal);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], checkVal);
    }

    public virtual void StepSix(bool checkVal)
    {
      // Some CPU intensive code to test back pressure
      double y = 100.01;
      for (int i = 100000000; i > 0; i--)
      {
        double x = i * y;
        y = x / i;
      }

      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
      UpdateEntry(m_regionNames[0], m_keys[1], m_nvals[1], checkVal);
      UpdateEntry(m_regionNames[1], m_keys[3], m_nvals[3], checkVal);
    }

    //public virtual void StepSeven()
    //{
    //  VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
    //  VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3]);
    //  InvalidateEntry(m_regionNames[0], m_keys[0]);
    //  InvalidateEntry(m_regionNames[1], m_keys[2]);
    //}

    //public virtual void StepEight()
    //{
    //  VerifyInvalid(m_regionNames[0], m_keys[0]);
    //  VerifyInvalid(m_regionNames[1], m_keys[2]);
    //  InvalidateEntry(m_regionNames[0], m_keys[1]);
    //  InvalidateEntry(m_regionNames[1], m_keys[3]);
    //}

    //public virtual void StepNine()
    //{
    //  VerifyInvalid(m_regionNames[0], m_keys[1]);
    //  VerifyInvalid(m_regionNames[1], m_keys[3]);
    //  DestroyEntry(m_regionNames[0], m_keys[0]);
    //  DestroyEntry(m_regionNames[1], m_keys[2]);
    //}

    public virtual void StepTen()
    {
      VerifyDestroyed(m_regionNames[0], m_keys[0]);
      VerifyDestroyed(m_regionNames[1], m_keys[2]);
      DestroyEntry(m_regionNames[0], m_keys[1]);
      DestroyEntry(m_regionNames[1], m_keys[3]);
    }

    public virtual void StepEleven()
    {
      VerifyDestroyed(m_regionNames[0], m_keys[1]);
      VerifyDestroyed(m_regionNames[1], m_keys[3]);
    }
    
   

    public virtual void RemoveStepFive()
    {
      IRegion<object, object> reg0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> reg1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);

      ICollection<KeyValuePair<Object, Object>> IcollectionRegion0 = reg0;
      ICollection<KeyValuePair<Object, Object>> IcollectionRegion1 = reg1;

      // Try removing non-existent entry from regions, result should be false.      
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[4], m_vals[4])), "Result of Remove should be false, as this entry is not present in first region.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[4], m_vals[4])), "Result of Remove should be false, as this entry is not present in second region.");

      // Try removing non-existent key, but existing value from regions, result should be false.
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[4], m_vals[0])), "Result of Remove should be false, as this key is not present in first region.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[4], m_vals[0])), "Result of Remove should be false, as this key is not present in second region.");

      // Try removing existent key, but non-existing value from regions, result should be false.
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[0], m_vals[4])), "Result of Remove should be false, as this value is not present in first region.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[0], m_vals[4])), "Result of Remove should be false, as this value is not present in second region.");

      // Try removing existent key, and existing value from regions, result should be true.      
      Assert.IsTrue(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[0], m_vals[0])), "Result of Remove should be true, as this entry is present in first region.");
      Assert.IsTrue(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[2], m_vals[2])), "Result of Remove should be true, as this entry is present in second region.");
      Assert.IsFalse(reg0.ContainsKey(m_keys[0]), "ContainsKey should be false");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[0]), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[2]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[2]), "GetLocalView().ContainsKey should be false");

      // Try removing already deleted entry from regions, result should be false, but no exception.
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[0], m_vals[0])), "Result of Remove should be false, as this entry is not present in first region.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[0], m_vals[0])), "Result of Remove should be false, as this entry is not present in second region.");

      // Try locally destroying already deleted entry from regions, It should result into exception.
      Assert.IsFalse(reg0.GetLocalView().Remove(m_keys[0], null), "local Destroy on already removed key should have returned false");
      Assert.IsFalse(reg1.GetLocalView().Remove(m_keys[0], null), "local Destroy on already removed key should have returned false");

      Util.Log("StepFive complete.");
    }

    public virtual void RemoveStepSix()
    {
      IRegion<object, object> reg0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> reg1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);

      ICollection<KeyValuePair<Object, Object>> IcollectionRegion0 = reg0;
      ICollection<KeyValuePair<Object, Object>> IcollectionRegion1 = reg1;

      reg0[m_keys[1]] = m_nvals[1];
      reg1[m_keys[3]] = m_nvals[3];

      // Try removing value that is present on client as well as server, result should be true.
      Assert.IsTrue(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_nvals[1])), "Result of Remove should be true, as this value is present locally, & also present on server.");
      Assert.IsTrue(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_nvals[3])), "Result of Remove should be true, as this value is present locally, & also present on server.");
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be false");
      Util.Log("Step 6.1 complete.");

      // Try removing value that is present on client but not on server, result should be false.
      reg0[m_keys[1]] = m_vals[1];
      reg1[m_keys[3]] = m_vals[3];
      reg0.GetLocalView()[m_keys[1]] = m_nvals[1];
      reg1.GetLocalView()[m_keys[3]] = m_nvals[3];
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_nvals[1])), "Result of Remove should be false, as this value is present locally, but not present on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_nvals[3])), "Result of Remove should be false, as this value is present locally, but not present on server.");
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be true");
      Util.Log("Step 6.2 complete.");

      // Try removing value that is not present on client but present on server, result should be false.
      reg0.Remove(m_keys[1]);
      reg1.Remove(m_keys[3]);
      reg0[m_keys[1]] = m_vals[1];
      reg1[m_keys[3]] = m_vals[3];
      reg0.GetLocalView()[m_keys[1]] = m_nvals[1];
      reg1.GetLocalView()[m_keys[3]] = m_nvals[3];
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be false, as this value is not present locally, but present only on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value is not present locally, but present only on server.");
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be true");
      Util.Log("Step 6.3 complete.");

      // Try removing value that is invalidated on client but exists on server, result should be false.
      reg0.Remove(m_keys[1]);
      reg1.Remove(m_keys[3]);
      reg0[m_keys[1]] = m_vals[1];
      reg1[m_keys[3]] = m_vals[3];
      reg0.GetLocalView().Invalidate(m_keys[1]);
      reg1.GetLocalView().Invalidate(m_keys[3]);
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_nvals[1])), "Result of Remove should be false, as this value is not present locally, but present only on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_nvals[3])), "Result of Remove should be false, as this value is not present locally, but present only on server.");
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be true");
      Util.Log("Step 6.4 complete.");

      // Try removing null value, that is invalidated on client but exists on the server, result should be false.
      reg0.Remove(m_keys[1]);
      reg1.Remove(m_keys[3]);
      reg0[m_keys[1]] = m_vals[1];
      reg1[m_keys[3]] = m_vals[3];
      reg0.GetLocalView().Invalidate(m_keys[1]);
      reg1.GetLocalView().Invalidate(m_keys[3]);
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be false, as this value is not present locally, but present only on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], null)), "Result of Remove should be false, as this value is not present locally, but present only on server.");
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be true");
      Util.Log("Step 6.5 complete.");

      // Try removing a entry (value) which is not present on client as well as server, result should be false.
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>("NewKey1", "NewValue1")), "Result of Remove should be false, as this value is not present locally, and not present on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>("NewKey3", "NewValue3")), "Result of Remove should be false, as this value is not present locally, and not present on server.");
      Assert.IsFalse(reg0.ContainsKey("NewKey1"), "ContainsKey should be false");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey("NewKey1"), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey("NewKey3"), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey("NewKey3"), "GetLocalView().ContainsKey should be false");
      Util.Log("Step 6.6 complete.");

      // Try removing a entry with a null value, which is not present on client as well as server, result should be false.
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>("NewKey1", null)), "Result of Remove should be false, as this value is not present locally, and not present on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>("NewKey3", null)), "Result of Remove should be false, as this value is not present locally, and not present on server.");
      Assert.IsFalse(reg0.ContainsKey("NewKey1"), "ContainsKey should be false");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey("NewKey1"), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey("NewKey3"), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey("NewKey3"), "GetLocalView().ContainsKey should be false");
      Util.Log("Step 6.7 complete.");

      // Try removing a entry (value) which is not present on client but exists on the server, result should be true.
      reg0.Remove(m_keys[1]);
      reg1.Remove(m_keys[3]);
      reg0[m_keys[1]] = m_nvals[1];
      reg1[m_keys[3]] = m_nvals[3];
      reg0.GetLocalView().Remove(m_keys[1]);
      reg1.GetLocalView().Remove(m_keys[3]);
      Assert.IsTrue(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_nvals[1])), "Result of Remove should be true, as this value does not exist locally, but exists on server.");
      Assert.IsTrue(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_nvals[3])), "Result of Remove should be true, as this value does not exist locally, but exists on server.");
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be false");
      Util.Log("Step6.8 complete.");

      reg0[m_keys[1]] = m_nvals[1];
      reg1[m_keys[3]] = m_nvals[3];
      reg0.Remove(m_keys[1]);
      reg1.Remove(m_keys[3]);
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be false, as this value does not exist locally, but exists on server.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], null)), "Result of Remove should be false, as this value does not exist locally, but exists on server.");
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "GetLocalView().ContainsKey should be false");
      Util.Log("Step6.8.1 complete.");

      // Try locally removing an entry which is locally destroyed with a NULL.
      reg0[m_keys[1]] = m_vals[1];
      reg1[m_keys[3]] = m_vals[3];
      Assert.IsTrue(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be true, as this value does not exists locally.");
      Assert.IsFalse(IcollectionRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsTrue(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]), "ContainsKey should be false");
      Util.Log("Step6.8.2 complete.");

      //-------------------------------------GetLocalView().Remove Testcases------------------------------------------------

      ICollection<KeyValuePair<Object, Object>> IcollectionLocalRegion0 = reg0.GetLocalView();
      ICollection<KeyValuePair<Object, Object>> IcollectionLocalRegion1 = reg1.GetLocalView();

      // Try locally removing an entry (value) which is present on the client.
      reg0.Remove(m_keys[1]);
      reg1.Remove(m_keys[3]);
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      Assert.IsTrue(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be true, as this value exists locally.");
      Assert.IsTrue(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be true, as this value exists locally.");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be false");
      Util.Log("Step6.9 complete.");
      Assert.IsFalse(reg0.GetLocalView().Remove(m_keys[1], null), "local Destroy on already removed key should have returned false");
      Assert.IsFalse(reg1.GetLocalView().Remove(m_keys[3], null), "local Destroy on already removed key should have returned false");

      Util.Log("Step6.10 complete.");

      // Try locally removing an entry (value) which is not present on the client (value mismatch).
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      Assert.IsFalse(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_nvals[1])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_nvals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be true");
      Util.Log("Step6.11 complete.");

      // Try locally removing an entry (value) which is invalidated with a value.
      reg0.GetLocalView().Remove(m_keys[1]);
      reg1.GetLocalView().Remove(m_keys[3]);
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      reg0.GetLocalView().Invalidate(m_keys[1]);
      reg1.GetLocalView().Invalidate(m_keys[3]);
      Assert.IsFalse(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be true");
      Util.Log("Step6.12 complete.");

      // Try locally removing an entry (value) which is invalidated with a NULL.
      reg0.GetLocalView().Remove(m_keys[1]);
      reg1.GetLocalView().Remove(m_keys[3]);
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      reg0.GetLocalView().Invalidate(m_keys[1]);
      reg1.GetLocalView().Invalidate(m_keys[3]);
      Assert.IsTrue(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be true, as this value does not exists locally.");
      Assert.IsTrue(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], null)), "Result of Remove should be true, as this value does not exists locally.");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be false");
      Util.Log("Step6.13 complete.");

      // Try locally removing an entry (value) with a NULL.
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      Assert.IsFalse(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], null)), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsTrue(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be true");
      Util.Log("Step6.14 complete.");

      // Try locally removing an entry which is locally destroyed with a value.
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      reg0.GetLocalView().Remove(m_keys[1]);
      reg1.GetLocalView().Remove(m_keys[3]);
      Assert.IsFalse(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be true");
      Util.Log("Step6.15 complete.");

      // Try locally removing an entry which is locally destroyed with a NULL.
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      reg0.GetLocalView().Remove(m_keys[1]);
      reg1.GetLocalView().Remove(m_keys[3]);
      Assert.IsFalse(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], null)), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be false");
      Util.Log("Step6.16 complete.");

      // Try locally removing an entry which is already removed.
      reg0.GetLocalView()[m_keys[1]] = m_vals[1];
      reg1.GetLocalView()[m_keys[3]] = m_vals[3];
      Assert.IsTrue(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be true, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion0.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsTrue(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(IcollectionLocalRegion1.Remove(new KeyValuePair<Object, Object>(m_keys[3], m_vals[3])), "Result of Remove should be false, as this value does not exists locally.");
      Assert.IsFalse(reg0.GetLocalView().ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg1.GetLocalView().ContainsKey(m_keys[3]), "ContainsKey should be false");
      Util.Log("Step6.17 complete.");
      // Try locally removing an entry when region scope is not null.

      Util.Log("StepSix complete.");
    }

    public virtual void RemoveStepEight()
    {
      IRegion<object, object> reg = CacheHelper.GetVerifyRegion<object, object>("exampleRegion");

      ICollection<KeyValuePair<Object, Object>> IcollectionRegion = reg;

      // Try removing a entry which is present on client (value) but invalidated on the server, result should be false.
      reg.Remove(m_keys[0]);
      reg.Remove(m_keys[1]);
      reg[m_keys[0]] = m_vals[0];
      reg[m_keys[1]] = m_vals[1];
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsFalse(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[0], m_vals[0])), "Result of Remove should be false, as this value is present locally, but not present on server.");
      Assert.IsFalse(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[1], m_vals[1])), "Result of Remove should be false, as this value is present locally, but not present on server.");
      Assert.IsTrue(reg.ContainsKey(m_keys[0]), "ContainsKey should be true");
      Assert.IsTrue(reg.GetLocalView().ContainsKey(m_keys[0]), "GetLocalView().ContainsKey should be true");
      Assert.IsTrue(reg.ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be true");
      Util.Log("Step 8.1 complete.");

      // Try removing a entry that is not present on client, but invalidated on server with null value, result should be true.
      reg.Remove(m_keys[0]);
      reg.Remove(m_keys[1]);
      reg[m_keys[0]] = m_nvals[0];
      reg[m_keys[1]] = m_nvals[1];
      reg.GetLocalView().Remove(m_keys[0]);
      reg.GetLocalView().Remove(m_keys[1]);
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsTrue(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[0], null)), "Result of Remove should be true, as this value is not present locally, & not present on server.");
      Assert.IsTrue(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be true, as this value is not present locally, & not present on server.");
      Assert.IsFalse(reg.GetLocalView().ContainsKey(m_keys[0]), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKey(m_keys[0]), "ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be false");
      Util.Log("Step 8.2 complete.");

      // Try removing a entry with a (value) on client that is invalidated on server with null , result should be false.
      reg.Remove(m_keys[0]);
      reg.Remove(m_keys[1]);
      reg[m_keys[0]] = m_nvals[0];
      reg[m_keys[1]] = m_nvals[1];
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsFalse(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[0], null)), "Result of Remove should be false, as this value is present locally, & not present on server.");
      Assert.IsFalse(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be false, as this value is present locally, & not present on server.");
      Assert.IsTrue(reg.ContainsKey(m_keys[0]), "ContainsKey should be true");
      Assert.IsTrue(reg.GetLocalView().ContainsKey(m_keys[0]), "GetLocalView().ContainsKey should be true");
      Assert.IsTrue(reg.ContainsKey(m_keys[1]), "ContainsKey should be true");
      Assert.IsTrue(reg.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be true");
      Util.Log("Step 8.3 complete.");

      // Try removing a entry with a entry that is invalidated on the client as well as on server with a null value, result should be true.
      reg.Remove(m_keys[0]);
      reg.Remove(m_keys[1]);
      reg[m_keys[0]] = m_nvals[0];
      reg[m_keys[1]] = m_nvals[1];
      reg.Invalidate(m_keys[0]);
      reg.Invalidate(m_keys[1]);
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsTrue(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[0], null)), "Result of Remove should be true, as this value is not present locally, & not present on server.");
      Assert.IsTrue(IcollectionRegion.Remove(new KeyValuePair<Object, Object>(m_keys[1], null)), "Result of Remove should be true, as this value is not present locally, & not present on server.");
      Assert.IsFalse(reg.ContainsKey(m_keys[0]), "ContainsKey should be false");
      Assert.IsFalse(reg.GetLocalView().ContainsKey(m_keys[0]), "GetLocalView().ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKey(m_keys[1]), "ContainsKey should be false");
      Assert.IsFalse(reg.GetLocalView().ContainsKey(m_keys[1]), "GetLocalView().ContainsKey should be false");

      // Test case for Bug #639, destroy operation on key that is not present in the region reduces the region's size by 1.
      // Steps to reproduce: Put 2 entries in to region. Destroy an entry that is not present in the region. Check for the sizes of keys, values and region. 
      // It is observed that regions size is less by 1 from that of keys and values sizes.

      reg["Key100"] = "Value100";
      reg["Key200"] = "Value200";
      Util.Log("Region 2 puts complete ");
      Util.Log("Regions size = {0} ", reg.Count);

      reg.Remove("key300");
      Assert.IsTrue(reg.Count == 2, "region size should be equal to 2");

      System.Collections.Generic.ICollection<object> keys = reg.Keys;
      Util.Log("Region keys = {0} ", keys.Count);
      Assert.IsTrue(keys.Count == reg.Count, "region size should be equal to keys size");

      System.Collections.Generic.ICollection<object> values = reg.Values;
      Util.Log("Region values = {0} ", values.Count);
      Assert.IsTrue(values.Count == reg.Count, "region size should be equal to values size");

      reg.Remove("Key100");

      keys = reg.Keys;
      Util.Log("Region keys = {0} ", keys.Count);
      Assert.IsTrue(keys.Count == reg.Count, "region size should be equal to keys size");

      values = reg.Values;
      Util.Log("Region values = {0} ", values.Count);
      Assert.IsTrue(values.Count == reg.Count, "region size should be equal to values size");  

      Util.Log("RemoveStepEight complete.");
    }
    public virtual void LocalOpsStepOne()
    {
        IRegion<object, object> reg0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
        IRegion<object, object> localregion = reg0.GetLocalView();
        localregion.Add(1, 1);
        object val = localregion[1];
        Assert.IsTrue(val.Equals(1), "value should be equal");
        localregion.Invalidate(1);
        val = localregion[1];
        Assert.AreEqual(val, null);

        localregion.Add(2, "IntKeyStringValue");
        val = localregion[1];
        Assert.IsTrue(val.Equals("IntKeyStringValue"), "value should be equal");

        try
        {
            localregion.Add(1, 1);
            Assert.Fail("Expected EntryExistException here");
        }
        catch (EntryExistsException )
        {
            Util.Log(" Expected EntryExistsException exception thrown by localCreate"); 
        }

        string key = "LongKeyStringValue";
        Int64 val1 = 9843754396659L;
        localregion.Add(key, val1);
        val = localregion[key];
        Assert.IsTrue(val.Equals(val1), "value should be equal");

        Int64 key1= 34324242L;
        string val11 = "LongKeyStringValue";
        localregion.Add(key1, val11);
        val = localregion[key1];
        Assert.IsTrue(val.Equals(val11), "value should be equal");

        Int64 key2 = 34324242L;
        Int64 val2 = 9843754396659L;
        localregion.Add(key2, val2);
        val = localregion[key2];
        Assert.IsTrue(val.Equals(val2), "value should be equal");

        string key3 = "LongKeyStringValue";
        string val3 = "LongKeyStringValue";
        localregion.Add(key3, val3);
        val = localregion[key3];
        Assert.IsTrue(val.Equals(val3), "value should be equal");
        
        string i = "";
        try
        {
            localregion.Add(i, val1);
            Assert.Fail("Expected IllegalArgumentException  here");
        }
        catch (IllegalArgumentException )
        {
            Util.Log(" Expected IllegalArgumentException exception thrown by localCreate");
        }
        try
        {
            localregion[i] = val1;
            Assert.Fail("Expected IllegalArgumentException  here");
        }
        catch (IllegalArgumentException )
        {
            Util.Log(" Expected IllegalArgumentException exception thrown by localCreate");
        }
        try
        {
            localregion.Remove(i);
            Assert.Fail("Expected IllegalArgumentException  here");
        }
        catch (IllegalArgumentException )
        {
            Util.Log(" Expected IllegalArgumentException exception thrown by localCreate");
        }
        try
        {
            localregion.Invalidate(i);
            Assert.Fail("Expected IllegalArgumentException  here");
        }
        catch (IllegalArgumentException)
        {
            Util.Log(" Expected IllegalArgumentException exception thrown by localCreate");
        }
        

        Util.Log("LocalOpsStepOne complete.");
    }
    public virtual void IdictionaryRegionNullKeyOperations(String RegionName)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      IRegion<object, object> localRegion = region.GetLocalView();

      RegionNullKeyOperations(region, true);
      RegionNullKeyOperations(localRegion, false);
    }

    public virtual void RegionNullKeyOperations(IRegion<object, object> region, bool isRemoteInstance)
    {
      // chk for IllegalArgumentException.
      object value = 0;
      object NullKey = null;
      try
      {
        region.TryGetValue(NullKey, out value); //null
        Assert.Fail("Should have got IllegalArgumentException for null key arguement.");
      }
      catch (IllegalArgumentException ex)
      {
        Util.Log("Got expected IllegalArgumentException for null key arguement. {0} ", ex);
      }
      Util.Log("RegionNullKeyOperations TryGetValue Step complete.");

      // Try adding using Add, an entry in region with key as null, should get IllegalArgumentException.
      try
      {
        region.Add(NullKey, value); // null
        Assert.Fail("Should have got IllegalArgumentException.");
      }
      catch (IllegalArgumentException ex)
      {
        Util.Log("Got expected IllegalArgumentException {0}", ex);
      }

      try
      {
        region.Add(new KeyValuePair<object, object>(NullKey, value));//null
        Assert.Fail("Should have got IllegalArgumentException.");
      }
      catch (IllegalArgumentException ex)
      {
        Util.Log("Got expected IllegalArgumentException {0}", ex);
      }
      Util.Log("RegionNullKeyOperations Add Step complete.");

      // Try putting using Item_Set, an entry in region with key as null, should get IllegalArgumentException.
      try
      {
        region[NullKey] = value; //null key
        Assert.Fail("Should have got IllegalArgumentException");
      }
      catch (IllegalArgumentException)
      {
        Util.Log("Got expected IllegalArgumentException ");
      }
      Util.Log("RegionNullKeyOperations Item_Set Step complete.");

      // Try putting using Item_Set, an entry in region with key as null, should get IllegalArgumentException.
      try
      {
        Object val = region[NullKey];//null
        Assert.Fail("Should have got IllegalArgumentException");
      }
      catch (IllegalArgumentException)
      {
        Util.Log("Got expected IllegalArgumentException ");
      }
      Util.Log("RegionNullKeyOperations Item_Get Step complete.");

      // Try contains with null key, i should throw exception.
      try
      {
        region.Contains(new KeyValuePair<object, object>(NullKey, value));//null key
        Assert.Fail("Should have got IllegalArgumentException");

      }
      catch (IllegalArgumentException ex)
      {
        Util.Log("Got expected IllegalArgumentException {0} ", ex);
      }
      Util.Log("RegionNullKeyOperations Contains Step complete.");
      // Try removing entry with null key, should throw IllegalArgumentException.
      try
      {
        region.Remove(NullKey);//null
        Assert.Fail("Should have got IllegalArgumentException.");
      }
      catch (IllegalArgumentException ex)
      {
        Util.Log("Got expected IllegalArgumentException {0} ", ex);
      }

      Util.Log("RegionNullKeyOperations Remove Step complete.");

      try
      {
        Object val = region[1];
        Assert.Fail("Should have got KeyNotFoundException");
      }
      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex)
      {
        Util.Log("Got expected KeyNotFoundException {0} ", ex);
      }
      region[1] = 1;

      //Invalidate an entry and then do a get on to it, should throw EntryNotFoundException.
      region.Invalidate(1);
      try
      {
        Object val = region[1];
        if (!isRemoteInstance)
        {
          Assert.Fail("Should have got KeyNotFoundException");
        }
      }
      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex)
      {
        Util.Log("Got expected KeyNotFoundException {0} ", ex);
      }
      region.Remove(1);

      //Remove an entry and then do a get on to it, should throw KeyNotFoundException.
      try
      {
        Object val = region[1];
        Assert.Fail("Should have got KeyNotFoundException");
      }
      catch (GemStone.GemFire.Cache.Generic.KeyNotFoundException ex)
      {
        Util.Log("Got expected KeyNotFoundException {0} ", ex);
      }

    }
    public virtual void IdictionaryRegionArrayOperations(String RegionName)
    {
      TypesClass type = new TypesClass();

      IdictionaryGenericRegionArrayOperations<int, bool[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.BoolArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, char[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.CharArrayId, false);
      // IdictionaryGenericRegionArrayOperations<int, sbyte[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.SbyteArrayId, false);
      //IdictionaryGenericRegionArrayOperations<int, uint[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.UintArrayId, false);
      //IdictionaryGenericRegionArrayOperations<int, ulong[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.UlongArrayId, false);
      //IdictionaryGenericRegionArrayOperations<int, ushort[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.UshortArrayId, false);      
      IdictionaryGenericRegionArrayOperations<int, string[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.StringArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, byte[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.ByteArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, double[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.DoubleArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, float[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.FloatArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, int[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.IntArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, long[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.LongArrayId, false);
      IdictionaryGenericRegionArrayOperations<int, short[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.ShortArrayId, false);
      //IdictionaryGenericRegionArrayOperations<int, object[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.ObjectArrayId, false);
      //IdictionaryGenericRegionArrayOperations<int, decimal[]>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.DecimalArrayId, false);            

      IdictionaryGenericRegionArrayOperations<string, bool[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.BoolArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, char[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.CharArrayId, false);
      //IdictionaryGenericRegionArrayOperations<string, sbyte[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.SbyteArrayId, false);
      //IdictionaryGenericRegionArrayOperations<string, uint[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.UintArrayId, false);
      //IdictionaryGenericRegionArrayOperations<string, ulong[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.UlongArrayId, false);
      //IdictionaryGenericRegionArrayOperations<string, ushort[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.UshortArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, string[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.StringArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, byte[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.ByteArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, double[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.DoubleArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, float[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.FloatArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, int[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.IntArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, long[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.LongArrayId, false);
      IdictionaryGenericRegionArrayOperations<string, short[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.ShortArrayId, false);
      //IdictionaryGenericRegionArrayOperations<string, object[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.ObjectArrayId, false);            
      //IdictionaryGenericRegionArrayOperations<string, decimal[]>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.DecimalArrayId, false);      

    }

    public virtual void IdictionaryGenericRegionArrayOperations<TKey, TValue>(String RegionName, TypesClass type, int KeyId, int ValueId, bool IsValByRef)
    {
      //Create region and local Region instances.
      IRegion<TKey, TValue> region = CacheHelper.GetVerifyRegion<TKey, TValue>(RegionName);
      IRegion<TKey, TValue> localRegion = region.GetLocalView();

      // Test CopyTo with remote region & local region instances.
      Idictionary_Array_CopyTo_Step<TKey, TValue>(region, type, KeyId, ValueId);
      Idictionary_Array_CopyTo_Step<TKey, TValue>(localRegion, type, KeyId, ValueId);
      Util.Log("IdictionaryRegionOperations array CopyTo complete.");

      // Test TryGetValue with remote region & local region instances.
      Idictionary_Array_TryGetValue_Step<TKey, TValue>(region, type, KeyId, ValueId, IsValByRef);
      Idictionary_Array_TryGetValue_Step<TKey, TValue>(localRegion, type, KeyId, ValueId, IsValByRef);
      Util.Log("IdictionaryRegionOperations array TryGetValue complete.");

      // Test generic & non-generic GetEnumerator with remote region & local region instances.
      Idictionary_Array_GetEnumerator_Step<TKey, TValue>(region, type, KeyId, ValueId);
      Idictionary_Array_GetEnumerator_Step<TKey, TValue>(localRegion, type, KeyId, ValueId);
      Util.Log("IdictionaryRegionOperations array GetEnumerator complete.");

      // Test generic Add/Put/Get/Remove/Contains with remote region & local region instances.
      Idictionary_Array_Item_Add_Get_Set_Remove_Step<TKey, TValue>(region, type, KeyId, ValueId);
      Idictionary_Array_Item_Add_Get_Set_Remove_Step<TKey, TValue>(localRegion, type, KeyId, ValueId);
      Util.Log("IdictionaryRegionOperations array Add/Put/Get/Remove/Contains complete.");

      region.Clear();
      localRegion.Clear();
    }

    public virtual void IdictionaryRegionOperations(String RegionName)
    {
      TypesClass type = new TypesClass();

      //IdictionaryGenericRegionOperations<byte, sbyte>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.ByteId, false);
      ////IdictionaryGenericRegionOperations<byte, sbyte>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<byte, bool>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.BoolId, false);
      //IdictionaryGenericRegionOperations<byte, char>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.CharId, false);
      //IdictionaryGenericRegionOperations<byte, float>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.FloatId, false);
      //IdictionaryGenericRegionOperations<byte, double>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.DoubleId, false);
      //IdictionaryGenericRegionOperations<byte, int>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.IntId, false);
      ////IdictionaryGenericRegionOperations<byte, uint>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.UintId, false);
      //IdictionaryGenericRegionOperations<byte, long>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.LongId, false);
      ////IdictionaryGenericRegionOperations<byte, ulong>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.UlongId, false);
      //IdictionaryGenericRegionOperations<byte, object>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.ObjectId, true);
      //IdictionaryGenericRegionOperations<byte, string>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.StringId, true);
      //IdictionaryGenericRegionOperations<byte, short>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.ShortId, false);
      ////IdictionaryGenericRegionOperations<byte, ushort>(RegionName, type, (int)TypesClass.TypeIds.ByteId, (int)TypesClass.TypeIds.UshortId, false);

      //IdictionaryGenericRegionOperations<sbyte, byte>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.ByteId, false);
      IdictionaryGenericRegionOperations<sbyte, sbyte>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<sbyte, bool>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<sbyte, char>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<sbyte, float>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<sbyte, double>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<sbyte, int>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<sbyte, uint>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<sbyte, long>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<sbyte, ulong>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<sbyte, object>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<sbyte, string>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<sbyte, short>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<sbyte, ushort>(RegionName, type, (int)TypesClass.TypeIds.SbyteId, (int)TypesClass.TypeIds.UshortId, false);

      IdictionaryGenericRegionOperations<short, sbyte>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<short, sbyte>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<short, bool>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<short, char>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<short, float>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<short, double>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<short, int>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<short, uint>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<short, long>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<short, ulong>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<short, object>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<short, string>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<short, short>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.ShortId, false);
      // IdictionaryGenericRegionOperations<short, ushort>(RegionName, type, (int)TypesClass.TypeIds.ShortId, (int)TypesClass.TypeIds.UshortId, false);

      //IdictionaryGenericRegionOperations<ushort, byte>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.ByteId, false);
      //IdictionaryGenericRegionOperations<ushort, sbyte>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<ushort, bool>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.BoolId, false);
      //IdictionaryGenericRegionOperations<ushort, char>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.CharId, false);
      //IdictionaryGenericRegionOperations<ushort, float>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.FloatId, false);
      //IdictionaryGenericRegionOperations<ushort, double>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.DoubleId, false);
      //IdictionaryGenericRegionOperations<ushort, int>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<ushort, uint>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.UintId, false);
      //IdictionaryGenericRegionOperations<ushort, long>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<ushort, ulong>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.UlongId, false);
      //IdictionaryGenericRegionOperations<ushort, object>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.ObjectId, true);
      //IdictionaryGenericRegionOperations<ushort, string>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.StringId, true);
      //IdictionaryGenericRegionOperations<ushort, short>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<ushort, ushort>(RegionName, type, (int)TypesClass.TypeIds.UshortId, (int)TypesClass.TypeIds.UshortId, false);

      IdictionaryGenericRegionOperations<int, sbyte>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.SbyteId, false);
      // IdictionaryGenericRegionOperations<int, sbyte>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<int, sbyte>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<int, bool>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<int, char>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<int, float>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<int, double>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<int, int>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<int, uint>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<int, long>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<int, ulong>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<int, object>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<int, string>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<int, short>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<int, ushort>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.UshortId, false);

      //IdictionaryGenericRegionOperations<uint, byte>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.ByteId, false);
      ////IdictionaryGenericRegionOperations<uint, sbyte>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<uint, bool>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.BoolId, false);
      //IdictionaryGenericRegionOperations<uint, char>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.CharId, false);
      //IdictionaryGenericRegionOperations<uint, float>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.FloatId, false);
      //IdictionaryGenericRegionOperations<uint, double>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.DoubleId, false);
      //IdictionaryGenericRegionOperations<uint, int>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.IntId, false);
      ////IdictionaryGenericRegionOperations<uint, uint>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.UintId, false);
      //IdictionaryGenericRegionOperations<uint, long>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.LongId, false);
      ////IdictionaryGenericRegionOperations<uint, ulong>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.UlongId, false);
      //IdictionaryGenericRegionOperations<uint, object>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.ObjectId, true);
      //IdictionaryGenericRegionOperations<uint, string>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.StringId, true);
      //IdictionaryGenericRegionOperations<uint, short>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.ShortId, false);
      ////IdictionaryGenericRegionOperations<uint, ushort>(RegionName, type, (int)TypesClass.TypeIds.UintId, (int)TypesClass.TypeIds.UshortId, false);

      IdictionaryGenericRegionOperations<long, sbyte>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<long, sbyte>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<long, int>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<long, uint>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<long, char>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<long, bool>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<long, float>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<long, double>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<long, long>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<long, ulong>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<long, object>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<long, string>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<long, short>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<long, ushort>(RegionName, type, (int)TypesClass.TypeIds.LongId, (int)TypesClass.TypeIds.UshortId, false);

      //IdictionaryGenericRegionOperations<ulong, byte>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.ByteId, false);
      //IdictionaryGenericRegionOperations<ulong, sbyte>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<ulong, bool>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.BoolId, false);
      //IdictionaryGenericRegionOperations<ulong, char>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.CharId, false);
      //IdictionaryGenericRegionOperations<ulong, float>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.FloatId, false);
      //IdictionaryGenericRegionOperations<ulong, double>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.DoubleId, false);
      //IdictionaryGenericRegionOperations<ulong, int>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<ulong, uint>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.UintId, false);
      //IdictionaryGenericRegionOperations<ulong, long>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<ulong, ulong>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.UlongId, false);
      //IdictionaryGenericRegionOperations<ulong, object>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.ObjectId, true);
      //IdictionaryGenericRegionOperations<ulong, string>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.StringId, true);
      //IdictionaryGenericRegionOperations<ulong, short>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<ulong, ushort>(RegionName, type, (int)TypesClass.TypeIds.UlongId, (int)TypesClass.TypeIds.UshortId, false);      

      IdictionaryGenericRegionOperations<char, sbyte>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<char, sbyte>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<char, int>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<char, uint>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<char, bool>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<char, char>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<char, float>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<char, double>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<char, long>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<char, ulong>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<char, object>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<char, string>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<char, short>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<char, ushort>(RegionName, type, (int)TypesClass.TypeIds.CharId, (int)TypesClass.TypeIds.UshortId, false);

      IdictionaryGenericRegionOperations<float, sbyte>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<float, sbyte>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<float, int>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<float, uint>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<float, char>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<float, bool>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<float, float>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<float, double>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<float, long>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<float, ulong>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<float, object>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<float, string>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<float, short>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<float, ushort>(RegionName, type, (int)TypesClass.TypeIds.FloatId, (int)TypesClass.TypeIds.UshortId, false);

      IdictionaryGenericRegionOperations<double, sbyte>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<double, sbyte>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<double, int>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<double, uint>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<double, char>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<double, bool>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<double, float>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<double, double>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<double, long>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<double, ulong>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<double, object>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<double, string>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<double, short>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<double, ushort>(RegionName, type, (int)TypesClass.TypeIds.DoubleId, (int)TypesClass.TypeIds.UshortId, false);  

      IdictionaryGenericRegionOperations<object, sbyte>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<object, sbyte>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<object, int>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<object, uint>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<object, char>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<object, bool>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<object, float>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<object, double>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<object, long>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<object, ulong>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<object, object>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<object, string>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<object, short>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<object, ushort>(RegionName, type, (int)TypesClass.TypeIds.ObjectId, (int)TypesClass.TypeIds.UshortId, false);

      IdictionaryGenericRegionOperations<String, sbyte>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.SbyteId, false);
      //IdictionaryGenericRegionOperations<String, sbyte>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.SbyteId, false);
      IdictionaryGenericRegionOperations<String, int>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.IntId, false);
      //IdictionaryGenericRegionOperations<String, uint>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.UintId, false);
      IdictionaryGenericRegionOperations<String, char>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.CharId, false);
      IdictionaryGenericRegionOperations<String, bool>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.BoolId, false);
      IdictionaryGenericRegionOperations<String, float>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.FloatId, false);
      IdictionaryGenericRegionOperations<String, double>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.DoubleId, false);
      IdictionaryGenericRegionOperations<String, long>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.LongId, false);
      //IdictionaryGenericRegionOperations<String, ulong>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.UlongId, false);
      IdictionaryGenericRegionOperations<String, object>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.ObjectId, true);
      IdictionaryGenericRegionOperations<String, string>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.StringId, true);
      IdictionaryGenericRegionOperations<String, short>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.ShortId, false);
      //IdictionaryGenericRegionOperations<String, ushort>(RegionName, type, (int)TypesClass.TypeIds.StringId, (int)TypesClass.TypeIds.UshortId, false);

      // decimal is not supported.
      //IdictionaryGenericRegionOperations<int, decimal>(RegionName, type, (int)TypesClass.TypeIds.IntId, (int)TypesClass.TypeIds.DecimalId, false);      
    }

    public virtual void IdictionaryGenericRegionOperations<TKey, TValue>(String RegionName, TypesClass type, int KeyId, int ValueId, bool IsValByRef)
    {
      //Create region and local Region instances.
      IRegion<TKey, TValue> region = CacheHelper.GetVerifyRegion<TKey, TValue>(RegionName);
      IRegion<TKey, TValue> localRegion = region.GetLocalView();

      // Test CopyTo with remote region & local region instances.
      Idictionary_CopyTo_Step<TKey, TValue>(region, type, KeyId, ValueId);
      Idictionary_CopyTo_Step<TKey, TValue>(localRegion, type, KeyId, ValueId);
      Util.Log("IdictionaryRegionOperations CopyTo complete.");

      // Test TryGetValue with remote region & local region instances.
      Idictionary_TryGetValue_Step<TKey, TValue>(region, type, KeyId, ValueId, IsValByRef);
      Idictionary_TryGetValue_Step<TKey, TValue>(localRegion, type, KeyId, ValueId, IsValByRef);
      Util.Log("IdictionaryRegionOperations TryGetValue complete.");

      // Test generic & non-generic GetEnumerator with remote region & local region instances.
      Idictionary_GetEnumerator_Step<TKey, TValue>(region, type, KeyId, ValueId);
      Idictionary_GetEnumerator_Step<TKey, TValue>(localRegion, type, KeyId, ValueId);
      Util.Log("IdictionaryRegionOperations GetEnumerator complete.");

      // Test generic Add/Put/Get/Remove/Contains with remote region & local region instances.
      Idictionary_Item_Add_Get_Set_Remove_Step<TKey, TValue>(region, type, KeyId, ValueId);
      Idictionary_Item_Add_Get_Set_Remove_Step<TKey, TValue>(localRegion, type, KeyId, ValueId);
      Util.Log("IdictionaryRegionOperations Add/Put/Get/Remove/Contains complete.");

      region.Clear();
      localRegion.Clear();
    }

    public virtual void Idictionary_CopyTo_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId)
    {
      Util.Log("Idictionary_CopyTo_Step KeyId = {0} , valueId = {1} ", KeyId, ValueId);
      ICollection<KeyValuePair<TKey, TValue>> IcollectionRegion = region;

      // Create an destination array of size 5 
      KeyValuePair<TKey, TValue>[] DestinationArray = new KeyValuePair<TKey, TValue>[5];
      List<KeyValuePair<TKey, TValue>> DestinationArray1 = new List<KeyValuePair<TKey, TValue>>(5);
      KeyValuePair<TKey, TValue>[] DestinationArray2 = null;

      // Try copying into DestinationArray when no entry is present in the client region, check nothing is crashing.
      region.CopyTo(DestinationArray, 0);
      Assert.AreEqual(false, DestinationArray[0].Equals(null));
      Util.Log("Idictionary_CopyTo_Step 1 complete.");

      // Put 5 items in the region now and test again with above case.
      region[type.GetTypeItem<TKey>(KeyId, 0)] = type.GetTypeItem<TValue>(ValueId, 0);
      Util.Log("Idictionary_CopyTo_Step put1 complete.");
      region[type.GetTypeItem<TKey>(KeyId, 1)] = type.GetTypeItem<TValue>(ValueId, 1);
      region[type.GetTypeItem<TKey>(KeyId, 2)] = type.GetTypeItem<TValue>(ValueId, 2);
      region[type.GetTypeItem<TKey>(KeyId, 3)] = type.GetTypeItem<TValue>(ValueId, 3);
      region[type.GetTypeItem<TKey>(KeyId, 4)] = type.GetTypeItem<TValue>(ValueId, 4);

      region.CopyTo(DestinationArray, 0);

      // chk for ArgumentNullException by passing null array
      try
      {
        region.CopyTo(DestinationArray2, 0);
        Assert.Fail("Should have got ArgumentNullException for null destination array.");
      }
      catch (ArgumentNullException ex)
      {
        Util.Log("Got expected ArgumentNullException for null destination array {0} ", ex);
      }
      Util.Log("Idictionary_CopyTo_Step 2 complete.");

      // chk for ArgumentOutOfRangeException by passing -ve index/huge +ve index.
      try
      {
        region.CopyTo(DestinationArray, -5);
        Assert.Fail("Should have got ArgumentOutOfRangeException for -ve index.");
      }
      catch (ArgumentOutOfRangeException ex)
      {
        Util.Log("Got expected ArgumentOutOfRangeException for -ve index {0} ", ex);
      }
      Util.Log("Idictionary_CopyTo_Step 3 complete. count = {0} array length = {1} ", region.Count, DestinationArray.Length);

      // chk for ArgumentException by passing inappropriate index than length of the array.
      try
      {
        region.CopyTo(DestinationArray, 5);
        Assert.Fail("Should have got ArgumentException for inappropriate index.");
      }
      catch (ArgumentException ex)
      {
        Util.Log("Got expected ArgumentException for inappropriate index {0} ", ex);
      }
      Util.Log("Idictionary_CopyTo_Step 4 complete.");

      // chk for ArgumentException by passing inappropriate arguement as array.
      try
      {
        region.CopyTo(DestinationArray1.ToArray(), 0);
        Assert.Fail("Should have got ArgumentException for inappropriate arguement.");
      }
      catch (ArgumentException ex)
      {
        Util.Log("Got expected ArgumentException for inappropriate arguement {0} ", ex);
      }
      Util.Log("Idictionary_CopyTo_Step 5 complete.");
    }

    public virtual void Idictionary_Array_CopyTo_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId)
    {
      Util.Log("Idictionary_Array_CopyTo_Step KeyId = {0} , valueId = {1} ", KeyId, ValueId);
      ICollection<KeyValuePair<TKey, TValue>> IcollectionRegion = region;

      // Create an destination array of size 5 
      KeyValuePair<TKey, TValue>[] DestinationArray = new KeyValuePair<TKey, TValue>[5];
      List<KeyValuePair<TKey, TValue>> DestinationArray1 = new List<KeyValuePair<TKey, TValue>>(5);
      KeyValuePair<TKey, TValue>[] DestinationArray2 = null;

      // Try copying into DestinationArray when no entry is present in the client region, check nothing is crashing.
      region.CopyTo(DestinationArray, 0);
      Assert.AreEqual(false, DestinationArray[0].Equals(null));
      Util.Log("Idictionary_Array_CopyTo_Step 1 complete.");

      // Put 5 items in the region now and test again with above case.
      region[type.GetTypeItem<TKey>(KeyId, 0)] = type.GetArrayTypeItem<TValue>(ValueId, 0);
      region[type.GetTypeItem<TKey>(KeyId, 1)] = type.GetArrayTypeItem<TValue>(ValueId, 1);
      region[type.GetTypeItem<TKey>(KeyId, 2)] = type.GetArrayTypeItem<TValue>(ValueId, 2);
      region[type.GetTypeItem<TKey>(KeyId, 3)] = type.GetArrayTypeItem<TValue>(ValueId, 3);
      region[type.GetTypeItem<TKey>(KeyId, 4)] = type.GetArrayTypeItem<TValue>(ValueId, 4);
      region.CopyTo(DestinationArray, 0);

      // chk for ArgumentNullException by passing null array
      try
      {
        region.CopyTo(DestinationArray2, 0);
        Assert.Fail("Should have got ArgumentNullException for null destination array.");
      }
      catch (ArgumentNullException ex)
      {
        Util.Log("Got expected ArgumentNullException for null destination array {0} ", ex);
      }
      Util.Log("Idictionary_Array_CopyTo_Step 2 complete.");

      // chk for ArgumentOutOfRangeException by passing -ve index/huge +ve index.
      try
      {
        region.CopyTo(DestinationArray, -5);
        Assert.Fail("Should have got ArgumentOutOfRangeException for -ve index.");
      }
      catch (ArgumentOutOfRangeException ex)
      {
        Util.Log("Got expected ArgumentOutOfRangeException for -ve index {0} ", ex);
      }
      Util.Log("Idictionary_Array_CopyTo_Step 3 complete. count = {0} array length = {1} ", region.Count, DestinationArray.Length);

      // chk for ArgumentException by passing inappropriate index than length of the array.
      try
      {
        region.CopyTo(DestinationArray, 5);
        Assert.Fail("Should have got ArgumentException for inappropriate index.");
      }
      catch (ArgumentException ex)
      {
        Util.Log("Got expected ArgumentException for inappropriate index {0} ", ex);
      }
      Util.Log("Idictionary_Array_CopyTo_Step 4 complete.");

      // chk for ArgumentException by passing inappropriate arguement as array.
      try
      {
        region.CopyTo(DestinationArray1.ToArray(), 0);
        Assert.Fail("Should have got ArgumentException for inappropriate arguement.");
      }
      catch (ArgumentException ex)
      {
        Util.Log("Got expected ArgumentException for inappropriate arguement {0} ", ex);
      }
      Util.Log("Idictionary_Array_CopyTo_Step 5 complete.");
    }


    public virtual void Idictionary_TryGetValue_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId, bool IsValByRef)
    {
      TValue value = default(TValue);

      // chk for entry that is present.
      Assert.IsTrue(region.TryGetValue(type.GetTypeItem<TKey>(KeyId, 0), out value));
      Assert.AreEqual(value, type.GetArrayTypeItem<TValue>(ValueId, 0));
      Util.Log("Idictionary_TryGetValue_Step 1 complete.");

      // chk for entry that is not present.
      Assert.IsFalse(region.TryGetValue(type.GetTypeItem<TKey>(KeyId, 5), out value));
      if (IsValByRef)
      {
        Assert.AreEqual(value, null);
      }
      else
      {
        Assert.AreEqual(value, default(TValue));
      }
      Util.Log("Idictionary_TryGetValue_Step 2 complete.");
    }

    public virtual void Idictionary_Array_TryGetValue_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId, bool IsValByRef)
    {
      TValue value = default(TValue);

      // chk for entry that is present.      
      Assert.IsTrue(region.TryGetValue(type.GetTypeItem<TKey>(KeyId, 0), out value));
      Assert.AreEqual(value, type.GetArrayTypeItem<TValue>(ValueId, 0));
      Util.Log("Idictionary_Array_TryGetValue_Step 1 complete.");

      // chk for entry that is not present.
      Assert.IsFalse(region.TryGetValue(type.GetTypeItem<TKey>(KeyId, 5), out value));
      if (IsValByRef)
      {
        Util.Log("Idictionary_Array_TryGetValue_Step ref value is {0} ", value);
        Assert.AreEqual(value, null);
      }
      else
      {
        Util.Log("Idictionary_Array_TryGetValue_Step value is {0} ", value);
        Assert.AreEqual(value, default(TValue));
      }
      Util.Log("Idictionary_Array_TryGetValue_Step 2 complete.");
    }

    public virtual void Idictionary_GetEnumerator_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId)
    {
      // Generic enumerator
      IEnumerator RegionGenericEnumerator = region.GetEnumerator();

      // Non-Generic enumerator
      IEnumerator<KeyValuePair<TKey, TValue>> RegionEnumerator = region.GetEnumerator();

      // Chk before doing generic Enumerator's MoveNext what is the value at Current and that exception is thrown.
      try
      {
        Object r = RegionGenericEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }

      // Chk before doing non-generic Enumerator's MoveNext what is the value at Current and that exception is thrown.
      try
      {
        Object r = RegionEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      Util.Log("Idictionary_GetEnumerator_Step 1 complete.");

      // Chk after doing Generic Enumerator's Reset, what is the value at Current and that exception is thrown.      
      RegionGenericEnumerator.Reset();
      try
      {
        Object r = RegionGenericEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }

      // Chk after doing non-Generic Enumerator's Reset, what is the value at Current and that exception is thrown.
      RegionEnumerator.Reset();
      try
      {
        Object r = RegionEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      Util.Log("Idictionary_GetEnumerator_Step 2 complete.");

      //Chk for equality of individual elements from within the dictionary with those with Generic enumerator's current.
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();

      //Chk for equality of individual elements from within the dictionary with those with non Generic enumerator's current.
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      Util.Log("Idictionary_GetEnumerator_Step 3 complete.");

      //Modify region by adding 1 more element and see that accessing Generic enumerator does not throw exception.
      region.Add(type.GetTypeItem<TKey>(KeyId, 5), type.GetTypeItem<TValue>(ValueId, 5));
      try
      {
        RegionGenericEnumerator.MoveNext();
        Util.Log("No exception expected.");
      }
      catch (InvalidOperationException ex)
      {
        Assert.Fail("Got unexpected InvalidOperationException. {0} ", ex);
      }

      //Modify region by adding 1 more element and see that accessing non Generic enumerator does not throw exception.
      region.Add(type.GetTypeItem<TKey>(KeyId, 6), type.GetTypeItem<TValue>(ValueId, 6));
      try
      {
        RegionEnumerator.MoveNext();
        Util.Log("No exception expected.");
      }
      catch (InvalidOperationException ex)
      {
        Assert.Fail("Got unexpected InvalidOperationException. {0} ", ex);
      }
      Util.Log("Idictionary_GetEnumerator_Step 4 complete.");

      // Since region has 5 entries chk while enumerating, count of generic enumerator's iterator.
      RegionGenericEnumerator.Reset();
      int RegionEntryCount = 0;
      while (RegionGenericEnumerator.MoveNext())
      {
        RegionEntryCount++;
      }

      Assert.IsTrue(5 == RegionEntryCount, "Region entry count does not match.");
      RegionEntryCount = 0; //This is done for iteration where localRegionInstance calls this method.

      // Since region has 5 entries chk while enumerating, count of non-generic enumerator's iterator.
      RegionEnumerator.Reset();
      while (RegionEnumerator.MoveNext())
      {
        RegionEntryCount++;
      }

      Assert.IsTrue(5 == RegionEntryCount, "Region entry count does not match.");
      RegionEntryCount = 0;
      Util.Log("Idictionary_GetEnumerator_Step 5 complete.");

      // Chk after doing Generic Enumerator's MoveNext to end what is the value at Current and that exception is thrown.      
      RegionGenericEnumerator.MoveNext();

      try
      {
        Object r = RegionGenericEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      // Chk after doing non-Generic Enumerator's MoveNext to end what is the value at Current and that exception is thrown
      RegionEnumerator.MoveNext();
      try
      {
        Object r = RegionEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      Util.Log("Idictionary_GetEnumerator_Step 6 complete.");

      Assert.IsTrue(region.Remove(type.GetTypeItem<TKey>(KeyId, 5)), "Remove should be successfull as entry is present in the region.");

      Assert.IsTrue(region.Remove(type.GetTypeItem<TKey>(KeyId, 6)), "Remove should be successfull as entry is present in the region.");
    }

    public virtual void Idictionary_Array_GetEnumerator_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId)
    {
      // Generic enumerator
      IEnumerator RegionGenericEnumerator = region.GetEnumerator();

      // Non-Generic enumerator
      IEnumerator<KeyValuePair<TKey, TValue>> RegionEnumerator = region.GetEnumerator();

      // Chk before doing generic Enumerator's MoveNext what is the value at Current and that exception is thrown.
      try
      {
        Object r = RegionGenericEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }

      // Chk before doing non-generic Enumerator's MoveNext what is the value at Current and that exception is thrown.
      try
      {
        Object r = RegionEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      Util.Log("Idictionary_Array_GetEnumerator_Step 1 complete.");

      // Chk after doing Generic Enumerator's Reset, what is the value at Current and that exception is thrown.      
      RegionGenericEnumerator.Reset();
      try
      {
        Object r = RegionGenericEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }

      // Chk after doing non-Generic Enumerator's Reset, what is the value at Current and that exception is thrown.
      RegionEnumerator.Reset();
      try
      {
        Object r = RegionEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      Util.Log("Idictionary_Array_GetEnumerator_Step 2 complete.");

      //Chk for equality of individual elements from within the dictionary with those with Generic enumerator's current.
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();
      RegionGenericEnumerator.MoveNext();

      //Chk for equality of individual elements from within the dictionary with those with non Generic enumerator's current.
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      RegionEnumerator.MoveNext();
      Util.Log("Idictionary_Array_GetEnumerator_Step 3 complete.");

      //Modify region by adding 1 more element and see that accessing Generic enumerator does not throw exception.
      region.Add(type.GetTypeItem<TKey>(KeyId, 5), type.GetArrayTypeItem<TValue>(ValueId, 5));
      try
      {
        RegionGenericEnumerator.MoveNext();
        Util.Log("No exception expected.");
      }
      catch (InvalidOperationException ex)
      {
        Assert.Fail("Got unexpected InvalidOperationException. {0} ", ex);
      }

      //Modify region by adding 1 more element and see that accessing non Generic enumerator does not throw exception.
      region.Add(type.GetTypeItem<TKey>(KeyId, 6), type.GetArrayTypeItem<TValue>(ValueId, 6));
      try
      {
        RegionEnumerator.MoveNext();
        Util.Log("No exception expected.");
      }
      catch (InvalidOperationException ex)
      {
        Assert.Fail("Got unexpected InvalidOperationException. {0} ", ex);
      }
      Util.Log("Idictionary_Array_GetEnumerator_Step 4 complete.");

      // Since region has 5 entries chk while enumerating, count of generic enumerator's iterator.
      RegionGenericEnumerator.Reset();
      int RegionEntryCount = 0;
      while (RegionGenericEnumerator.MoveNext())
      {
        RegionEntryCount++;
      }

      Assert.IsTrue(5 == RegionEntryCount, "Region entry count does not match.");
      RegionEntryCount = 0; //This is done for iteration where localRegionInstance calls this method.

      // Since region has 5 entries chk while enumerating, count of non-generic enumerator's iterator.
      RegionEnumerator.Reset();
      while (RegionEnumerator.MoveNext())
      {
        RegionEntryCount++;
      }

      Assert.IsTrue(5 == RegionEntryCount, "Region entry count does not match.");
      RegionEntryCount = 0;
      Util.Log("Idictionary_Array_GetEnumerator_Step 5 complete.");

      // Chk after doing Generic Enumerator's MoveNext to end what is the value at Current and that exception is thrown.      
      RegionGenericEnumerator.MoveNext();

      try
      {
        Object r = RegionGenericEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      // Chk after doing non-Generic Enumerator's MoveNext to end what is the value at Current and that exception is thrown
      RegionEnumerator.MoveNext();
      try
      {
        Object r = RegionEnumerator.Current;
        Assert.Fail("Should have got InvalidOperationException");
      }
      catch (InvalidOperationException ex)
      {
        Util.Log("Got expected InvalidOperationException {0} ", ex);
      }
      Util.Log("Idictionary_Array_GetEnumerator_Step 6 complete.");

      Assert.IsTrue(region.Remove(type.GetTypeItem<TKey>(KeyId, 5)), "Remove should be successfull as entry is present in the region.");

      Assert.IsTrue(region.Remove(type.GetTypeItem<TKey>(KeyId, 6)), "Remove should be successfull as entry is present in the region.");
    }

    public virtual void Idictionary_Array_Item_Add_Get_Set_Remove_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId)
    {
      // Try adding an element that is already existing, should throw EntryExistsException.
      try
      {
        region.Add(type.GetTypeItem<TKey>(KeyId, 0), type.GetArrayTypeItem<TValue>(ValueId, 0));
        Assert.Fail("Should have got EntryExistsException.");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got expected EntryExistsException {0}", ex);
      }
      try
      {
        region.Add(new KeyValuePair<TKey, TValue>(type.GetTypeItem<TKey>(KeyId, 0), type.GetArrayTypeItem<TValue>(ValueId, 0)));
        Assert.Fail("Should have got EntryExistsException.");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got expected EntryExistsException {0}", ex);
      }
      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 1 complete.");

      // Try putting using Item_Set, an entry in region that was already added by Add method, no exception expected.
      try
      {
        region[type.GetTypeItem<TKey>(KeyId, 0)] = type.GetArrayTypeItem<TValue>(ValueId, 0);
        Util.Log("No exception should come.");
      }
      catch (EntryExistsException ex)
      {
        Assert.Fail("EntryExistsException should not have come {0} ", ex);
      }
      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 2 complete.");

      // Try getting using Item_Get, an entry in region that was already added by Add method, no exception expected.
      Assert.AreEqual(type.GetArrayTypeItem<TValue>(ValueId, 0), region[type.GetTypeItem<TKey>(KeyId, 0)]);

      // Chk with old api that it still gets EntryExistsException when an element has been added by new Add method.
      try
      {
        region.Add(type.GetTypeItem<TKey>(KeyId, 2), type.GetArrayTypeItem<TValue>(ValueId, 2), null);
        Assert.Fail("Should have got EntryExistsException");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got Expected EntryExistsException {0} ", ex);
      }
      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 3 complete.");

      // Actually add new element using old Create api.
      region.Add(type.GetTypeItem<TKey>(KeyId, 6), type.GetArrayTypeItem<TValue>(ValueId, 6), null); //8 key

      // Chk now with new api that it still gets EntryExistsException when an element has been added by old Create method.
      try
      {
        region.Add(type.GetTypeItem<TKey>(KeyId, 6), type.GetArrayTypeItem<TValue>(ValueId, 6)); //8 key
        Assert.Fail("Should have got EntryExistsException");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got Expected EntryExistsException {0} ", ex);
      }
      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 4 complete.");

      // Chk with old api that element that is added by new api exists.
      Assert.AreEqual(type.GetArrayTypeItem<TValue>(ValueId, 0), region.Get(type.GetTypeItem<TKey>(KeyId, 0), null));
      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 5 complete.");

      // Try removing entry that does not exist in the region, no exception, but returns false.      
      Assert.IsFalse(region.Remove(type.GetTypeItem<TKey>(KeyId, 5)), "Remove should be unsuccessfull as no entry present.");//100

      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 6 complete.");

      // Try contains with key & value that are present. Result should be true.
      Assert.IsTrue(region.Contains(new KeyValuePair<TKey, TValue>(type.GetTypeItem<TKey>(KeyId, 0), type.GetArrayTypeItem<TValue>(ValueId, 0))), "Result should be true as key & value are present");

      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 7 complete.");

      // Try removing an entry which is actually present in the region.      
      region[type.GetTypeItem<TKey>(KeyId, 7)] = type.GetArrayTypeItem<TValue>(ValueId, 7);
      Assert.IsTrue(region.Remove(type.GetTypeItem<TKey>(KeyId, 7)), "Remove should be successfull as entry is present in the region."); //8

      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 8 complete.");

      Assert.IsTrue(region.Keys.Count == region.Values.Count, "Keys and Values Count should equal.");

      //[ToDo] : Open this once when Ticket #639 is fixed and write test for old region as well.
      /*Assert.IsTrue(region.Keys.Count == region.Count, "Keys and Region Entry Count should equal.");
      Assert.IsTrue(region.Values.Count == region.Count, "Values and Region Entry Count should equal.");*/

      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 9 complete.");

      try
      {
        bool r = region.IsReadOnly;
      }
      catch (NotImplementedException ex)
      {
        Util.Log("Got expected NotImplementedException {0} ", ex);
      }
      Util.Log("Idictionary_Array_Item_Add_Get_Set_Remove_Step 10 complete.");
      region.Remove(type.GetTypeItem<TKey>(KeyId, 6));
      region.Remove(type.GetTypeItem<TKey>(KeyId, 7));
    }

    public virtual void Idictionary_Item_Add_Get_Set_Remove_Step<TKey, TValue>(IRegion<TKey, TValue> region, TypesClass type, int KeyId, int ValueId)
    {
      // Try adding an element that is already existing, should throw EntryExistsException.
      try
      {
        region.Add(type.GetTypeItem<TKey>(KeyId, 0), type.GetTypeItem<TValue>(ValueId, 0));
        Assert.Fail("Should have got EntryExistsException.");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got expected EntryExistsException {0}", ex);
      }
      try
      {
        region.Add(new KeyValuePair<TKey, TValue>(type.GetTypeItem<TKey>(KeyId, 0), type.GetTypeItem<TValue>(ValueId, 0)));
        Assert.Fail("Should have got EntryExistsException.");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got expected EntryExistsException {0}", ex);
      }
      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 1 complete.");

      // Try putting using Item_Set, an entry in region that was already added by Add method, no exception expected.
      try
      {
        region[type.GetTypeItem<TKey>(KeyId, 0)] = type.GetTypeItem<TValue>(ValueId, 0);
        Util.Log("No exception should come.");
      }
      catch (EntryExistsException ex)
      {
        Assert.Fail("EntryExistsException should not have come {0} ", ex);
      }
      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 2 complete.");

      // Try getting using Item_Get, an entry in region that was already added by Add method, no exception expected.
      Assert.AreEqual(type.GetTypeItem<TValue>(ValueId, 0), region[type.GetTypeItem<TKey>(KeyId, 0)]);

      // Chk with old api that it still gets EntryExistsException when an element has been added by new Add method.
      try
      {
        region.Add(type.GetTypeItem<TKey>(KeyId, 2), type.GetTypeItem<TValue>(ValueId, 2), null);
        Assert.Fail("Should have got EntryExistsException");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got Expected EntryExistsException {0} ", ex);
      }
      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 3 complete.");

      // Actually add new element using old Create api.
      region.Add(type.GetTypeItem<TKey>(KeyId, 6), type.GetTypeItem<TValue>(ValueId, 6), null); //8 key

      // Chk now with new api that it still gets EntryExistsException when an element has been added by old Create method.
      try
      {
        region.Add(type.GetTypeItem<TKey>(KeyId, 6), type.GetTypeItem<TValue>(ValueId, 6)); //8 key
        Assert.Fail("Should have got EntryExistsException");
      }
      catch (EntryExistsException ex)
      {
        Util.Log("Got Expected EntryExistsException {0} ", ex);
      }
      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 4 complete.");

      // Chk with old api that element that is added by new api exists.
      Assert.AreEqual(type.GetTypeItem<TValue>(ValueId, 0), region.Get(type.GetTypeItem<TKey>(KeyId, 0), null));
      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 5 complete.");

      // Try removing entry that does not exist in the region, no exception, but returns false.      
      Assert.IsFalse(region.Remove(type.GetTypeItem<TKey>(KeyId, 5)), "Remove should be unsuccessfull as no entry present.");//100

      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 6 complete.");

      // Try contains with key & value that are present. Result should be true.
      Assert.IsTrue(region.Contains(new KeyValuePair<TKey, TValue>(type.GetTypeItem<TKey>(KeyId, 0), type.GetTypeItem<TValue>(ValueId, 0))), "Result should be true as key & value are present");

      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 7 complete.");

      // Try removing an entry which is actually present in the region.      
      region[type.GetTypeItem<TKey>(KeyId, 7)] = type.GetTypeItem<TValue>(ValueId, 7);
      Assert.IsTrue(region.Remove(type.GetTypeItem<TKey>(KeyId, 7)), "Remove should be successfull as entry is present in the region."); //8

      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 8 complete.");

      Assert.IsTrue(region.Keys.Count == region.Values.Count, "Keys and Values Count should equal.");

      //[ToDo] : Open this once when Ticket #639 is fixed and write test for old region as well.
      /*Assert.IsTrue(region.Keys.Count == region.Count, "Keys and Region Entry Count should equal.");
      Assert.IsTrue(region.Values.Count == region.Count, "Values and Region Entry Count should equal.");*/

      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 9 complete.");

      try
      {
        bool r = region.IsReadOnly;
      }
      catch (NotImplementedException ex)
      {
        Util.Log("Got expected NotImplementedException {0} ", ex);
      }
      Util.Log("Idictionary_Item_Add_Get_Set_Remove_Step 10 complete.");
      region.Remove(type.GetTypeItem<TKey>(KeyId, 6));
      region.Remove(type.GetTypeItem<TKey>(KeyId, 7));
    }
    #endregion
  }
}

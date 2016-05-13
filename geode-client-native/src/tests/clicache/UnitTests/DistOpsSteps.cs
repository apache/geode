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

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

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

      Region region = CacheHelper.GetVerifyRegion(regionName);
      CacheableString cKey = new CacheableString(key);
      Thread.Sleep(100); // give distribution threads a chance to run

      // if the region is no ack, then we may need to wait...
      if (!isCreated)
      {
        if (!noKey)
        { // need to find the key!
          Assert.IsTrue(region.ContainsKey(cKey), "Key not found in region.");
        }
        if (val != null && checkVal)
        { // need to have a value!
          Assert.IsTrue(region.ContainsValueForKey(cKey),
            "Value not found in region.");
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
          if (!region.ContainsKey(cKey))
            containsKeyCnt++;
          else
            break;
          Assert.Less(containsKeyCnt, maxLoop,
            "Key has not been created in region.");
        }
        else
        {
          if (noKey)
          {
            if (region.ContainsKey(cKey))
              containsKeyCnt++;
            else
              break;
            Assert.Less(containsKeyCnt, maxLoop, "Key found in region.");
          }
          if (val == null)
          {
            if (region.ContainsValueForKey(cKey))
              containsValueCnt++;
            else
              break;
            Assert.Less(containsValueCnt, maxLoop, "Value found in region.");
          }
          else
          {
            CacheableString cVal = region.Get(cKey) as CacheableString;
            Assert.IsNotNull(cVal, "Value should not be null.");
            if (cVal.Value != val)
              testValueCnt++;
            else
              break;
            Assert.Less(testValueCnt, maxLoop,
              "Incorrect value found. Expected: '{0}' ; Got: '{1}'",
              val, cVal.Value);
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

    protected virtual IGFSerializable GetEntry(string regionName, string key)
    {
      IGFSerializable val = CacheHelper.GetVerifyRegion(regionName).Get(key);
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
      Region region = CacheHelper.GetVerifyRegion(regionName);
      Assert.IsFalse(region.ContainsKey(key), "Key should not have been found in region.");
      region.Create(key, val);
      VerifyEntry(regionName, key, val);
    }

    protected virtual void CreateEntryWithLocatorException(string regionName, string key, string val)
    {
      bool foundException = false;
      try
      {
        CreateEntry(regionName, key, val);
      }
      catch (GemStone.GemFire.Cache.NotConnectedException ex)
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
      Region region = CacheHelper.GetVerifyRegion(regionName);
      Assert.IsTrue(region.ContainsKey(key), "Key should have been found in region.");
      if (checkVal)
      {
        Assert.IsTrue(region.ContainsValueForKey(key),
          "Value should have been found in region.");
      }
      region.Put(key, val);
      VerifyEntry(regionName, key, val, checkVal);
    }

    protected virtual void DoNetsearch(string regionName, string key,
      string val, bool checkNoKey)
    {
      Util.Log("Netsearching for entry -- key: {0}  " + 
        "expecting value: {1} in region {2}", key, val, regionName);

      // Get entry created in Process A, verify entry is correct
      Region region = CacheHelper.GetVerifyRegion(regionName);
      if (checkNoKey)
      {
        Assert.IsFalse(region.ContainsKey(key),
          "Key should not have been found in region.");
      }
      CacheableString checkVal =
        region.Get(key) as CacheableString; // force a netsearch
      Util.Log("Got value: {0} for key {1}, expecting {2}", checkVal, key, val);
      VerifyEntry(regionName, key, val);
    }

    protected virtual void InvalidateEntry(string regionName, string key)
    {
      Util.Log("Invalidating entry -- key: {0}  in region {1}",
        key, regionName);

      // Invalidate entry, verify entry is invalidated
      Region region = CacheHelper.GetVerifyRegion(regionName);
      Assert.IsTrue(region.ContainsKey(key), "Key should have been found in region.");
      Assert.IsTrue(region.ContainsValueForKey(key), "Value should have been found in region.");
      region.LocalInvalidate(key);
      VerifyInvalid(regionName, key);
    }

    protected virtual void DestroyEntry(string regionName, string key)
    {
      Util.Log("Destroying entry -- key: {0}  in region {1}",
        key, regionName);

      // Destroy entry, verify entry is destroyed
      Region region = CacheHelper.GetVerifyRegion(regionName);
      Assert.IsTrue(region.ContainsKey(key), "Key should have been found in region.");
      region.Destroy(key);
      VerifyDestroyed(regionName, key);
    }

    #endregion

    #region Protected members

    protected string[] m_keys = { "Key-1", "Key-2", "Key-3", "Key-4",
      "Key-5" };
    protected string[] m_vals = { "Value-1", "Value-2", "Value-3", "Value-4",
      "Value-5" };
    protected string[] m_nvals = { "New Value-1", "New Value-2", "New Value-3",
      "New Value-4", "New Value-5" };

    protected string[] m_regionNames;

    #endregion

    #region Various steps for DistOps

    public virtual void CreateRegions(string[] regionNames)
    {
      CacheHelper.CreateILRegion(regionNames[0], true, true, null);
      CacheHelper.CreateILRegion(regionNames[1], false, true, null);
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions(string[] regionNames,
      string endpoints, bool clientNotification)
    {
      CacheHelper.CreateTCRegion(regionNames[0], true, true,
        null, endpoints, clientNotification);
      CacheHelper.CreateTCRegion(regionNames[1], false, true,
        null, endpoints, clientNotification);
      m_regionNames = regionNames;
    }



    public virtual void CreateTCRegions_Pool(string[] regionNames,
      string endpoints, string locators, string poolName, bool clientNotification)
    {
      CreateTCRegions_Pool(regionNames, endpoints, locators, poolName, clientNotification, false);
    }

    public virtual void CreateTCRegions_Pool(string[] regionNames,
      string endpoints, string locators, string poolName, bool clientNotification, bool ssl)
    {
      CacheHelper.CreateTCRegion_Pool(regionNames[0], true, true,
        null, endpoints, locators, poolName, clientNotification, ssl, false);
      CacheHelper.CreateTCRegion_Pool(regionNames[1], false, true,
        null, endpoints, locators, poolName, clientNotification, ssl, false);
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool2(string[] regionNames,
      string endpoints, string locators, string poolName, bool clientNotification, IPartitionResolver pr)
    {
      CreateTCRegions_Pool2(regionNames, endpoints, locators, poolName, clientNotification, false, pr);
    }

    public virtual void CreateTCRegions_Pool1(string regionName,
      string endpoints, string locators, string poolName, bool clientNotification, IPartitionResolver pr)
    {
      CreateTCRegions_Pool1(regionName, endpoints, locators, poolName, clientNotification, false, pr);
    }

    public virtual void CreateTCRegions_Pool2(string[] regionNames,
      string endpoints, string locators, string poolName, bool clientNotification, bool ssl, IPartitionResolver pr)
    {
      CacheHelper.CreateTCRegion_Pool2(regionNames[0], true, true,
        null, endpoints, locators, poolName, clientNotification, ssl, false, pr);
      CacheHelper.CreateTCRegion_Pool2(regionNames[1], false, true,
        null, endpoints, locators, poolName, clientNotification, ssl, false, pr);
      m_regionNames = regionNames;
    }

    public virtual void CreateTCRegions_Pool1(string regionName,
      string endpoints, string locators, string poolName, bool clientNotification, bool ssl, IPartitionResolver pr)
    {
      CacheHelper.CreateTCRegion_Pool1(regionName, true, true,
        null, endpoints, locators, poolName, clientNotification, ssl, false, pr);      
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

      m_chw_forFirstAppDomain = (CacheHelperWrapper)m_firstAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.CacheHelperWrapper");
      m_chw_forSecondAppDomain = (CacheHelperWrapper)m_secondAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.CacheHelperWrapper");

      m_putGetTests_forFirstAppDomain = (PutGetTestsAD)m_firstAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.PutGetTestsAD");
      m_putGetTests_forSecondAppDomain = (PutGetTestsAD)m_secondAppDomain.CreateInstanceAndUnwrap(Assembly.GetExecutingAssembly().FullName, "GemStone.GemFire.Cache.UnitTests.PutGetTestsAD");
    }

    public void CloseCacheAD()
    {
      m_chw_forFirstAppDomain.CloseCache();
      m_chw_forSecondAppDomain.CloseCache();            
    }

    //for appdomain
    public virtual void CreateTCRegions_Pool_AD(string[] regionNames,
      string endpoints, string locators, string poolName, bool clientNotification, bool ssl)
    {

      string filename = Util.DUnitLogDir + System.IO.Path.DirectorySeparatorChar + "tmp" + ".log";
      m_chw_forFirstAppDomain.SetLogFile(filename);
      m_chw_forSecondAppDomain.SetLogFile(filename);

      m_chw_forFirstAppDomain.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
   //   m_chw_forFirstAppDomain.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
      //m_regionNames = regionNames;

      try
      {
        Console.WriteLine("initializing second app domain");
       // m_chw_forSecondAppDomain.CallDistrinbutedConnect();
        m_chw_forSecondAppDomain.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
        Console.WriteLine("initializing second app domain done");
      }
      catch (Exception e)
      {
        Util.Log("initializing second app domain goty exception " + e.Message);
      }
      try
      {
     //   m_chw_forSecondAppDomain.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
      }
      catch (Exception )
      { 
      }

      CacheableHelper.RegisterBuiltinsAD();
      try
      {
        Console.WriteLine("in main process ");
        CacheHelperWrapper chw = new CacheHelperWrapper();
        chw.CreateTCRegions_Pool_AD(regionNames, endpoints, locators, poolName, clientNotification, ssl);
        Console.WriteLine("in main process done");
        //Util.LogFile = filename;
        //CacheHelper.ConnectConfig("DSNAME", null);
      }
      catch (Exception e)
      {
        Console.WriteLine("hitesh fot exception in main process " + e.Message);
      }
    }

    public virtual void SetRegionAD(String regionName)
    {
      m_putGetTests_forFirstAppDomain.SetRegion(regionName);
      m_putGetTests_forSecondAppDomain.SetRegion(regionName);
    }

    public virtual void RegisterBuiltinsAD()
    {
      m_chw_forFirstAppDomain.RegisterBuiltins();
      m_chw_forSecondAppDomain.RegisterBuiltins();
    }

    public void TestAllKeyValuePairsAD(string regionName, bool runQuery, bool pool)
    {
      

      ICollection<UInt32> registeredKeyTypeIds =
        CacheableWrapperFactory.GetRegisteredKeyTypeIds();
      ICollection<UInt32> registeredValueTypeIds =
        CacheableWrapperFactory.GetRegisteredValueTypeIds();

      m_chw_forFirstAppDomain.RegisterBuiltins();
      m_chw_forSecondAppDomain.RegisterBuiltins();
      
      foreach (UInt32 keyTypeId in registeredKeyTypeIds)
      {
        int numKeys;
        numKeys = m_putGetTests_forFirstAppDomain.InitKeys(keyTypeId, PutGetTests.NumKeys, PutGetTests.KeySize);
        numKeys = m_putGetTests_forSecondAppDomain.InitKeys(keyTypeId, PutGetTests.NumKeys, PutGetTests.KeySize);
        Type keyType = CacheableWrapperFactory.GetTypeForId(keyTypeId);

        foreach (UInt32 valueTypeId in registeredValueTypeIds)
        {
          m_putGetTests_forFirstAppDomain.InitValues(valueTypeId, numKeys, PutGetTests.ValueSize);
          m_putGetTests_forSecondAppDomain.InitValues( valueTypeId, numKeys, PutGetTests.ValueSize);
          Type valueType = CacheableWrapperFactory.GetTypeForId(valueTypeId);

          Util.Log("Starting gets/puts with keyType '{0}' and valueType '{1}'",
            keyType.Name, valueType.Name);
         // StartTimer();
          Util.Log("Running warmup task which verifies the puts.");
          PutGetStepsAD(regionName, true, runQuery, pool);
          Util.Log("End warmup task.");
          /*LogTaskTiming(client1,
            string.Format("Region:{0},Key:{1},Value:{2},KeySize:{3},ValueSize:{4},NumOps:{5}",
            regionName, keyType.Name, valueType.Name, KeySize, ValueSize, 4 * numKeys),
            4 * numKeys);
          */
          //m_chw_forFirstAppDomain.InvalidateRegion(regionName);
         // m_chw_forSecondAppDomain.InvalidateRegion(regionName);

        }
      }
    }

    public void PutGetStepsAD( string regionName, bool verifyGets, bool runQuery, bool pool)
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
          m_putGetTests_forFirstAppDomain.DoRunQuery( pool);
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
        m_putGetTests_forFirstAppDomain.DoRunQuery( pool);
      }
    }
    
    public virtual void DestroyRegions()
    {
      if (m_regionNames != null)
      {
        CacheHelper.DestroyRegion(m_regionNames[0], false, true);
        CacheHelper.DestroyRegion(m_regionNames[1], false, true);
      }
    }

    public virtual void RegisterAllKeysR0WithoutValues()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      region0.RegisterAllKeys(false, null, false, false);
    }

    public virtual void RegisterAllKeysR1WithoutValues()
    {
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      region1.RegisterAllKeys(false, null, false, false);
    }

    public virtual void StepThree()
    {
      CreateEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      CreateEntry(m_regionNames[1], m_keys[2], m_vals[2]);
    }

    public virtual void StepFour()
    {
      DoNetsearch(m_regionNames[0], m_keys[0], m_vals[0], true);
      DoNetsearch(m_regionNames[1], m_keys[2], m_vals[2], true);
      CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public virtual void StepFive(bool checkVal)
    {
      DoNetsearch(m_regionNames[0], m_keys[1], m_vals[1], true);
      DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], true);
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], checkVal);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], checkVal);
    }

    public virtual void RemoveStepFive()
    {
      Region reg0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region reg1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);

      // Try removing non-existent entry from regions, result should be false.  
      Assert.IsFalse(reg0.Remove(m_keys[4], m_vals[4]), "Result of Remove should be false, as this entry is not present in first region.");
      Assert.IsFalse(reg1.Remove(m_keys[4], m_vals[4]), "Result of Remove should be false, as this entry is not present in second region.");

      // Try removing non-existent key, but existing value from regions, result should be false.
      Assert.IsFalse(reg0.Remove(m_keys[4], m_vals[0]), "Result of Remove should be false, as this key is not present in first region.");
      Assert.IsFalse(reg1.Remove(m_keys[4], m_vals[0]), "Result of Remove should be false, as this key is not present in second region.");

      // Try removing existent key, but non-existing value from regions, result should be false.
      Assert.IsFalse(reg0.Remove(m_keys[0], m_vals[4]), "Result of Remove should be false, as this value is not present in first region.");
      Assert.IsFalse(reg1.Remove(m_keys[0], m_vals[4]), "Result of Remove should be false, as this value is not present in second region.");

      // Try removing existent key, and existing value from regions, result should be true.
      Util.Log("key0 is {0} ", m_keys[0]);
      Assert.IsTrue(reg0.Remove(m_keys[0], m_vals[0]), "Result of Remove should be true, as this entry is present in first region.");
      Assert.IsTrue(reg1.Remove(m_keys[2], m_vals[2]), "Result of Remove should be true, as this entry is present in second region.");
      Util.Log("1 key0 is {0} ", m_keys[0]);
      Assert.IsFalse(reg0.ContainsKey(m_keys[0]) , "ContainsKey should be false");
      Assert.IsFalse(reg0.ContainsKeyOnServer(m_keys[0]) , "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[2]), "ContainsKey should be false");  
      Assert.IsFalse(reg1.ContainsKeyOnServer(m_keys[2]), "ContainsKeyOnServer should be false");

      // Try removing already deleted entry from regions, result should be false, but no exception.
      Assert.IsFalse(reg0.Remove(m_keys[0], m_vals[0]), "Result of Remove should be false, as this entry is not present in first region.");
      Assert.IsFalse(reg1.Remove(m_keys[0], m_vals[0]), "Result of Remove should be false, as this entry is not present in second region.");

      // Try locally destroying already deleted entry from regions, It should result into exception.
      try
      {
        reg0.LocalDestroy(m_keys[0]);
        Assert.Fail( "local Destroy on already removed key should have thrown EntryNotFoundException" );
      } catch(EntryNotFoundException/*& ex*/)
      {
        Util.Log( "Got expected EntryNotFoundException for LocalDestroy operation on already removed entry." );
      }

      try
      {
        reg1.LocalDestroy(m_keys[0]);
        Assert.Fail( "local Destroy on already removed key should have thrown EntryNotFoundException" );
      } catch(EntryNotFoundException/*& ex*/)
      {
        Util.Log( "Got expected EntryNotFoundException for LocalDestroy operation on already removed entry." );
      } 
      Util.Log( "StepFive complete." );
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

    public virtual void RemoveStepSix()
    {
      Region reg0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region reg1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);

      reg0.Put(m_keys[1], m_nvals[1]);
      reg1.Put(m_keys[3], m_nvals[3]);
      
      // Try removing value that is present on client as well as server, result should be true.
      Assert.IsTrue(reg0.Remove(m_keys[1], m_nvals[1]) , "Result of Remove should be true, as this value is present locally, & also present on server.");
      Assert.IsTrue(reg1.Remove(m_keys[3], m_nvals[3]) , "Result of Remove should be true, as this value is present locally, & also present on server.");
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg0.ContainsKeyOnServer(m_keys[1]), "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKeyOnServer(m_keys[3]), "ContainsKeyOnServer should be false");
      Util.Log("Step 6.1 complete.");

      // Try removing value that is present on client but not on server, result should be false.
      reg0.Put(m_keys[1], m_vals[1]);
      reg1.Put(m_keys[3], m_vals[3]);
      reg0.LocalPut(m_keys[1], m_nvals[1]);
      reg1.LocalPut(m_keys[3], m_nvals[3]);
      Assert.IsFalse( reg0.Remove(m_keys[1], m_nvals[1]) , "Result of Remove should be false, as this value is present locally, but not present on server." );  
      Assert.IsFalse( reg1.Remove(m_keys[3], m_nvals[3]) , "Result of Remove should be false, as this value is present locally, but not present on server." );
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg0.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKeyOnServer(m_keys[3]) , "ContainsKeyOnServer should be true");
      Util.Log("Step 6.2 complete.");

      // Try removing value that is not present on client but present on server, result should be false.
      reg0.Destroy(m_keys[1]);
      reg1.Destroy(m_keys[3]);
      reg0.Put(m_keys[1], m_vals[1]);
      reg1.Put(m_keys[3], m_vals[3]);
      reg0.LocalPut(m_keys[1], m_nvals[1]);
      reg1.LocalPut(m_keys[3], m_nvals[3]);
      Assert.IsFalse( reg0.Remove(m_keys[1], m_vals[1]) , "Result of Remove should be false, as this value is not present locally, but present only on server." );  
      Assert.IsFalse( reg1.Remove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value is not present locally, but present only on server." );
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg0.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKeyOnServer(m_keys[3]) , "ContainsKeyOnServer should be true");
      Util.Log("Step 6.3 complete.");

      // Try removing value that is invalidated on client but exists on server, result should be false.
      reg0.Destroy(m_keys[1]);
      reg1.Destroy(m_keys[3]);
      reg0.Put(m_keys[1], m_nvals[1]);
      reg1.Put(m_keys[3], m_nvals[3]); 
      reg0.Invalidate(m_keys[1]);
      reg1.Invalidate(m_keys[3]);
      Assert.IsFalse( reg0.Remove(m_keys[1], m_nvals[1]) , "Result of Remove should be false, as this value is not present locally, but present only on server." );  
      Assert.IsFalse( reg1.Remove(m_keys[3], m_nvals[3]) , "Result of Remove should be false, as this value is not present locally, but present only on server." );
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg0.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKeyOnServer(m_keys[3]) , "ContainsKeyOnServer should be true");
      Util.Log("Step 6.4 complete.");

      // Try removing null value, that is invalidated on client but exists on the server, result should be false.
      reg0.Destroy(m_keys[1]);
      reg1.Destroy(m_keys[3]);
      reg0.Put(m_keys[1], m_vals[1]);
      reg1.Put(m_keys[3], m_vals[3]);
      reg0.LocalInvalidate(m_keys[1]);
      reg1.LocalInvalidate(m_keys[3]);
      Assert.IsFalse( reg0.Remove(m_keys[1], null) , "Result of Remove should be false, as this value is not present locally, but present only on server." );  
      Assert.IsFalse( reg1.Remove(m_keys[3], null) , "Result of Remove should be false, as this value is not present locally, but present only on server." );
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg0.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");  
      Assert.IsTrue(reg1.ContainsKeyOnServer(m_keys[3]) , "ContainsKeyOnServer should be true");
      Util.Log("Step 6.5 complete.");

      // Try removing a entry (value) which is not present on client as well as server, result should be false.
      Assert.IsFalse( reg0.Remove("NewKey1", "NewValue1") , "Result of Remove should be false, as this value is not present locally, and not present on server." );  
      Assert.IsFalse( reg1.Remove("NewKey3", "NewValue3") , "Result of Remove should be false, as this value is not present locally, and not present on server." );
      Assert.IsFalse(reg0.ContainsKey("NewKey1") , "ContainsKey should be false");
      Assert.IsFalse(reg0.ContainsKeyOnServer("NewKey1"), "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg1.ContainsKey("NewKey3") , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKeyOnServer("NewKey3"), "ContainsKeyOnServer should be false");
      Util.Log("Step 6.6 complete.");
      
      // Try removing a entry with a null value, which is not present on client as well as server, result should be false.
      Assert.IsFalse( reg0.Remove("NewKey1", null) , "Result of Remove should be false, as this value is not present locally, and not present on server." );  
      Assert.IsFalse( reg1.Remove("NewKey3", null) , "Result of Remove should be false, as this value is not present locally, and not present on server." );
      Assert.IsFalse(reg0.ContainsKey("NewKey1") , "ContainsKey should be false");
      Assert.IsFalse(reg0.ContainsKeyOnServer("NewKey1"), "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg1.ContainsKey("NewKey3") , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKeyOnServer("NewKey3"), "ContainsKeyOnServer should be false");
      Util.Log("Step 6.7 complete.");
      
      // Try removing a entry (value) which is not present on client but exists on the server, result should be true.
      reg0.Destroy(m_keys[1]);
      reg1.Destroy(m_keys[3]);
      reg0.Put(m_keys[1], m_nvals[1]);
      reg1.Put(m_keys[3], m_nvals[3]);
      reg0.LocalDestroy(m_keys[1]);
      reg1.LocalDestroy(m_keys[3]);
      Assert.IsTrue( reg0.Remove(m_keys[1], m_nvals[1]) , "Result of Remove should be true, as this value does not exist locally, but exists on server." );  
      Assert.IsTrue( reg1.Remove(m_keys[3], m_nvals[3]) , "Result of Remove should be true, as this value does not exist locally, but exists on server." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg0.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");  
      Assert.IsFalse(reg1.ContainsKeyOnServer(m_keys[3]) , "ContainsKeyOnServer should be false");
      Util.Log( "Step6.8 complete." );

      reg0.Put(m_keys[1], m_nvals[1]);
      reg1.Put(m_keys[3], m_nvals[3]);
      reg0.Destroy(m_keys[1]);
      reg1.Destroy(m_keys[3]);
      Assert.IsFalse( reg0.Remove(m_keys[1], null) , "Result of Remove should be false, as this value does not exist locally, but exists on server." );  
      Assert.IsFalse( reg1.Remove(m_keys[3], null) , "Result of Remove should be false, as this value does not exist locally, but exists on server." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg0.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");  
      Assert.IsFalse(reg1.ContainsKeyOnServer(m_keys[3]) , "ContainsKeyOnServer should be false");
      Util.Log( "Step6.8.1 complete." );

      // Try locally removing an entry which is locally destroyed with a NULL.
      reg0.Put(m_keys[1], m_vals[1]);
      reg1.Put(m_keys[3], m_vals[3]);
      Assert.IsTrue( reg0.Remove(m_keys[1], m_vals[1]) , "Result of Remove should be true, as this value does not exists locally." );  
      Assert.IsFalse( reg0.Remove(m_keys[1], m_vals[1]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsTrue( reg1.Remove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.Remove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value does not exists locally." );
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");
      Util.Log( "Step6.8.2 complete." );

      //-------------------------------------LocalRemove Testcases------------------------------------------------
      // Try locally removing an entry (value) which is present on the client.
      reg0.Destroy(m_keys[1]);
      reg1.Destroy(m_keys[3]);
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      Assert.IsTrue( reg0.LocalRemove(m_keys[1], m_vals[1]) , "Result of Remove should be true, as this value exists locally." );  
      Assert.IsTrue( reg1.LocalRemove(m_keys[3], m_vals[3]) , "Result of Remove should be true, as this value exists locally." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");
      Util.Log( "Step6.9 complete." );

      // Try local Destroy on entry that is already removed, should get an exception.
      try
      {
        reg0.LocalDestroy(m_keys[1]);
        Assert.Fail( "local Destroy on already removed key should have thrown EntryNotFoundException" );
      } catch(EntryNotFoundException/*& ex*/)
      {
        Util.Log( "Got expected EntryNotFoundException for LocalDestroy operation on already removed entry." );
      }
      try
      {
        reg1.LocalDestroy(m_keys[3]);
        Assert.Fail( "local Destroy on already removed key should have thrown EntryNotFoundException" );
      } catch(EntryNotFoundException/*& ex*/)
      {
        Util.Log( "Got expected EntryNotFoundException for LocalDestroy operation on already removed entry." );
      }
      Util.Log( "Step6.10 complete." );

      // Try locally removing an entry (value) which is not present on the client (value mismatch).
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      Assert.IsFalse( reg0.LocalRemove(m_keys[1], m_nvals[1]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.LocalRemove(m_keys[3], m_nvals[3]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Util.Log( "Step6.11 complete." );

      // Try locally removing an entry (value) which is invalidated with a value.
      reg0.LocalDestroy(m_keys[1]);
      reg1.LocalDestroy(m_keys[3]);
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      reg0.Invalidate(m_keys[1]);
      reg1.Invalidate(m_keys[3]);
      Assert.IsFalse( reg0.LocalRemove(m_keys[1], m_vals[1]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.LocalRemove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Util.Log( "Step6.12 complete." );

      // Try locally removing an entry (value) which is invalidated with a NULL.
      reg0.LocalDestroy(m_keys[1]);
      reg1.LocalDestroy(m_keys[3]);
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      reg0.Invalidate(m_keys[1]);
      reg1.Invalidate(m_keys[3]);
      Assert.IsTrue( reg0.LocalRemove(m_keys[1], null) , "Result of Remove should be true, as this value does not exists locally." );  
      Assert.IsTrue( reg1.LocalRemove(m_keys[3], null) , "Result of Remove should be true, as this value does not exists locally." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");
      Util.Log( "Step6.13 complete." );

      // Try locally removing an entry (value) with a NULL.
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      Assert.IsFalse( reg0.LocalRemove(m_keys[1], null) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.LocalRemove(m_keys[3], null) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsTrue(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsTrue(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Util.Log( "Step6.14 complete." );

      // Try locally removing an entry which is locally destroyed with a value.
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      reg0.LocalDestroy(m_keys[1]);
      reg1.LocalDestroy(m_keys[3]);
      Assert.IsFalse( reg0.LocalRemove(m_keys[1], m_vals[1]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.LocalRemove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be true");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be true");
      Util.Log( "Step6.15 complete." );

      // Try locally removing an entry which is locally destroyed with a NULL.
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      reg0.LocalDestroy(m_keys[1]);
      reg1.LocalDestroy(m_keys[3]);
      Assert.IsFalse( reg0.LocalRemove(m_keys[1], null) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.LocalRemove(m_keys[3], null) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");
      Util.Log( "Step6.16 complete." );

      // Try locally removing an entry which is already removed.
      reg0.LocalPut(m_keys[1], m_vals[1]);
      reg1.LocalPut(m_keys[3], m_vals[3]);
      Assert.IsTrue( reg0.LocalRemove(m_keys[1], m_vals[1]) , "Result of Remove should be true, as this value does not exists locally." );  
      Assert.IsFalse( reg0.LocalRemove(m_keys[1], m_vals[1]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsTrue( reg1.LocalRemove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse( reg1.LocalRemove(m_keys[3], m_vals[3]) , "Result of Remove should be false, as this value does not exists locally." );  
      Assert.IsFalse(reg0.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg1.ContainsKey(m_keys[3]) , "ContainsKey should be false");
      Util.Log( "Step6.17 complete." );
      // Try locally removing an entry when region scope is not null.

      Util.Log( "StepSix complete." );
    }

    public virtual void StepSeven()
    {
      VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3]);
      InvalidateEntry(m_regionNames[0], m_keys[0]);
      InvalidateEntry(m_regionNames[1], m_keys[2]);
    }

    public virtual void RemoveStepEight()
    {
      Region reg = CacheHelper.GetVerifyRegion("exampleRegion");

      // Try removing a entry which is present on client (value) but invalidated on the server, result should be false.
      reg.Destroy(m_keys[0]);
      reg.Destroy(m_keys[1]);
      reg.Put( m_keys[0], m_vals[0]);
      reg.Put( m_keys[1], m_vals[1]);
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsFalse(reg.Remove(m_keys[0], m_vals[0]) , "Result of Remove should be false, as this value is present locally, but not present on server.");
      Assert.IsFalse(reg.Remove(m_keys[1], m_vals[1]) , "Result of Remove should be false, as this value is present locally, but not present on server.");
      Assert.IsTrue(reg.ContainsKey(m_keys[0]) , "ContainsKey should be true");
      Assert.IsTrue(reg.ContainsKeyOnServer(m_keys[0]) , "ContainsKeyOnServer should be true");
      Assert.IsTrue(reg.ContainsKey(m_keys[1]) , "ContainsKey should be true");  
      Assert.IsTrue(reg.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be true");
      Util.Log( "Step 8.1 complete." );

      // Try removing a entry that is not present on client, but invalidated on server with null value, result should be true.
      reg.Destroy(m_keys[0]);
      reg.Destroy(m_keys[1]);
      reg.Put(m_keys[0], m_nvals[0]);
      reg.Put(m_keys[1], m_nvals[1]);
      reg.LocalDestroy(m_keys[0]);
      reg.LocalDestroy(m_keys[1]);
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsTrue( reg.Remove(m_keys[0], null) , "Result of Remove should be true, as this value is not present locally, & not present on server." );  
      Assert.IsTrue( reg.Remove(m_keys[1], null) , "Result of Remove should be true, as this value is not present locally, & not present on server." );      
      Assert.IsFalse(reg.ContainsKeyOnServer(m_keys[0]) , "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg.ContainsKey(m_keys[0]) , "ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be false");
      Util.Log( "Step 8.2 complete." );

      // Try removing a entry with a (value) on client that is invalidated on server with null , result should be false.
      reg.Destroy(m_keys[0]);
      reg.Destroy(m_keys[1]);
      reg.Put(m_keys[0], m_nvals[0]);
      reg.Put(m_keys[1], m_nvals[1]);
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsFalse(reg.Remove(m_keys[0], null) , "Result of Remove should be false, as this value is present locally, & not present on server.");
      Assert.IsFalse(reg.Remove(m_keys[1], null) , "Result of Remove should be false, as this value is present locally, & not present on server.");
      Assert.IsTrue(reg.ContainsKey(m_keys[0]) , "ContainsKey should be true");
      Assert.IsTrue(reg.ContainsKeyOnServer(m_keys[0]) , "ContainsKeyOnServer should be true");
      Assert.IsTrue(reg.ContainsKey(m_keys[1]) , "ContainsKey should be true");  
      Assert.IsTrue(reg.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be true");
      Util.Log( "Step 8.3 complete." );

      // Try removing a entry with a entry that is invalidated on the client as well as on server with a null value, result should be true.
      reg.Destroy(m_keys[0]);
      reg.Destroy(m_keys[1]);
      reg.Put(m_keys[0], m_nvals[0]);
      reg.Put(m_keys[1], m_nvals[1]);
      reg.Invalidate(m_keys[0]);
      reg.Invalidate(m_keys[1]);
      Thread.Sleep(10000); //This is for expiration on server to execute.
      Assert.IsTrue( reg.Remove(m_keys[0], null) , "Result of Remove should be true, as this value is not present locally, & not present on server." );  
      Assert.IsTrue( reg.Remove(m_keys[1], null) , "Result of Remove should be true, as this value is not present locally, & not present on server." );
      Assert.IsFalse(reg.ContainsKey(m_keys[0]) , "ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKeyOnServer(m_keys[0]) , "ContainsKeyOnServer should be false");
      Assert.IsFalse(reg.ContainsKey(m_keys[1]) , "ContainsKey should be false");
      Assert.IsFalse(reg.ContainsKeyOnServer(m_keys[1]) , "ContainsKeyOnServer should be false");

      Util.Log("RemoveStepEight complete.");
    }

    public virtual void StepEight()
    {
      VerifyInvalid(m_regionNames[0], m_keys[0]);
      VerifyInvalid(m_regionNames[1], m_keys[2]);
      InvalidateEntry(m_regionNames[0], m_keys[1]);
      InvalidateEntry(m_regionNames[1], m_keys[3]);
    }

    public virtual void StepNine()
    {
      VerifyInvalid(m_regionNames[0], m_keys[1]);
      VerifyInvalid(m_regionNames[1], m_keys[3]);
      DestroyEntry(m_regionNames[0], m_keys[0]);
      DestroyEntry(m_regionNames[1], m_keys[2]);
    }

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

    #endregion
  }
}

//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientHARegionTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2, m_client3;
    private string[] m_regexes = { "Key.*1", "Key.*2", "Key.*3", "Key.*4" };

    private static string QueryRegionName = "Portfolios";

    #endregion

    protected override string ExtraPropertiesFile
    {
      get
      {
        return "gfcpp.properties.mixed";
      }
    }

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);
        m_client3.Call(CacheHelper.Close);
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    #region Various steps for HA tests

    public void InitClient(string endpoints, int redundancyLevel)
    {
      CacheHelper.InitConfig(endpoints, redundancyLevel);
    }

    public void InitClient_Pool(string endpoints, string locators, int redundancyLevel)
    {
      CacheHelper.CreatePool("__TESTPOOL1_", endpoints, locators, null, redundancyLevel, true);
    }

    public void InitClientForEventId(string endpoints, int redundancyLevel, int ackInterval, int dupCheckLife)
    {
      CacheHelper.InitConfigForEventId(endpoints, redundancyLevel, ackInterval, dupCheckLife);
    }

    public void InitClientForEventId_Pool(string endpoints, string locators, bool notification,
      int redundancyLevel, int ackInterval, int dupCheckLife)
    {
      CacheHelper.Init();
      CacheHelper.CreatePool("__TESTPOOL1_", endpoints, locators, null,
        redundancyLevel, notification, ackInterval, dupCheckLife);
    }

    public void InitClientXml(string cacheXml)
    {
      CacheHelper.InitConfig(cacheXml);
    }

    public void InitClientXml(string cacheXml, int serverport1, int serverport2)
    {
      CacheHelper.HOST_PORT_1 = serverport1;
      CacheHelper.HOST_PORT_2 = serverport2;
      CacheHelper.InitConfig(cacheXml);
    }
    
    public void CreateEntriesForEventId(int sleep)
    {
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region2 = CacheHelper.GetVerifyRegion(m_regionNames[1]);

      for (int value = 1; value <= 100; value++)
      {
        region1.Put(m_keys[0], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region1.Put(m_keys[1], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region1.Put(m_keys[2], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region1.Put(m_keys[3], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region2.Put(m_keys[0], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region2.Put(m_keys[1], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region2.Put(m_keys[2], new CacheableInt32(value));
        Thread.Sleep(sleep);
        region2.Put(m_keys[3], new CacheableInt32(value));
        Thread.Sleep(sleep);
      }
    }

    public void CheckClientForEventId()
    {
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region2 = CacheHelper.GetVerifyRegion(m_regionNames[1]);

      DupListener checker1 = region1.Attributes.CacheListener as DupListener;
      DupListener checker2 = region2.Attributes.CacheListener as DupListener;

      Util.Log("Validating checker1 cachelistener");
      checker1.validate();
      Util.Log("Validating checker2 cachelistener");
      checker2.validate();
    }

    public void InitDupListeners()
    {
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region2 = CacheHelper.GetVerifyRegion(m_regionNames[1]);

      region1.GetAttributesMutator().SetCacheListener(DupListener.Create());
      region2.GetAttributesMutator().SetCacheListener(DupListener.Create());
      
      Thread.Sleep(5000);
      
      region1.RegisterAllKeys();
      region2.RegisterAllKeys();
    }

    public void CreateHATCRegions(string[] regionNames, bool useList,
      string endpoints, string locators, bool clientNotification, bool pool, bool create)
    {
      if (create)
      {
        if (pool)
        {
          CacheHelper.CreateTCRegion_Pool(regionNames[0], true, true,
            null, endpoints, locators, "__TESTPOOL1_", clientNotification);
          CacheHelper.CreateTCRegion_Pool(regionNames[1], false, true,
            null, endpoints, locators, "__TESTPOOL1_", clientNotification);
        }
        else
        {
          CacheHelper.CreateTCRegion(regionNames[0], true, true,
            null, endpoints, clientNotification);
          CacheHelper.CreateTCRegion(regionNames[1], false, true,
            null, endpoints, clientNotification);
        }
      }
      m_regionNames = regionNames;
    }

    public void CreateCPPRegion(string regionName)
    {
      CacheHelper.CreateTCRegion(regionName, true, true,
        null, "none", false);
    }

    public void CreateMixedEntry(string regionName, string key, string val)
    {
      CreateEntry(regionName, key, val);
    }

    public void DoNetsearchMixed(string regionName, string key, string val, bool checkNoKey)
    {
      DoNetsearch(regionName, key, val, checkNoKey);
    }

    public void UpdateEntryMixed(string regionName, string key, string val, bool checkVal)
    {
      UpdateEntry(regionName, key, val, checkVal);
    }

    public void LocalDestroyEntry(string regionName, string key)
    {
      Util.Log("Locally Destroying entry -- key: {0}  in region {1}",
        key, regionName);

      // Destroy entry, verify entry is destroyed
      Region region = CacheHelper.GetVerifyRegion(regionName);
      Assert.IsTrue(region.ContainsKey(key), "Key should have been found in region.");
      region.LocalDestroy(key);
      VerifyDestroyed(regionName, key);
    }

    public void Create2Vals(bool even)
    {
      if (even)
      {
        CreateEntry(m_regionNames[0], m_keys[0], m_vals[0]);
        CreateEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      }
      else
      {
        CreateEntry(m_regionNames[0], m_keys[1], m_vals[1]);
        CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
      }
    }

    public void CreateVals()
    {
      Create2Vals(true);
      Create2Vals(false);
    }

    public void VerifyValCreations(bool even)
    {
      if (even)
      {
        VerifyCreated(m_regionNames[0], m_keys[0]);
        VerifyCreated(m_regionNames[1], m_keys[2]);
      }
      else
      {
        VerifyCreated(m_regionNames[0], m_keys[1]);
        VerifyCreated(m_regionNames[1], m_keys[3]);
      }
    }

    public void UpdateVals()
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], true);
      UpdateEntry(m_regionNames[0], m_keys[1], m_vals[1], true);
      UpdateEntry(m_regionNames[1], m_keys[2], m_vals[2], true);
      UpdateEntry(m_regionNames[1], m_keys[3], m_vals[3], true);
    }

    public void Update2NVals(bool even, bool checkVal)
    {
      if (even)
      {
        UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], checkVal);
        UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], checkVal);
      }
      else
      {
        UpdateEntry(m_regionNames[0], m_keys[1], m_nvals[1], checkVal);
        UpdateEntry(m_regionNames[1], m_keys[3], m_nvals[3], checkVal);
      }
    }

    public void UpdateNVals(bool checkVal)
    {
      Update2NVals(true, checkVal);
      Update2NVals(false, checkVal);
    }

    public void Verify2Vals(bool even)
    {
      if (even)
      {
        VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
        VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      }
      else
      {
        VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
        VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);
      }
    }

    public void VerifyVals()
    {
      Verify2Vals(true);
      Verify2Vals(false);
    }

    public void Verify2NVals(bool even)
    {
      if (even)
      {
        VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
        VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
      }
      else
      {
        VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
        VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3]);
      }
    }

    public void DoNetsearch2Vals(bool even)
    {
      if (even)
      {
        DoNetsearch(m_regionNames[0], m_keys[0], m_vals[0], true);
        DoNetsearch(m_regionNames[1], m_keys[2], m_vals[2], true);
      }
      else
      {
        DoNetsearch(m_regionNames[0], m_keys[1], m_vals[1], true);
        DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], true);
      }
    }

    public void DoNetsearchVals()
    {
      DoNetsearch2Vals(true);
      DoNetsearch2Vals(false);
    }

    public void VerifyNVals()
    {
      Verify2NVals(true);
      Verify2NVals(false);
    }

    public void VerifyNValsVals()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], true);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1], true);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2], true);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3], true);
    }

    public void VerifyNValVals()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], true);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1], true);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2], true);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3], true);
    }

    public void VerifyValsNVals()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0], true);
      VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1], true);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2], true);
      VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3], true);
    }

    public void VerifyMixedNVals()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], true);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1], true);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2], true);
      VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3], true);
    }

    public void RegisterKeysException(string key0, string key1)
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      if (key0 != null)
      {
          region0.RegisterKeys(new CacheableKey[] {
            new CacheableString(key0) });
      }
      if (key1 != null)
      {
          region1.RegisterKeys(new CacheableKey[] {
            new CacheableString(key1) });
      }
    }

    public void RegisterRegexesException(string regex0, string regex1)
    {
      if (regex0 != null)
      {
        Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
          region0.RegisterRegex(regex0);
      }
      if (regex1 != null)
      {
        Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
          region1.RegisterRegex(regex1);
      }
    }

    public void DistOpsCommonSteps(bool clientNotification, bool pool)
    {
      DistOpsCommonSteps(clientNotification, pool, true);
    }

    public void DistOpsCommonSteps(bool clientNotification, bool pool, bool createRegions)
    {
      m_client1.Call(CreateHATCRegions, RegionNames, false,
        (string)null, (string)null, clientNotification, pool, createRegions);
      m_client1.Call(Create2Vals, true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, CacheHelper.Locators, !clientNotification, pool, createRegions);
      m_client2.Call(Create2Vals, false);
      Util.Log("StepTwo complete.");

      m_client1.Call(DoNetsearch2Vals, false);
      m_client1.Call(RegisterKeys, m_keys[1], m_keys[3]);
      Util.Log("StepThree complete.");

      m_client2.Call(CheckServerKeys);
      m_client2.Call(DoNetsearch2Vals, true);
      m_client2.Call(RegisterKeys, m_keys[0], m_keys[2]);
      Util.Log("StepFour complete.");

      m_client1.Call(Update2NVals, true, true);
      Util.Log("StepFive complete.");

      m_client2.Call(Verify2NVals, true);
      m_client2.Call(Update2NVals, false, true);
      Util.Log("StepSix complete.");

      m_client1.Call(Verify2NVals, false);
      Util.Log("StepSeven complete.");
    }

    public void FailoverCommonSteps(int redundancyLevel, bool useRegexes, bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml",
          "cacheserver_notify_subscription3.xml");

        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");

        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");
      }
      else
      {
        CacheHelper.SetupJavaServers(false,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml",
          "cacheserver_notify_subscription3.xml");

        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");
      }

      if (pool)
      {
        if (locator)
        {
          m_client1.Call(InitClient_Pool, (string)null, CacheHelper.Locators, redundancyLevel);
        }
        else
        {
          m_client1.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, redundancyLevel);
        }
      }
      else
      {
        m_client1.Call(InitClient, CacheHelper.Endpoints, redundancyLevel);
      }
      m_client1.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, useRegexes, pool, true);
      Util.Log("StepOne complete.");

      if (pool)
      {
        if (locator)
        {
          m_client2.Call(InitClient_Pool, (string)null, CacheHelper.Locators, redundancyLevel);
        }
        else
        {
          m_client2.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, redundancyLevel);
        }
      }
      else
      {
        m_client2.Call(InitClient, CacheHelper.Endpoints, redundancyLevel);
      }
      m_client2.Call(CreateHATCRegions, RegionNames, false,
        (string)null, (string)null, !useRegexes, pool, true);
      Util.Log("StepTwo complete.");

      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      if (redundancyLevel > 1)
      {
        if (useRegexes)
        {
          m_client2.Call(RegisterRegexesException, m_regexes[0], m_regexes[2]);
        }
        else
        {
          m_client2.Call(RegisterKeysException, m_keys[0], m_keys[2]);
        }
      }
      else
      {
        if (useRegexes)
        {
          m_client2.Call(RegisterRegexes, m_regexes[0], m_regexes[2]);
        }
        else
        {
          m_client2.Call(RegisterKeys, m_keys[0], m_keys[2]);
        }
      }
      Util.Log("RegisterKeys done.");

      m_client1.Call(CreateVals);
      Util.Log("StepThree complete.");

      m_client2.Call(VerifyValCreations, true);
      m_client2.Call(Verify2Vals, true);
      m_client2.Call(DoNetsearch2Vals, false);
      Util.Log("StepFour complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      //For Failover to complete.
      Thread.Sleep(5000);

      m_client1.Call(CheckServerKeys);
      m_client1.Call(UpdateNVals, true);
      Thread.Sleep(1000);
      Util.Log("StepFive complete.");

      m_client2.Call(VerifyNValsVals);
      Util.Log("StepSix complete.");

      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      //For Failover to complete.
      Thread.Sleep(5000);

      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(3, "GFECS3");
      }
      Util.Log("Cacheserver 3 started.");

      m_client1.Call(UpdateVals);
      Thread.Sleep(1000);
      Util.Log("StepSeven complete.");

      m_client2.Call(VerifyVals);
      if (useRegexes)
      {
        m_client2.Call(UnregisterRegexes, (string)null, m_regexes[2]);
      }
      else
      {
        m_client2.Call(UnregisterKeys, (string)null, m_keys[2]);
      }
      Util.Log("StepEight complete.");

      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      m_client1.Call(UpdateNVals, true);
      Thread.Sleep(1000);
      Util.Log("StepNine complete.");

      m_client2.Call(VerifyNValVals);
      Util.Log("StepTen complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      //For Failover to complete.
      Thread.Sleep(5000);

      m_client1.Call(UpdateVals);
      Thread.Sleep(1000);
      Util.Log("StepEleven complete.");

      m_client2.Call(VerifyVals);
      Util.Log("StepTwelve complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void KillServer()
    {
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    public delegate void KillServerDelegate();

    public void StepOneFailover(bool pool, bool locator)
    {
      // This is here so that Client1 registers information of the cacheserver
      // that has been already started
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true,
          "cacheserver_remoteoql.xml",
          "cacheserver_remoteoql2.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        //CacheHelper.StartJavaServer(1, "GFECS1");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false,
          "cacheserver_remoteoql.xml",
          "cacheserver_remoteoql2.xml");
        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      try
      {
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);
      }
      catch (IllegalStateException)
      {
        // ignored since we run multiple iterations for pool and non pool configs
      }

      if (pool)
      {
        if (locator)
        {
          InitClient_Pool((string)null, CacheHelper.Locators, 1);
        }
        else
        {
          InitClient_Pool(CacheHelper.Endpoints, (string)null, 1);
        }
        CacheHelper.CreateTCRegion_Pool(QueryRegionName, true, true,
            null, null, null, "__TESTPOOL1_", true);
      }
      else
      {
        InitClient(CacheHelper.Endpoints, 1);
        CacheHelper.CreateTCRegion(QueryRegionName, true, true,
          null, null, true);
      }

      Region region = CacheHelper.GetVerifyRegion(QueryRegionName);
      Portfolio port1 = new Portfolio(1, 100);
      Portfolio port2 = new Portfolio(2, 200);
      Portfolio port3 = new Portfolio(3, 300);
      Portfolio port4 = new Portfolio(4, 400);

      region.Put("1", port1);
      region.Put("2", port2);
      region.Put("3", port3);
      region.Put("4", port4);
    }

    public void StepTwoFailover(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      IAsyncResult killRes = null;
      KillServerDelegate ksd = new KillServerDelegate(KillServer);

      QueryService qs = null;
      if (pool)
      {
        qs = PoolManager.Find("__TESTPOOL1_").GetQueryService();
      }
      else
      {
        qs = CacheHelper.DCache.GetQueryService();
      }
      for (int i = 0; i < 10000; i++)
      {
        Query qry = qs.NewQuery("select distinct * from /" + QueryRegionName);

        ISelectResults results = qry.Execute();

        if (i == 10)
        {
          Util.Log("Starting the kill server thread.");
          killRes = ksd.BeginInvoke(null, null);
        }

        Int32 resultSize = results.Size;

        if (i % 100 == 0)
        {
          Util.Log("Iteration upto {0} done, result size is {1}", i, resultSize);
        }

        Assert.AreEqual(4, resultSize, "Result size is not 4!");
      }

      killRes.AsyncWaitHandle.WaitOne();
      ksd.EndInvoke(killRes);
    }

    #endregion

    void runDistOps(bool pool, bool locator)
    {
      Util.Log("runDistOps: pool flag is {0}, locator flag is {1}", pool, locator);

      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaLocator(1, "GFELOC");

        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
      }

      if (pool)
      {
        if (locator)
        {
          m_client1.Call(InitClient_Pool, (string)null, CacheHelper.Locators, 1);
          m_client2.Call(InitClient_Pool, (string)null, CacheHelper.Locators, 1);
          m_client1.Call(CreateNonExistentRegion, (string)null, CacheHelper.Locators, pool);
        }
        else
        {
          m_client1.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, 1);
          m_client2.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, 1);
          m_client1.Call(CreateNonExistentRegion, CacheHelper.Endpoints, (string)null, pool);
        }
      }
      else
      {
        m_client1.Call(InitClient, CacheHelper.Endpoints, 1);
        m_client2.Call(InitClient, CacheHelper.Endpoints, 1);
        m_client1.Call(CreateNonExistentRegion, CacheHelper.Endpoints, (string)null, pool);
      }      

      DistOpsCommonSteps(true, pool);

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator 1 stopped.");
      }
    }

    void runDistOpsXml(bool pool, bool locator)
    {
      Util.Log("runDistOpsXml: pool flag is {0}, locator flag is {1}", pool, locator);

      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaLocator(1, "GFELOC");

        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      }
      else
      {
        CacheHelper.SetupJavaServers(false, "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
      }

      if (pool)
      {
        m_client1.Call(InitClientXml, "client_pool.xml", CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2);
        m_client2.Call(InitClientXml, "client_pool.xml", CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2);
      }
      else
      {
        m_client1.Call(InitClientXml, "cache_redundancy.xml", CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2);
        m_client2.Call(InitClientXml, "cache_redundancy.xml", CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2);
      }

      DistOpsCommonSteps(false, pool, !pool);

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }
    }

    void runMixedRedundancy(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml",
          "cacheserver_notify_subscription3.xml",
          "cacheserver_notify_subscription4.xml");

        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");

        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");

        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        Util.Log("Cacheserver 3 started.");
      }
      else
      {
        CacheHelper.SetupJavaServers(false,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml",
          "cacheserver_notify_subscription3.xml",
          "cacheserver_notify_subscription4.xml");

        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");

        CacheHelper.StartJavaServer(3, "GFECS3");
        Util.Log("Cacheserver 3 started.");
      }

      if (pool)
      {
        if (locator)
        {
          m_client1.Call(InitClient_Pool, (string)null, CacheHelper.Locators, 0);
        }
        else
        {
          m_client1.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, 0);
        }
      }
      else
      {
        m_client1.Call(InitClient, CacheHelper.Endpoints, 0);
      }
      m_client1.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, true, pool, true);
      m_client1.Call(RegisterKeys, m_keys[0], m_keys[2]);
      m_client1.Call(CreateVals);
      Util.Log("StepOne complete.");

      if (pool)
      {
        if (locator)
        {
          m_client2.Call(InitClient_Pool, (string)null, CacheHelper.Locators, 1);
        }
        else
        {
          m_client2.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, 1);
        }
      }
      else
      {
        m_client2.Call(InitClient, CacheHelper.Endpoints, 1);
      }
      m_client2.Call(CreateHATCRegions, RegionNames, false,
        (string)null, (string)null, true, pool, true);
      m_client2.Call(RegisterKeys, m_keys[1], m_keys[3]);
      m_client2.Call(DoNetsearchVals);
      Util.Log("StepTwo complete.");

      if (pool)
      {
        if (locator)
        {
          m_client3.Call(InitClient_Pool, (string)null, CacheHelper.Locators, 2);
        }
        else
        {
          m_client3.Call(InitClient_Pool, CacheHelper.Endpoints, (string)null, 2);
        }
      }
      else
      {
        m_client3.Call(InitClient, CacheHelper.Endpoints, 2);
      }
      m_client3.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, false, pool, true);
      m_client3.Call(RegisterKeys, m_keys[0], m_keys[3]);
      m_client3.Call(DoNetsearchVals);
      Util.Log("StepThree complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(4, "GFECS4", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(4, "GFECS4");
      }
      Util.Log("Cacheserver 4 started.");

      // Update from client1 and check on client2 and client3

      // no check for value because the region may have been invalidated
      // after a failover.
      m_client1.Call(UpdateNVals, false);
      Thread.Sleep(1000);
      Util.Log("StepFour complete.");

      m_client2.Call(VerifyValsNVals);
      Util.Log("StepFive complete.");

      m_client3.Call(VerifyMixedNVals);
      Util.Log("StepSix complete.");

      // Update from client2 and check on client1 and client3

      m_client2.Call(UpdateVals);
      Util.Log("StepSeven complete.");

      m_client1.Call(VerifyValsNVals);
      Util.Log("StepEight complete.");

      m_client3.Call(VerifyVals);
      Util.Log("StepNine complete.");

      // Update from client3 and check on client1 and client2

      m_client3.Call(UpdateNVals, true);
      Util.Log("StepTen complete.");

      m_client1.Call(VerifyNVals);
      Util.Log("StepEleven complete.");

      m_client2.Call(VerifyValsNVals);
      Util.Log("StepTwelve complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);
      m_client3.Call(Close);

      // Stop the servers

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaServer(3);
      Util.Log("Cacheserver 3 stopped.");

      CacheHelper.StopJavaServer(4);
      Util.Log("Cacheserver 4 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runQueryFailover(bool pool, bool locator)
    {
      try
      {
        m_client1.Call(StepOneFailover, pool, locator);
        Util.Log("StepOneFailover complete.");

        m_client1.Call(StepTwoFailover, pool, locator);
        Util.Log("StepTwoFailover complete.");

        m_client1.Call(Close);
        Util.Log("Client closed");
      }
      finally
      {
        m_client1.Call(CacheHelper.StopJavaServers);
        if (pool && locator)
        {
          m_client1.Call(CacheHelper.StopJavaLocator, 1);
          Util.Log("Locator stopped");
        }
      }
    }

    void runPeriodicAck(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");

        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        Util.Log("Cacheserver 2 started.");
      }
      else
      {
        CacheHelper.SetupJavaServers(false,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaServer(1, "GFECS1");
        Util.Log("Cacheserver 1 started.");

        CacheHelper.StartJavaServer(2, "GFECS2");
        Util.Log("Cacheserver 2 started.");
      }

      if (pool)
      {
        if (locator)
        {
          m_client1.Call(InitClientForEventId_Pool, (string)null, CacheHelper.Locators, false, 1, 10, 30);
        }
        else
        {
          m_client1.Call(InitClientForEventId_Pool, CacheHelper.Endpoints, (string)null, false, 1, 10, 30);
        }
      }
      else
      {
        m_client1.Call(InitClientForEventId, CacheHelper.Endpoints, 1, 10, 30);
      }
      m_client1.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, false, pool, true);
      Util.Log("StepOne complete.");

      if (pool)
      {
        if (locator)
        {
          m_client2.Call(InitClientForEventId_Pool, (string)null, CacheHelper.Locators, true, 1, 10, 30);
        }
        else
        {
          m_client2.Call(InitClientForEventId_Pool, CacheHelper.Endpoints, (string)null, true, 1, 10, 30);
        }
      }
      else
      {
        m_client2.Call(InitClientForEventId, CacheHelper.Endpoints, 1, 10, 30);
      }
      m_client2.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, true, pool, true);
      m_client2.Call(InitDupListeners);
      Util.Log("StepTwo complete.");

      m_client1.Call(CreateEntriesForEventId, 50);
      Util.Log("CreateEntries complete.");

      Thread.Sleep(30000);

      m_client2.Call(CheckClientForEventId);
      Util.Log("CheckClient complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    void runEventIDMap(bool pool, bool locator)
    {
      if (pool && locator)
      {
        CacheHelper.SetupJavaServers(true,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");

        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      }
      else
      {
        CacheHelper.SetupJavaServers(false,
          "cacheserver_notify_subscription.xml",
          "cacheserver_notify_subscription2.xml");

        CacheHelper.StartJavaServer(1, "GFECS1");
      }
      Util.Log("Cacheserver 1 started.");

      if (pool)
      {
        if (locator)
        {
          m_client1.Call(InitClientForEventId_Pool, (string)null, CacheHelper.Locators, false, 1, 3600, 3600);
        }
        else
        {
          m_client1.Call(InitClientForEventId_Pool, CacheHelper.Endpoints, (string)null, false, 1, 3600, 3600);
        }
      }
      else
      {
        m_client1.Call(InitClientForEventId, CacheHelper.Endpoints, 1, 3600, 3600);
      }
      m_client1.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, false, pool, true);
      Util.Log("StepOne complete.");

      if (pool)
      {
        if (locator)
        {
          m_client2.Call(InitClientForEventId_Pool, (string)null, CacheHelper.Locators, true, 1, 3600, 3600);
        }
        else
        {
          m_client2.Call(InitClientForEventId_Pool, CacheHelper.Endpoints, (string)null, true, 1, 3600, 3600);
        }
      }
      else
      {
        m_client2.Call(InitClientForEventId, CacheHelper.Endpoints, 1, 3600, 3600);
      }
      m_client2.Call(CreateHATCRegions, RegionNames, false,
        CacheHelper.Endpoints, (string)null, true, pool, true);
      m_client2.Call(InitDupListeners);
      Util.Log("StepTwo complete.");

      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      }
      else
      {
        CacheHelper.StartJavaServer(2, "GFECS2");
      }
      Util.Log("Cacheserver 2 started.");

      m_client1.Call(CreateEntriesForEventId, 10);
      Util.Log("CreateEntries complete.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      Thread.Sleep(30000);

      m_client2.Call(CheckClientForEventId);
      Util.Log("CheckClient complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
        Util.Log("Locator stopped");
      }
    }

    [Test]
    public void DistOps()
    {
      runDistOps(false, false); // region config
      runDistOps(true, false); // pool with server endpoints
      runDistOps(true, true); // pool with locators
    }

    [Test]
    public void DistOpsXml()
    {
      runDistOpsXml(false, false); // region config
      runDistOpsXml(true, false); // pool with server endpoints
      runDistOpsXml(true, true); // pool with locators
    }

    [Test]
    public void FailoverR1()
    {
      FailoverCommonSteps(1, false, false, false); // region config
      FailoverCommonSteps(1, false, true, false); // pool with server endpoints
      FailoverCommonSteps(1, false, true, true); // pool with locators
    }

    [Test]
    public void FailoverR3()
    {
      FailoverCommonSteps(3, false, false, false); // region config
      FailoverCommonSteps(3, false, true, false); // pool with server endpoints
      FailoverCommonSteps(3, false, true, true); // pool with locators
    }

    [Test]
    public void FailoverRegexR1()
    {
      FailoverCommonSteps(1, true, false, false); // region config
      FailoverCommonSteps(1, true, true, false); // pool with server endpoints
      FailoverCommonSteps(1, true, true, true); // pool with locators
    }

    [Test]
    public void FailoverRegexR3()
    {
      FailoverCommonSteps(3, true, false, false); // region config
      FailoverCommonSteps(3, true, true, false); // pool with server endpoints
      FailoverCommonSteps(3, true, true, true); // pool with locators
    }

    [Test]
    public void MixedRedundancy()
    {
      runMixedRedundancy(false, false); // region config
      runMixedRedundancy(true, false); // pool with server endpoints
      runMixedRedundancy(true, true); // pool with locators
    }

    [Test]
    public void QueryFailover()
    {
      runQueryFailover(false, false); // region config
      runQueryFailover(true, false); // pool with server endpoints
      runQueryFailover(true, true); // pool with locators
    }
    
    [Test]
    public void PeriodicAck()
    {
      runPeriodicAck(false, false); // region config
      runPeriodicAck(true, false); // pool with server endpoints
      runPeriodicAck(true, true); // pool with locators
    }
    
    [Test]
    public void EventIDMap()
    {
      runEventIDMap(false, false); // region config
      runEventIDMap(true, false); // pool with server endpoints
      runEventIDMap(true, true); // pool with locators
    }
    
  }
}

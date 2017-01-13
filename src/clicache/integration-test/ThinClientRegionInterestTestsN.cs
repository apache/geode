//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientRegionInterestTests : ThinClientRegionSteps
  {
    #region Private members and methods

    private UnitProcess m_client1, m_client2, m_client3, m_feeder;
    private static string[] m_regexes = { "Key-*1", "Key-*2",
      "Key-*3", "Key-*4" };
    private const string m_regex23 = "Key-[23]";
    private const string m_regexWildcard = "Key-.*";
    private const int m_numUnicodeStrings = 5;

    private static string[] m_keysNonRegex = { "key-1", "key-2", "key-3" };
    private static string[] m_keysForRegex = {"key-regex-1",
      "key-regex-2", "key-regex-3" };
    private static string[] RegionNamesForInterestNotify =
      { "RegionTrue", "RegionFalse", "RegionOther" };

    string GetUnicodeString(int index)
    {
      return new string('\x0905', 40) + index.ToString("D10");
    }

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      m_feeder  = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3, m_feeder };
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
      try
      {
        m_client1.Call(DestroyRegions);
        m_client2.Call(DestroyRegions);
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    #region Steps for Thin Client IRegion<object, object> with Interest

    public void StepFourIL()
    {
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
    }

    public void StepFourRegex3()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      try
      {
        Util.Log("Registering empty regular expression.");
        region0.GetSubscriptionService().RegisterRegex(string.Empty);
        Assert.Fail("Did not get expected exception!");
      }
      catch (Exception ex)
      {
        Util.Log("Got expected exception {0}: {1}", ex.GetType(), ex.Message);
      }
      try
      {
        Util.Log("Registering null regular expression.");
        region1.GetSubscriptionService().RegisterRegex(null);
        Assert.Fail("Did not get expected exception!");
      }
      catch (Exception ex)
      {
        Util.Log("Got expected exception {0}: {1}", ex.GetType(), ex.Message);
      }
      try
      {
        Util.Log("Registering non-existent regular expression.");
        region1.GetSubscriptionService().UnregisterRegex("Non*Existent*Regex*");
        Assert.Fail("Did not get expected exception!");
      }
      catch (Exception ex)
      {
        Util.Log("Got expected exception {0}: {1}", ex.GetType(), ex.Message);
      }
    }

    public void StepFourFailoverRegex()
    {
      VerifyCreated(m_regionNames[0], m_keys[0]);
      VerifyCreated(m_regionNames[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);

      UpdateEntry(m_regionNames[1], m_keys[1], m_vals[1], true);
      UnregisterRegexes(null, m_regexes[2]);
    }

    public void StepFiveIL()
    {
      VerifyCreated(m_regionNames[0], m_keys[1]);
      VerifyCreated(m_regionNames[1], m_keys[3]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);

      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], false);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], false);
    }

    public void StepFiveRegex()
    {
      CreateEntry(m_regionNames[0], m_keys[2], m_vals[2]);
      CreateEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public void CreateAllEntries(string regionName)
    {
      CreateEntry(regionName, m_keys[0], m_vals[0]);
      CreateEntry(regionName, m_keys[1], m_vals[1]);
      CreateEntry(regionName, m_keys[2], m_vals[2]);
      CreateEntry(regionName, m_keys[3], m_vals[3]);
    }

    public void VerifyAllEntries(string regionName, bool newVal, bool checkVal)
    {
      string[] vals = newVal ? m_nvals : m_vals;
      VerifyEntry(regionName, m_keys[0], vals[0], checkVal);
      VerifyEntry(regionName, m_keys[1], vals[1], checkVal);
      VerifyEntry(regionName, m_keys[2], vals[2], checkVal);
      VerifyEntry(regionName, m_keys[3], vals[3], checkVal);
    }

    public void VerifyInvalidAll(string regionName, params string[] keys)
    {
      if (keys != null)
      {
        foreach (string key in keys)
        {
          VerifyInvalid(regionName, key);
        }
      }
    }

    public void UpdateAllEntries(string regionName, bool checkVal)
    {
      UpdateEntry(regionName, m_keys[0], m_nvals[0], checkVal);
      UpdateEntry(regionName, m_keys[1], m_nvals[1], checkVal);
      UpdateEntry(regionName, m_keys[2], m_nvals[2], checkVal);
      UpdateEntry(regionName, m_keys[3], m_nvals[3], checkVal);
    }

    public void DoNetsearchAllEntries(string regionName, bool newVal,
      bool checkNoKey)
    {
      string[] vals;
      if (newVal)
      {
        vals = m_nvals;
      }
      else
      {
        vals = m_vals;
      }
      DoNetsearch(regionName, m_keys[0], vals[0], checkNoKey);
      DoNetsearch(regionName, m_keys[1], vals[1], checkNoKey);
      DoNetsearch(regionName, m_keys[2], vals[2], checkNoKey);
      DoNetsearch(regionName, m_keys[3], vals[3], checkNoKey);
    }

    public void StepFiveFailoverRegex()
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], false);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], false);
      VerifyEntry(m_regionNames[1], m_keys[1], m_vals[1], false);
    }

    public void StepSixIL()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetRegion<object, object>(m_regionNames[1]);
      region0.Remove(m_keys[1]);
      region1.Remove(m_keys[3]);
    }

    public void StepSixRegex()
    {
      CreateEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      CreateEntry(m_regionNames[1], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[0], m_keys[2], m_vals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);

      UnregisterRegexes(null, m_regexes[3]);
    }

    public void StepSixFailoverRegex()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], false);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2], false);
      UpdateEntry(m_regionNames[1], m_keys[1], m_nvals[1], false);
    }

    public void StepSevenIL()
    {
      VerifyDestroyed(m_regionNames[0], m_keys[1]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);
    }

    public void StepSevenRegex()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[1], m_keys[1], m_vals[1]);
      UpdateEntry(m_regionNames[0], m_keys[2], m_nvals[2], true);
      UpdateEntry(m_regionNames[1], m_keys[3], m_nvals[3], true);

      UnregisterRegexes(null, m_regexes[1]);
    }

    public void StepSevenRegex2()
    {
      VerifyEntry(m_regionNames[0], m_keys[1], m_vals[1]);
      VerifyEntry(m_regionNames[0], m_keys[2], m_vals[2]);

      DoNetsearch(m_regionNames[0], m_keys[0], m_vals[0], true);
      DoNetsearch(m_regionNames[0], m_keys[3], m_vals[3], true);

      UpdateAllEntries(m_regionNames[1], true);
    }

    public void StepSevenInterestResultPolicyInv()
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region.GetSubscriptionService().RegisterRegex(m_regex23);

      VerifyInvalidAll(m_regionNames[0], m_keys[1], m_keys[2]);
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0], true);
      VerifyEntry(m_regionNames[0], m_keys[3], m_vals[3], true);
    }

    public void StepSevenFailoverRegex()
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], true);
      UpdateEntry(m_regionNames[1], m_keys[2], m_vals[2], true);
      VerifyEntry(m_regionNames[1], m_keys[1], m_nvals[1]);
    }

    public void StepEightIL()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
    }

    public void StepEightRegex()
    {
      VerifyEntry(m_regionNames[0], m_keys[2], m_nvals[2]);
      VerifyEntry(m_regionNames[1], m_keys[3], m_vals[3]);
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], true);
      UpdateEntry(m_regionNames[1], m_keys[1], m_nvals[1], true);
    }

    public void StepEightInterestResultPolicyInv()
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      region.GetSubscriptionService().RegisterAllKeys();

      VerifyInvalidAll(m_regionNames[1], m_keys[0], m_keys[1],
      m_keys[2], m_keys[3]);
      UpdateAllEntries(m_regionNames[0], true);
    }

    public void StepEightFailoverRegex()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_vals[2]);
    }

    public void StepNineRegex()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[1], m_vals[1]);
    }

    public void StepNineRegex2()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_vals[0]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1]);
      VerifyEntry(m_regionNames[0], m_keys[2], m_nvals[2]);
      VerifyEntry(m_regionNames[0], m_keys[3], m_vals[3]);
    }

    public void StepNineInterestResultPolicyInv()
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      region.GetSubscriptionService().UnregisterRegex(m_regex23);
      List<Object> keys = new List<Object>();
      keys.Add(m_keys[0]);
      keys.Add(m_keys[1]);
      keys.Add(m_keys[2]);  
      region.GetSubscriptionService().RegisterKeys(keys);

      VerifyInvalidAll(m_regionNames[0], m_keys[0], m_keys[1], m_keys[2]);
    }

    public void PutUnicodeKeys(string regionName, bool updates)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      string key;
      object val;
      for (int index = 0; index < m_numUnicodeStrings; ++index)
      {
        key = GetUnicodeString(index);
        if (updates)
        {
          val = index + 100;
        }
        else
        {
          val = (float)index + 20.0F;
        }
        region[key] = val;
      }
    }

    public void RegisterUnicodeKeys(string regionName)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      string[] keys = new string[m_numUnicodeStrings];
      for (int index = 0; index < m_numUnicodeStrings; ++index)
      {
        keys[m_numUnicodeStrings - index - 1] = GetUnicodeString(index);
      }
      region.GetSubscriptionService().RegisterKeys(keys);
    }

    public void VerifyUnicodeKeys(string regionName, bool updates)
    {
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(regionName);
      string key;
      object expectedVal;
      for (int index = 0; index < m_numUnicodeStrings; ++index)
      {
        key = GetUnicodeString(index);
        if (updates)
        {
          expectedVal = index + 100;
          Assert.AreEqual(expectedVal, region.GetEntry(key).Value,
            "Got unexpected value");
        }
        else
        {
          expectedVal = (float)index + 20.0F;
          Assert.AreEqual(expectedVal, region[key],
            "Got unexpected value");
        }
      }
    }

    public void CreateRegionsInterestNotify_Pool(string[] regionNames,
      string locators, string poolName, bool notify, string nbs)
    {
      Properties<string, string> props = Properties<string, string>.Create<string, string>();
      //props.Insert("notify-by-subscription-override", nbs);
      CacheHelper.InitConfig(props);
      CacheHelper.CreateTCRegion_Pool(regionNames[0], true, true,
        new TallyListener<object, object>(), locators, poolName, notify);
      CacheHelper.CreateTCRegion_Pool(regionNames[1], true, true,
        new TallyListener<object, object>(), locators, poolName, notify);
      CacheHelper.CreateTCRegion_Pool(regionNames[2], true, true,
        new TallyListener<object, object>(), locators, poolName, notify);
    }

    /*
    public void CreateRegionsInterestNotify(string[] regionNames,
      string endpoints, bool notify, string nbs)
    {
      Properties props = Properties.Create();
      //props.Insert("notify-by-subscription-override", nbs);
      CacheHelper.InitConfig(props);
      CacheHelper.CreateTCRegion(regionNames[0], true, false,
        new TallyListener(), endpoints, notify);
      CacheHelper.CreateTCRegion(regionNames[1], true, false,
        new TallyListener(), endpoints, notify);
      CacheHelper.CreateTCRegion(regionNames[2], true, false,
        new TallyListener(), endpoints, notify);
    }
     * */

    public void DoFeed()
    {
      foreach (string regionName in RegionNamesForInterestNotify)
      {
        IRegion<object, object> region = CacheHelper.GetRegion<object, object>(regionName);
        foreach (string key in m_keysNonRegex)
        {
          region[key] = "00";
        }
        foreach (string key in m_keysForRegex)
        {
          region[key] = "00";
        }
      }
    }

    public void DoFeederOps()
    {
      foreach (string regionName in RegionNamesForInterestNotify)
      {
        IRegion<object, object> region = CacheHelper.GetRegion<object, object>(regionName);
        foreach (string key in m_keysNonRegex)
        {
          region[key] = "11";
          region[key] = "22";
          region[key] = "33";
          region.GetLocalView().Invalidate(key);
          region.Remove(key);
        }
        foreach (string key in m_keysForRegex)
        {
          region[key] = "11";
          region[key] = "22";
          region[key] = "33";
          region.GetLocalView().Invalidate(key);
          region.Remove(key);
        }
      }
    }

    public void DoRegister()
    {
      DoRegisterInterests(RegionNamesForInterestNotify[0], true);
      DoRegisterInterests(RegionNamesForInterestNotify[1], false);
      // We intentionally do not register interest in Region3
      //DoRegisterInterestsBlah(RegionNamesForInterestNotifyBlah[2]);
    }

    public void DoRegisterInterests(string regionName, bool receiveValues)
    {
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(regionName);
      List<string> keys = new List<string>();
      foreach (string key in m_keysNonRegex)
      {
        keys.Add(key);
      }
      region.GetSubscriptionService().RegisterKeys(keys.ToArray(), false, false, receiveValues);
      region.GetSubscriptionService().RegisterRegex("key-regex.*", false, null, false, receiveValues);
    }

    public void DoUnregister()
    {
      DoUnregisterInterests(RegionNamesForInterestNotify[0]);
      DoUnregisterInterests(RegionNamesForInterestNotify[1]);
    }

    public void DoUnregisterInterests(string regionName)
    {
      List<string> keys = new List<string>();
      foreach (string key in m_keysNonRegex)
      {
        keys.Add(key);
      }
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(regionName);
      region.GetSubscriptionService().UnregisterKeys(keys.ToArray());
      region.GetSubscriptionService().UnregisterRegex("key-regex.*");
    }

    public void DoValidation(string clientName, string regionName,
      int creates, int updates, int invalidates, int destroys)
    {
      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(regionName);
      TallyListener<object, object> listener = region.Attributes.CacheListener as TallyListener<object, object>;

      Util.Log(clientName + ": " + regionName + ": creates expected=" + creates +
        ", actual=" + listener.Creates);
      Util.Log(clientName + ": " + regionName + ": updates expected=" + updates +
        ", actual=" + listener.Updates);
      Util.Log(clientName + ": " + regionName + ": invalidates expected=" + invalidates +
        ", actual=" + listener.Invalidates);
      Util.Log(clientName + ": " + regionName + ": destroys expected=" + destroys +
        ", actual=" + listener.Destroys);

      Assert.AreEqual(creates, listener.Creates, clientName + ": " + regionName);
      Assert.AreEqual(updates, listener.Updates, clientName + ": " + regionName);
      Assert.AreEqual(invalidates, listener.Invalidates, clientName + ": " + regionName);
      Assert.AreEqual(destroys, listener.Destroys, clientName + ": " + regionName);
    }

    #endregion

    void runInterestList()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThree);
      m_client1.Call(RegisterKeys, m_keys[1], m_keys[3]);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      m_client2.Call(RegisterKeys, m_keys[0], (string)null);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFiveIL);
      m_client1.Call(UnregisterKeys, (string)null, m_keys[3]);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixIL);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenIL);
      Util.Log("StepSeven complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }
    void RegisterKeysPdx()
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      region.GetSubscriptionService().RegisterAllKeys();
    }
    void StepThreePdx()
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      region[1] = new PdxTests.PdxTypes8();
    }
    void StepFourPdx()
    {
      Thread.Sleep(2000);
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);

      IRegion<object, object> regionLocal = region.GetLocalView();
      object ret = regionLocal[1];

      Assert.IsNotNull(ret);
      Assert.IsTrue(ret is IPdxSerializable);
    }
    void runInterestListPdx()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client2.Call(RegisterKeysPdx);

      m_client1.Call(StepThreePdx);
      
      Util.Log("StepThreePdx complete.");

      m_client2.Call(StepFourPdx);
      Util.Log("StepFourPdx complete.");

      
      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runInterestList2()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThree);
      m_client1.Call(RegisterAllKeys,
        new string[] { RegionNames[0], RegionNames[1] });
      Util.Log("StepThree complete.");

      m_client2.Call(StepFour);
      m_client2.Call(RegisterAllKeys, new string[] { RegionNames[0] });
      Util.Log("StepFour complete.");

      m_client1.Call(StepFiveIL);
      m_client1.Call(UnregisterAllKeys, new string[] { RegionNames[1] });
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixIL);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenIL);
      Util.Log("StepSeven complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRegexInterest()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client1.Call(RegisterRegexes, m_regexes[0], m_regexes[1]);
      Util.Log("StepThree complete.");

      m_client2.Call(RegisterRegexes, m_regexes[2], m_regexes[3]);
      Util.Log("StepFour complete.");

      m_client1.Call(StepFiveRegex);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixRegex);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenRegex);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEightRegex);
      Util.Log("StepEight complete.");

      m_client1.Call(StepNineRegex);
      Util.Log("StepNine complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
 
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRegexInterest2()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client1.Call(RegisterRegexes, m_regex23, (string)null);
      Util.Log("StepThree complete.");

      m_client2.Call(RegisterRegexes, (string)null, m_regexWildcard);
      Util.Log("StepFour complete.");

      m_client1.Call(CreateAllEntries, RegionNames[1]);
      Util.Log("StepFive complete.");

      m_client2.Call(CreateAllEntries, RegionNames[0]);
      m_client2.Call(VerifyAllEntries, RegionNames[1], false, false);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenRegex2);
      m_client1.Call(UpdateAllEntries, RegionNames[1], true);
      Util.Log("StepSeven complete.");

      m_client2.Call(VerifyAllEntries, RegionNames[1], true, true);
      m_client2.Call(UpdateAllEntries, RegionNames[0], true);
      Util.Log("StepEight complete.");

      m_client1.Call(StepNineRegex2);
      Util.Log("StepNine complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runRegexInterest3()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      try
      {
        m_client1.Call(RegisterRegexes, "a*", "*[*2-[");
        Assert.Fail("Did not get expected exception!");
      }
      catch (Exception ex)
      {
        Util.Log("Got expected exception {0}: {1}", ex.GetType(), ex.Message);
      }
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourRegex3);
      Util.Log("StepFour complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runInterestResultPolicyInv()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client1.Call(CreateAllEntries, RegionNames[1]);
      Util.Log("StepThree complete.");

      m_client2.Call(CreateAllEntries, RegionNames[0]);
      Util.Log("StepFour complete.");

      m_client2.Call(DoNetsearchAllEntries, RegionNames[1], false, true);
      Util.Log("StepFive complete.");

      m_client1.Call(DoNetsearchAllEntries, RegionNames[0], false, true);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenInterestResultPolicyInv);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEightInterestResultPolicyInv);
      Util.Log("StepEight complete.");

      m_client1.Call(StepNineInterestResultPolicyInv);
      Util.Log("StepNine complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailoverInterest()
    {
      CacheHelper.SetupJavaServers( true,
        "cacheserver_notify_subscription.xml",
        "cacheserver_notify_subscription2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client2.Call(RegisterKeys, m_keys[0], m_keys[2]);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourIL);
      Util.Log("StepFour complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      m_client1.Call(StepFiveFailover);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixFailover);
      Util.Log("StepSix complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailoverInterest2()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver_notify_subscription.xml",
        "cacheserver_notify_subscription2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      Util.Log("StepTwo complete.");

      m_client2.Call(RegisterAllKeys, RegionNames);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThree);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourIL);
      Util.Log("StepFour complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      m_client1.Call(StepFiveFailover);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixFailover);
      Util.Log("StepSix complete.");

      // Client2, unregister all keys
      m_client2.Call(UnregisterAllKeys, RegionNames);
      Util.Log("UnregisterAllKeys complete.");

      m_client1.Call(StepSevenFailover);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEightIL);
      Util.Log("StepEight complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runFailoverRegexInterest()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver_notify_subscription.xml",
        "cacheserver_notify_subscription2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_client1.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);

      m_client1.Call(CreateEntry, RegionNames[1], m_keys[1], m_vals[1]);
      Util.Log("StepOne complete.");

      m_client2.Call(CreateTCRegions_Pool, RegionNames,
        CacheHelper.Locators, "__TESTPOOL1_", true);

      m_client2.Call(CreateEntry, RegionNames[1], m_keys[1], m_nvals[1]);
      m_client2.Call(RegisterRegexes, m_regexes[0], m_regexes[2]);
      Util.Log("StepTwo complete.");

      m_client1.Call(StepThree);
      m_client1.Call(RegisterRegexes, (string)null, m_regexes[1]);
      m_client1.Call(DoNetsearch, RegionNames[1],
        m_keys[1], m_nvals[1], false);
      Util.Log("StepThree complete.");

      m_client2.Call(StepFourFailoverRegex);
      Util.Log("StepFour complete.");

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      Util.Log("Cacheserver 2 started.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      m_client1.Call(StepFiveFailoverRegex);
      Util.Log("StepFive complete.");

      m_client2.Call(StepSixFailoverRegex);
      Util.Log("StepSix complete.");

      m_client1.Call(StepSevenFailoverRegex);
      Util.Log("StepSeven complete.");

      m_client2.Call(StepEightFailoverRegex);
      Util.Log("StepEight complete.");

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runInterestNotify()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver_interest_notify.xml");

      // start locator and server

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver started.");

      // create feeder and 3 clients each with 3 regions and
      // populate initial keys

      m_feeder.Call(CreateRegionsInterestNotify_Pool, RegionNamesForInterestNotify,
        CacheHelper.Locators, "__TESTPOOL1_", false, "server" /* nbs */);

      m_feeder.Call(DoFeed);

      m_client1.Call(CreateRegionsInterestNotify_Pool, RegionNamesForInterestNotify,
        CacheHelper.Locators, "__TESTPOOL1_", true, "true" /* nbs */);
      //m_client2.Call(CreateRegionsInterestNotify_Pool, RegionNamesForInterestNotify,
      //  CacheHelper.Locators, "__TESTPOOL1_", true, "false" /* nbs */);
      //m_client3.Call(CreateRegionsInterestNotify_Pool, RegionNamesForInterestNotify,
      //  CacheHelper.Locators, "__TESTPOOL1_", true, "server" /* nbs */);

      // Register interests and get initial values
      m_client1.Call(DoRegister);
      //m_client2.Call(DoRegister);
      //m_client3.Call(DoRegister);

      // Do ops while interest is registered
      m_feeder.Call(DoFeederOps);

      m_client1.Call(DoUnregister);
      //m_client2.Call(DoUnregister);
      //m_client3.Call(DoUnregister);

      // Do ops while interest is no longer registered
      m_feeder.Call(DoFeederOps);

      m_client1.Call(DoRegister);
      //m_client2.Call(DoRegister);
      //m_client3.Call(DoRegister);

      // Do ops while interest is re-registered
      m_feeder.Call(DoFeederOps);

      // Validate clients receive relevant expected event counts:

      m_client1.Call(DoValidation, "Client1", RegionNamesForInterestNotify[0], 6, 30, 0, 12);
      m_client1.Call(DoValidation, "Client1", RegionNamesForInterestNotify[1], 0, 0, 36, 12);
      m_client1.Call(DoValidation, "Client1", RegionNamesForInterestNotify[2], 0, 0, 0, 0);

      /*
      m_client2.Call(DoValidation, "Client2", RegionNamesForInterestNotify[0], 0, 0, 54, 18);
      m_client2.Call(DoValidation, "Client2", RegionNamesForInterestNotify[1], 0, 0, 54, 18);
      m_client2.Call(DoValidation, "Client2", RegionNamesForInterestNotify[2], 0, 0, 54, 18);

      m_client3.Call(DoValidation, "Client3", RegionNamesForInterestNotify[0], 0, 0, 54, 18);
      m_client3.Call(DoValidation, "Client3", RegionNamesForInterestNotify[1], 0, 0, 54, 18);
      m_client3.Call(DoValidation, "Client3", RegionNamesForInterestNotify[2], 0, 0, 54, 18);
       * */

      // close down

      m_client1.Call(Close);
      //m_client2.Call(Close);
      //m_client3.Call(Close);
      m_feeder.Call(Close);

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    [Test]
    public void InterestList()
    {
      runInterestList(); 
    }

    [Test]
    public void InterestListWithPdx()
    {
      runInterestListPdx();
    }

    [Test]
    public void InterestList2()
    {
      runInterestList2();
    }

    [Test]
    public void RegexInterest()
    {
      runRegexInterest();
    }

    [Test]
    public void RegexInterest2()
    {
      runRegexInterest2();
    }

    [Test]
    public void RegexInterest3()
    {
      runRegexInterest3();
    }

    [Test]
    public void InterestResultPolicyInv()
    {
      runInterestResultPolicyInv();
    }

    [Test]
    public void FailoverInterest()
    {
      runFailoverInterest();
    }

    [Test]
    public void FailoverInterest2()
    {
      runFailoverInterest2();
    }

    [Test]
    public void FailoverRegexInterest()
    {
      runFailoverRegexInterest();
    }

    [Test]
    public void InterestNotify()
    {
      runInterestNotify();
    }
  }
}

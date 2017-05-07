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

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  public abstract class ThinClientRegionSteps : DistOpsSteps
  {
    #region Protected statics/constants and members

     protected const string RegionName = "DistRegionAck";
    protected static string[] RegionNames = { RegionName, "DistRegionNoAck"/*, "exampleRegion" */};
    protected const string PartitionRegion1 =  "R1" ;
    protected const string PartitionRegion2 =  "R2" ;
    protected const string PartitionRegion3 =  "R3" ;
    protected static string[] RegionNames2 = { "exampleRegion", RegionName };
    protected const string KeyPrefix = "key-";
    protected const string ValuePrefix = "value-";
    protected const string NValuePrefix = "nvalue-";

    #endregion

    protected override string ExtraPropertiesFile
    {
      get
      {
        return "gfcpp.properties.nativeclient";
      }
    }

    #region Steps for Thin Client Region

    public void CreateNonExistentRegion(string endpoints, string locators, bool pool)
    {
      string regionName = "non-region";

      if (pool)
      {
        CacheHelper.CreateTCRegion_Pool(regionName, true, true,
          null, endpoints, locators, "__TESTPOOL1_", false);
      }
      else
      {
        CacheHelper.CreateTCRegion(regionName, true,
         true, null, endpoints, false);
      }

      try
      {
        CreateEntry(regionName, m_keys[0], m_vals[0]);
        Assert.Fail("Expected CacheServerException for operations on a " +
          "non-existent region [{0}].", regionName);
      }
      catch (CacheServerException ex)
      {
        Util.Log("Got expected exception in CreateNonExistentRegion: {0}",
          ex.Message);
      }
    }

    public override void DestroyRegions()
    {
      if (m_regionNames != null)
      {
        CacheHelper.DestroyRegion(m_regionNames[0], true, false);
        CacheHelper.DestroyRegion(m_regionNames[1], true, false);
      }
    }

    public void RegisterKeys(string key0, string key1)
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

    public void UnregisterKeys(string key0, string key1)
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
      if (key0 != null)
      {
              region0.UnregisterKeys(new CacheableKey[] {
          new CacheableString(key0) });
      }
      if (key1 != null)
      {
              region1.UnregisterKeys(new CacheableKey[] {
          new CacheableString(key1) });
      }
    }

    public void RegisterAllKeys(string[] regionNames)
    {
      Region region;
      if (regionNames != null)
      {
        foreach (string regionName in regionNames)
        {
          region = CacheHelper.GetVerifyRegion(regionName);
          region.RegisterAllKeys();
        }
      }
    }

    public void UnregisterAllKeys(string[] regionNames)
    {
      Region region;
      if (regionNames != null)
      {
        foreach (string regionName in regionNames)
        {
          region = CacheHelper.GetVerifyRegion(regionName);
              region.UnregisterAllKeys();
        }
      }
    }

    public void RegisterRegexes(string regex0, string regex1)
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

    public void UnregisterRegexes(string regex0, string regex1)
    {
      if (regex0 != null)
      {
        Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
            region0.UnregisterRegex(regex0);
      }
      if (regex1 != null)
      {
        Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);
            region1.UnregisterRegex(regex1);
      }
    }

    public void CheckServerKeys()
    {
      Region region0 = CacheHelper.GetVerifyRegion(m_regionNames[0]);
      Region region1 = CacheHelper.GetVerifyRegion(m_regionNames[1]);

      ICacheableKey[] keys0 = region0.GetServerKeys();
      ICacheableKey[] keys1 = region1.GetServerKeys();
      Assert.AreEqual(2, keys0.Length, "Should have 2 keys in region {0}.",
        m_regionNames[0]);
      Assert.AreEqual(2, keys1.Length, "Should have 2 keys in region {0}.",
        m_regionNames[1]);

      string key0, key1;

      key0 = keys0[0].ToString();
      key1 = keys0[1].ToString();
      Assert.AreNotEqual(key0, key1,
        "The two keys should be different in region {0}.", m_regionNames[0]);
      Assert.IsTrue(key0 == m_keys[0] || key0 == m_keys[1],
        "Unexpected key in first region.");
      Assert.IsTrue(key1 == m_keys[0] || key1 == m_keys[1],
        "Unexpected key in first region.");

      key0 = keys1[0].ToString();
      key1 = keys1[1].ToString();
      Assert.AreNotEqual(key0, key1,
        "The two keys should be different in region {0}.", m_regionNames[1]);
      Assert.IsTrue(key0 == m_keys[2] || key0 == m_keys[3],
        "Unexpected key in first region.");
      Assert.IsTrue(key1 == m_keys[2] || key1 == m_keys[3],
        "Unexpected key in first region.");
    }

    public void StepFiveNotify()
    {
      DoNetsearch(m_regionNames[0], m_keys[1], m_vals[1], true);
      DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], true);
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], true);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], true);
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
    }

    public void StepFiveFailover()
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], false);
      //DestroyEntry(m_regionNames[0], m_keys[0]);
      UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], false);
    }

    public override void StepSix(bool checkVal)
    {
      DoNetsearch(m_regionNames[0], m_keys[0], m_vals[0], false);
      DoNetsearch(m_regionNames[1], m_keys[2], m_vals[2], false);
      UpdateEntry(m_regionNames[0], m_keys[1], m_nvals[1], checkVal);
      UpdateEntry(m_regionNames[1], m_keys[3], m_nvals[3], checkVal);
    }

    public void StepSixNotify(bool checkVal)
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], checkVal);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2], checkVal);
      UpdateEntry(m_regionNames[0], m_keys[1], m_nvals[1], checkVal);
      UpdateEntry(m_regionNames[1], m_keys[3], m_nvals[3], checkVal);
    }

    public void StepSixFailover()
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], false);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2], false);
    }

    public void StepSevenNotify(bool checkVal)
    {
      VerifyInvalid(m_regionNames[0], m_keys[1]);
      VerifyInvalid(m_regionNames[1], m_keys[3]);
      VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1], checkVal);
      VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3], checkVal);
      InvalidateEntry(m_regionNames[0], m_keys[0]);
      InvalidateEntry(m_regionNames[1], m_keys[2]);
    }

    public void StepSevenFailover()
    {
      UpdateEntry(m_regionNames[0], m_keys[0], m_vals[0], false);
      UpdateEntry(m_regionNames[1], m_keys[2], m_vals[2], false);
    }

    public void StepEightNotify(bool checkVal)
    {
      VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0], checkVal);
      VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2], checkVal);
      InvalidateEntry(m_regionNames[0], m_keys[1]);
      InvalidateEntry(m_regionNames[1], m_keys[3]);
    }

    public void StepNineNotify(bool checkVal)
    {
      VerifyEntry(m_regionNames[0], m_keys[1], m_nvals[1], checkVal);
      VerifyEntry(m_regionNames[1], m_keys[3], m_nvals[3], checkVal);
      DestroyEntry(m_regionNames[0], m_keys[0]);
      DestroyEntry(m_regionNames[1], m_keys[2]);
    }

    #endregion

    #region Functions invoked by the tests

    public void Close()
    {
      CacheHelper.Close();
    }

    public void CloseKeepAlive()
    {
      CacheHelper.CloseCacheKeepAlive();
    }

    public void ReadyForEvents2()
    {
      CacheHelper.ReadyForEvents();
    }
    
    public void DoPutsMU(int numOps, Properties credentials, bool multiuserMode, ExpectedResult result)
    {
      DoPuts(numOps, false, result, credentials, multiuserMode);
    }

    public void DoPutsMU(int numOps, Properties credentials, bool multiuserMode)
    {
      DoPuts(numOps, false, ExpectedResult.Success, credentials, multiuserMode);
    }

    public void DoPuts(int numOps)
    {
      DoPuts(numOps, false, ExpectedResult.Success, null, false);
    }

    public void DoPuts(int numOps, bool useNewVal)
    {
      DoPuts(numOps, useNewVal, ExpectedResult.Success, null, false);
    }

    public void DoPuts(int numOps, bool useNewVal, ExpectedResult expect)
    {
      DoPuts(numOps, useNewVal,expect, null, false);
    }

    public void DoPuts(int numOps, bool useNewVal, ExpectedResult expect, Properties credentials, bool multiuserMode)
    {
      string valPrefix = (useNewVal ? NValuePrefix : ValuePrefix);
      Region region;
      if (multiuserMode)
        region = CacheHelper.GetVerifyRegion(RegionName, credentials);
      else
        region = CacheHelper.GetVerifyRegion(RegionName);
      for (int opNum = 1; opNum <= numOps; ++opNum)
      {
        try
        {
          region.Put(KeyPrefix + opNum, valPrefix + opNum);
          if (expect != ExpectedResult.Success)
          {
            Assert.Fail("DoPuts: Expected an exception in put");
          }
        }
        catch (AssertionException)
        {
          throw;
        }
        catch (NotAuthorizedException ex)
        {
          if (expect == ExpectedResult.NotAuthorizedException)
          {
            Util.Log("DoPuts: got expected unauthorized exception: " +
              ex.GetType() + "::" + ex.Message);
          }
          else
          {
            Assert.Fail("DoPuts: unexpected exception caught: " + ex);
          }
        }
        catch (AuthenticationFailedException ex)
        {
          if (expect == ExpectedResult.AuthFailedException)
          {
            Util.Log("DoPuts: got expected authentication Failed exception: " +
              ex.GetType() + "::" + ex.Message);
          }
          else
          {
            Assert.Fail("DoPuts: unexpected exception caught: " + ex);
          }          
        }
        catch (Exception ex)
        {
          if (expect == ExpectedResult.OtherException)
          {
            Util.Log("DoPuts: got expected exception: " +
              ex.GetType() + "::" + ex.Message);
          }
          else
          {
            Assert.Fail("DoPuts: unexpected exception caught: " + ex);
          }
        }
      }
    }

    public void DoGetKeysOnServer(bool isMultiuser, Properties credential)
    {
      Region region;
      if (isMultiuser)
      {
        region = CacheHelper.GetVerifyRegion(RegionName, credential);
      }
      else
      {
        region = CacheHelper.GetVerifyRegion(RegionName);
      }

      bool isKeyThere = region.ContainsKeyOnServer(KeyPrefix + 1);

      Assert.IsTrue(isKeyThere);
    }

    public void DoGetsMU(int numOps, Properties credential, bool isMultiuser, ExpectedResult result)
    {
      DoGets(numOps, false, result, credential, isMultiuser);
    }

    public void DoGetsMU(int numOps, Properties credential, bool isMultiuser)
    {
      DoGets(numOps, false, ExpectedResult.Success, credential, isMultiuser);
    }

    public void DoGets(int numOps)
    {
      DoGets(numOps, false, ExpectedResult.Success, null, false);
    }

    public void DoGets(int numOps, bool useNewVal)
    {
      DoGets(numOps, useNewVal, ExpectedResult.Success, null, false);
    }

    public void DoGets(int numOps, bool useNewVal, ExpectedResult expect)
    {
      DoGets(numOps, useNewVal, expect, null, false);
    }
    public void DoGets(int numOps, bool useNewVal, ExpectedResult expect, Properties credential, bool isMultiuser)
    {
      string valPrefix = (useNewVal ? NValuePrefix : ValuePrefix);
      Region region;
      if (isMultiuser)
      {
        region = CacheHelper.GetVerifyRegion(RegionName, credential);
      }
      else
      { 
        region = CacheHelper.GetVerifyRegion(RegionName);
      }

      for (int index = 1; index <= numOps; ++index)
      {
        string key = KeyPrefix + index;
        try
        {
          region.LocalInvalidate(key);
        }
        catch (Exception)
        {
          // ignore exception if key is not found in the region
        }
        IGFSerializable value = null;
        try
        {
          value = region.Get(key);
          if (expect != ExpectedResult.Success)
          {
            Assert.Fail("DoGets: Expected an exception in get");
          }
        }
        catch (AssertionException)
        {
          throw;
        }
        catch (NotAuthorizedException ex)
        {
          if (expect == ExpectedResult.NotAuthorizedException)
          {
            Util.Log("DoGets: got expected unauthorized exception: " +
              ex.GetType() + "::" + ex.Message);
            continue;
          }
          else
          {
            Assert.Fail("DoGets: unexpected unauthorized exception caught: " +
              ex);
          }
        }
        catch (AuthenticationFailedException ex)
        {
          if (expect == ExpectedResult.AuthFailedException)
          {
            Util.Log("DoPuts: got expected authentication Failed exception: " +
              ex.GetType() + "::" + ex.Message);
          }
          else
          {
            Assert.Fail("DoPuts: unexpected exception caught: " + ex);
          }
        }
        catch (Exception ex)
        {
          if (expect == ExpectedResult.OtherException)
          {
            Util.Log("DoGets: got expected exception: " +
              ex.GetType() + "::" + ex.Message);
            continue;
          }
          else
          {
            Assert.Fail("DoGets: unexpected exception caught: " + ex);
          }
        }
        Assert.IsNotNull(value);
        Assert.AreEqual(valPrefix + index, value.ToString());
      }
    }

    public void DoLocalGets(int numOps)
    {
      DoLocalGets(numOps, false);
    }

    public void DoLocalGets(int numOps, bool useNewVal)
    {
      string valPrefix = (useNewVal ? NValuePrefix : ValuePrefix);
      Region region = CacheHelper.GetVerifyRegion(RegionName);
      for (int index = 1; index <= numOps; ++index)
      {
        int sleepMillis = 100;
        int numTries = 30;
        CacheableString key = new CacheableString(KeyPrefix + index);
        string value = valPrefix + index;
        while (numTries-- > 0)
        {
          if (region.ContainsValueForKey(key))
          {
            string foundValue = region.Get(key).ToString();
            if (value.Equals(foundValue))
            {
              break;
            }
          }
          Thread.Sleep(sleepMillis);
        }
      }
      for (int index = 1; index <= numOps; ++index)
      {
        CacheableString key = new CacheableString(KeyPrefix + index);
        Assert.IsTrue(region.ContainsValueForKey(key));
        IGFSerializable value = region.Get(key);
        Assert.IsNotNull(value);
        Assert.AreEqual(valPrefix + index, value.ToString());
      }
    }

    #endregion
  }
}

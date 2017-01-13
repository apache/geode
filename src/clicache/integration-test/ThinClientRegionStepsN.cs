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
  using AssertionException = GemStone.GemFire.Cache.Generic.AssertionException;


  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  public abstract class ThinClientRegionSteps : DistOpsSteps
  {
    #region Protected statics/constants and members

    protected const string RegionName = "DistRegionAck";
    protected static string[] RegionNames = { RegionName, "DistRegionNoAck" };
    protected static string[] RegionNames2 = { "exampleRegion", RegionName };
    protected static string[] RegionNames3 = { "testregion", RegionName };
    protected const string PartitionRegion1 = "R1";
    protected const string PartitionRegion2 = "R2";
    protected const string PartitionRegion3 = "R3";
    protected const string KeyPrefix = "key-";
    protected const string ValuePrefix = "value-";
    protected const string NValuePrefix = "nvalue-";
    protected const string TradeKeyRegion = "TradeKeyRegion";

    #endregion

    protected override string ExtraPropertiesFile
    {
      get
      {
        return "gfcpp.properties.nativeclient";
      }
    }

    #region Steps for Thin Client IRegion<object, object>

    public void CreateNonExistentRegion(string locators)
    {
      string regionName = "non-region";

      CacheHelper.CreateTCRegion_Pool<object, object>(regionName, true, true,
        null, locators, "__TESTPOOL1_", false);

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
        CacheHelper.DestroyRegion<object, object>(m_regionNames[0], true, false);
        CacheHelper.DestroyRegion<object, object>(m_regionNames[1], true, false);
      }
    }

    public void RegisterKeys(string key0, string key1)
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      
      if (key0 != null)
      {
        //region0.RegisterKeys(new CacheableKey[] {new CacheableString(key0) });
        List<Object> keys0 = new List<Object>();
        keys0.Add(key0);      
        region0.GetSubscriptionService().RegisterKeys(keys0);
      }
      if (key1 != null)
      {
        List<Object> keys1 = new List<Object>();
        keys1.Add(key1);      
        region1.GetSubscriptionService().RegisterKeys(keys1);
      }
    }

    public void UnregisterKeys(string key0, string key1)
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
      if (key0 != null)
      {
          List<Object> keys0 = new List<Object>();
          keys0.Add(key0);
          region0.GetSubscriptionService().UnregisterKeys(keys0);
      }
      if (key1 != null)
      {
          List<Object> keys1 = new List<Object>();
          keys1.Add(key1);
          region1.GetSubscriptionService().UnregisterKeys(keys1);
      }
    }

    public void RegisterAllKeys(string[] regionNames)
    {
      IRegion<object, object> region;
      if (regionNames != null)
      {
        foreach (string regionName in regionNames)
        {
          region = CacheHelper.GetVerifyRegion<object, object>(regionName);
          region.GetSubscriptionService().RegisterAllKeys();
        }
      }
    }

    public void UnregisterAllKeys(string[] regionNames)
    {
      IRegion<object, object> region;
      if (regionNames != null)
      {
        foreach (string regionName in regionNames)
        {
          region = CacheHelper.GetVerifyRegion<object, object>(regionName);
            region.GetSubscriptionService().UnregisterAllKeys();
        }
      }
    }

    public void RegisterRegexes(string regex0, string regex1)
    {
      if (regex0 != null)
      {
        IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
        region0.GetSubscriptionService().RegisterRegex(regex0);
      }
      if (regex1 != null)
      {
        IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
        region1.GetSubscriptionService().RegisterRegex(regex1);
      }
    }

    public void UnregisterRegexes(string regex0, string regex1)
    {
      if (regex0 != null)
      {
        IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
          region0.GetSubscriptionService().UnregisterRegex(regex0);
      }
      if (regex1 != null)
      {
        IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);
          region1.GetSubscriptionService().UnregisterRegex(regex1);
      }
    }

    public void CheckServerKeys()
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(m_regionNames[1]);

      //ICacheableKey[] keys0 = region0.GetServerKeys();
      ICollection<Object> keys0 = region0.Keys;
      //ICacheableKey[] keys1 = region1.GetServerKeys();
      ICollection<Object> keys1 = region1.Keys;

      Assert.AreEqual(2, keys0.Count, "Should have 2 keys in region {0}.",
        m_regionNames[0]);
      Assert.AreEqual(2, keys1.Count, "Should have 2 keys in region {0}.",
        m_regionNames[1]);

      string key0, key1;

      IEnumerator<Object> obj = keys0.GetEnumerator();
      obj.MoveNext();
      key0 = obj.Current.ToString();
      //Console.WriteLine("key0 = {0}", key0);
      obj.MoveNext();
      key1 = obj.Current.ToString();
      
      //key0 = keys0[0].ToString();
      //key1 = keys0[1].ToString();
      Assert.AreNotEqual(key0, key1,
        "The two keys should be different in region {0}.", m_regionNames[0]);
      Assert.IsTrue(key0 == m_keys[0] || key0 == m_keys[1],
        "Unexpected key in first region.");
      Assert.IsTrue(key1 == m_keys[0] || key1 == m_keys[1],
        "Unexpected key in first region.");

      //key0 = keys1[0].ToString();
      //key1 = keys1[1].ToString();
      IEnumerator<Object> obj1 = keys1.GetEnumerator();
      obj1.MoveNext();
      key0 = obj1.Current.ToString();
      //Console.WriteLine("key0 = {0}", key0);
      obj1.MoveNext();
      key1 = obj1.Current.ToString();

      Assert.AreNotEqual(key0, key1,
        "The two keys should be different in region {0}.", m_regionNames[1]);
      Assert.IsTrue(key0 == m_keys[2] || key0 == m_keys[3],
        "Unexpected key in first region.");
      Assert.IsTrue(key1 == m_keys[2] || key1 == m_keys[3],
        "Unexpected key in first region.");
    }

    //public void StepFiveNotify()
    //{
    //  DoNetsearch(m_regionNames[0], m_keys[1], m_vals[1], true);
    //  DoNetsearch(m_regionNames[1], m_keys[3], m_vals[3], true);
    //  UpdateEntry(m_regionNames[0], m_keys[0], m_nvals[0], true);
    //  UpdateEntry(m_regionNames[1], m_keys[2], m_nvals[2], true);
    //  VerifyEntry(m_regionNames[0], m_keys[0], m_nvals[0]);
    //  VerifyEntry(m_regionNames[1], m_keys[2], m_nvals[2]);
    //}

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

    public void DoPutsMU(int numOps, Generic.Properties<string, string> credentials, bool multiuserMode, ExpectedResult result)
    {
      DoPuts(numOps, false, result, credentials, multiuserMode);
    }

    public void DoPutsMU(int numOps, Generic.Properties<string, string> credentials, bool multiuserMode)
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
      DoPuts(numOps, useNewVal, expect, null, false);
    }

    public void DoPuts(int numOps, bool useNewVal, ExpectedResult expect, Generic.Properties<string, string> credentials, bool multiuserMode)
    {
      string valPrefix = (useNewVal ? NValuePrefix : ValuePrefix);
      IRegion<object, object> region;
      if (multiuserMode)
        region = CacheHelper.GetVerifyRegion<object, object>(RegionName, credentials);
      else
        region = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
      for (int opNum = 1; opNum <= numOps; ++opNum)
      {
        try
        {
          region[KeyPrefix + opNum] = valPrefix + opNum;

          region.Invalidate(KeyPrefix + opNum);

          region[KeyPrefix + opNum] = valPrefix + opNum;

          Util.Log("Pdx ops starts");
          region[opNum] = new PdxTests.PdxTypes8();
          region[opNum + "_pdx1"] = new PdxTests.PdxTypes1();
          region[opNum + "_pdx_8"] = new PdxTests.PdxTypes8();

          IDictionary<object, object> putall = new Dictionary<object,object>();
          putall.Add(opNum +"_pdxputall81", new PdxTests.PdxTypes8());
          putall.Add(opNum + "_pdxputall82", new PdxTests.PdxTypes8());
          region.PutAll(putall);

        
          Util.Log("Pdx ops ends");

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

    public void DoPutsTx(int numOps, bool useNewVal, ExpectedResult expect, Generic.Properties<string, string> credentials, bool multiuserMode)
    {
      Util.Log("DoPutsTx starts");
      CacheHelper.CSTXManager.Begin();
      string valPrefix = (useNewVal ? NValuePrefix : ValuePrefix);
      IRegion<object, object> region;
      if (multiuserMode)
          region = CacheHelper.GetVerifyRegion<object, object>(RegionName, credentials);
      else
          region = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
      for (int opNum = 1; opNum <= numOps; ++opNum)
      {
        try
        {
          region[KeyPrefix + opNum] = valPrefix + opNum;
          region.Invalidate(KeyPrefix + opNum);
          region[KeyPrefix + opNum] = valPrefix + opNum;
          Util.Log("Pdx ops starts");
          region[opNum] = new PdxTests.PdxTypes8();
          region[opNum + "_pdx1"] = new PdxTests.PdxTypes1();
          region[opNum + "_pdx_8"] = new PdxTests.PdxTypes8();
          IDictionary<object, object> putall = new Dictionary<object, object>();
          putall.Add(opNum + "_pdxputall81", new PdxTests.PdxTypes8());
          putall.Add(opNum + "_pdxputall82", new PdxTests.PdxTypes8());
          region.PutAll(putall);
          Util.Log("Pdx ops ends");
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
      CacheHelper.CSTXManager.Commit();
      Util.Log("DoPutsTx Done");
    }

    public void DoGetsMU(int numOps, Generic.Properties<string, string> credential, bool isMultiuser, ExpectedResult result)
    {
      DoGets(numOps, false, result, credential, isMultiuser);
    }

    public void DoGetsMU(int numOps, Generic.Properties<string, string> credential, bool isMultiuser)
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
    public void DoGets(int numOps, bool useNewVal, ExpectedResult expect, Generic.Properties<string, string> credential, bool isMultiuser)
    {
      string valPrefix = (useNewVal ? NValuePrefix : ValuePrefix);
      IRegion<object, object> region;
      Serializable.RegisterPdxType(PdxTests.PdxTypes1.CreateDeserializable);
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
      if (isMultiuser)
      {
        region = CacheHelper.GetVerifyRegion<object, object>(RegionName, credential);
      }
      else
      {
        region = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      }

      for (int index = 1; index <= numOps; ++index)
      {
        try
        {
          region.GetLocalView().Invalidate(index + "_pdxputall81");
          region.GetLocalView().Invalidate(index + "_pdxputall82");
        }
        catch (Exception )
        { }
      }
      
      if (expect == ExpectedResult.Success)
      {
        for (int index = 1; index <= numOps; ++index)
        {
          object ret1 = region[index + "_pdxputall81"];
          object ret2 = region[index + "_pdxputall82"];

          Assert.IsTrue(ret1 != null && ret1 is PdxTests.PdxTypes8);
          Assert.IsTrue(ret1 != null && ret2 is PdxTests.PdxTypes8);
        }
      }

      for (int index = 1; index <= numOps; ++index)
      {
        try
        {
          region.GetLocalView().Invalidate(index + "_pdxputall81");
          region.GetLocalView().Invalidate(index + "_pdxputall82");
        }
        catch (Exception )
        { }
      }

      if (expect == ExpectedResult.Success)
      {
        for (int index = 1; index <= numOps; ++index)
        {
          ICollection<object> pdxKeys = new List<object>();
          pdxKeys.Add(index + "_pdxputall81");
          pdxKeys.Add(index + "_pdxputall82");
          IDictionary<object, object> getall = new Dictionary<object, object>();
          region.GetAll(pdxKeys, getall, null);

          Assert.AreEqual(2, getall.Count);
        }
      }

      for (int index = 1; index <= numOps; ++index)
      {
        string key = KeyPrefix + index;
        try
        {
          region.GetLocalView().Invalidate(key);
        }
        catch (Exception)
        {
          // ignore exception if key is not found in the region
        }
        Object value = null;
        try
        {
          value = region[key];
          Object retPdx = region[index];

          Assert.IsTrue(retPdx != null && retPdx is PdxTests.PdxTypes8);
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
      IRegion<object, object> region = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      for (int index = 1; index <= numOps; ++index)
      {
        int sleepMillis = 100;
        int numTries = 30;
        string key = KeyPrefix + index;
        string value = valPrefix + index;
        while (numTries-- > 0)
        {
          if (region.ContainsValueForKey(key))
          {
            string foundValue = region[key].ToString();
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
        string key = KeyPrefix + index;
        Assert.IsTrue(region.ContainsValueForKey(key));
        Object value = region[key];
        Assert.IsNotNull(value);
        Assert.AreEqual(valPrefix + index, value.ToString());
      }
    }

    #endregion
  }
}

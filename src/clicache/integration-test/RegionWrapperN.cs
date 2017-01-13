//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  public class RegionWrapper
  {
    protected IRegion<object, object> m_region;
    protected bool m_noack;

    public IRegion<object, object> Region
    {
      get
      {
        return m_region;
      }
    }

    public RegionWrapper(IRegion<object, object> region)
    {
      if (region == null)
      {
        Assert.Fail("Cannot wrap null region.");
      }
      m_region = region;
    }

    public RegionWrapper(string name)
    {
      m_region = CacheHelper.GetRegion<object, object>(name);
      if (m_region == null)
      {
        Assert.Fail("No region with name {0} found!", name);
      }
    }

    public virtual void Put(int key, int value)
    {
      m_region["key" + key.ToString()] = value;
    }

    public virtual bool WaitForKey(string key)
    {
      return WaitForKey(key, 100, 100);
    }

    public virtual bool WaitForKey(string key, int maxTries, int sleepMillis)
    {
      int tries = 0;
      bool found = false;

      while ((tries < maxTries) && (!(found = m_region.ContainsKey(key))))
      {
        Thread.Sleep(sleepMillis);
        tries++;
      }
      return found;
    }

    public virtual bool WaitForValue(ICacheableKey key)
    {
      return WaitForValue(key, 100, 100);
    }

    public virtual bool WaitForValue(ICacheableKey cKey, int maxTries, int sleepMillis)
    {
      int tries = 0;
      bool found = false;

      while ((tries++ < maxTries) && (!(found = m_region.ContainsValueForKey(cKey))))
      {
        Thread.Sleep(sleepMillis);
      }
      return found;
    }

    public virtual Object WaitForValueGet(ICacheableKey cKey,
      int maxTries, int sleepMillis)
    {
      int tries = 0;
      Object cVal = null;

      while ((tries++ < maxTries) && ((cVal = m_region[cKey]) != null))
      {
        Thread.Sleep(sleepMillis);
      }
      return cVal;
    }

    public virtual int WaitForValue(Object key, int expected, bool noack)
    {
      int val = -1;
      Object cVal = null;

      if (noack)
      {
        for (int tries = 0; tries < 100; tries++)
        {
          cVal = m_region[key];
          Assert.IsNotNull(cVal, "value should not be null.");
          Util.Log("WaitForValue: Received value: {0}", cVal);
          val = int.Parse(cVal.ToString());
          if (val == expected)
          {
            break;
          }
          Thread.Sleep(100);
        }
      }
      else
      {
        cVal = m_region[key];
        Assert.IsNotNull(cVal, "value should not be null.");
        val = int.Parse(cVal.ToString());
      }
      return val;
    }

    // by convention, we'll accept value of -1 to mean not exists, 0 to mean invalid, and otherwise we'll compare.
    public virtual void Test(int key, int value)
    {
      Test(key, value, m_noack);
    }

    public virtual void Test(int key, int value, bool noack)
    {
      string cKey = "key" + key.ToString();
      StackFrame sf = new StackFrame(1, true);
      int line = sf.GetFileLineNumber();
      string method = sf.GetMethod().Name;
      if (value == -1)
      {
        Assert.IsFalse(m_region.GetLocalView().ContainsKey(cKey),
          "unexpected key found at line {0} in method {1}.", line, method);
        if (noack)
        { // need to wait a bit and retest...
          Thread.Sleep(1000);
          Assert.IsFalse(m_region.GetLocalView().ContainsKey(cKey),
            "unexpected key found at line {0} in method {1}.", line, method);
        }  
      }
      else if (value == 0)
      { 
        if (noack)
        {
          WaitForKey(cKey);
        }
        Assert.IsTrue(m_region.GetLocalView().ContainsKey(cKey),
          "missing key at line {0} in method {1}.", line, method);
        Assert.IsFalse(m_region.ContainsValueForKey(cKey),
          "should have found invalid at line {0} in method {1}.", line, method);
      }
      else
      {
        if (noack)
        {
          WaitForKey(cKey);
        }
        Assert.IsTrue(m_region.GetLocalView().ContainsKey(cKey),
          "missing key at line {0} in method {1}.", line, method);
        int val = WaitForValue(cKey, value, noack);
        Assert.AreEqual(value, val,
          "unexpected value: \"{0}\", expected \"{1}\" from line {2} in method {3}",
          val, value, line, method);
       
      }
    }

    // by convention, we'll accept value of -1 to mean not exists, otherwise we'll compare.
    public virtual void NetSearch(int key, int value)
    {
      string cKey = "key" + key.ToString();
      Assert.IsFalse(m_region.ContainsKey(cKey), "shouldn't have key before NetSearch.");
      int cVal = (int)m_region[cKey];
      if (value == -1)
      {
        Assert.IsNull(cVal, "unexpected value found.");
      }
      else
      {
        Assert.IsNotNull(cVal, "missing value for key[{0}].", cKey);
        Util.Log("got value='{0}' for key[{1}]", cVal, cKey);
        Assert.AreEqual(value, cVal);
        Assert.IsTrue(m_region.ContainsValueForKey(cKey),
          "should now be in the local cache.");
      }
    }

    public void ShowKeys()
    {
      ICollection<Object> keys = m_region.Keys;
      int len = keys.Count;
      Util.Log("Total keys in IRegion<object, object> {0} : {1}", m_region.Name, len);
      CacheHelper.ShowKeys(keys);
    }

    public void ShowValues()
    {
      ICollection<Object> values = m_region.Values;
      int len = values.Count;
      Util.Log("Total values in IRegion<object, object> {0} : {1}", m_region.Name, len);
      CacheHelper.ShowValues(values);
    }

    public void ShowKeysValues()
    {
      //Object value;
      ICollection<Object> keys = m_region.Keys;
      int len = keys.Count;
      Util.Log("Total keys in IRegion<object, object> {0} : {1}", m_region.Name, len);
      for (int i = 0; i < len; i++)
      {
        //TODO UNCOMMENT ONCE WE ATTACH THE LIST TO THE COLLECTION
        //value = m_region[keys[i]];
        //Util.Log("Key[{0}] = {1}, Value[{2}] = {3}", i, keys[i], i, value);
      }
    }
  }
}

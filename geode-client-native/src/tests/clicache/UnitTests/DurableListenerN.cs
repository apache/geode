//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache;
  using GemStone.GemFire.Cache.Generic;
  
  //using Com.Vmware.Cache;
  //using Region = Com.Vmware.Cache.IRegion<object, object>;

  class DurableListener<TKey, TVal> : ICacheListener<TKey, TVal>
  {
    #region Private members

    private int m_ops = 0;
    private Dictionary<object, object> m_map = new Dictionary<object, object>();
    
    #endregion

    #region Public accessors
    
    public int Ops
    {
      get
      {
        return m_ops;
      }
    }
    
    #endregion

    public static DurableListener<TKey, TVal> Create()
    {
      Util.Log(" DurableListener Created");

      return new DurableListener<TKey, TVal>();
    }

    private void check(EntryEvent<TKey, TVal> ev)
    {
      m_ops++;

      //CacheableString  key = ev.Key as CacheableString;
      //ICacheableKey<TKey> key = ev.Key;
      //TVal value = ev.NewValue;
      object key = (object)ev.Key;
      object value = (object)ev.NewValue;
      //object key = (object)ev.Key;
      //object value = (object)ev.NewValue;
      //CacheableInt32 value = ev.NewValue as CacheableInt32;
      //object value = (object)ev.NewValue;
      //object key1 = key.Value;
      //int value1 = (int)value;

      //if (m_map.ContainsKey(key))
      //{
      //  //CacheableInt32 old = m_map[key] as CacheableInt32;
      //  //TVal old =(TVal) m_map[(TKey)key];
      //  object old = m_map[key];
      //  Assert.AreEqual(value, old /*old + 1*/, "Duplicate or older value received");
      //}

      Util.Log("key={0} and Value={1}", key, value);
      Util.Log("key={0} and Value={1}", key.GetType(), value.GetType());
      m_map[key] = value;
    }
    
    // this method for the ThinClientDurableTests
    public void validate(int keys, int durable, int nondurable)
    {
      string msg1 = string.Format("Expected {0} keys but got {1}", keys, m_map.Count);
      Assert.AreEqual(keys, m_map.Count, msg1);

      int total = keys * (durable + nondurable) / 2;

      string msg2 = string.Format("Expected {0} events but got {1}", total, m_ops);
      Assert.AreEqual(total, m_ops, msg2);
      
      foreach (KeyValuePair<object, object> item in m_map)
      {
        string key = (string)item.Key;
        int checkvalue = (int)item.Value;
        int finalvalue = durable;
        if (key.StartsWith("Key"))
        {
          finalvalue = nondurable;
        }
        string msg3 = string.Format("Expected final value for key {0} to be {1} but was {2}", key, finalvalue, checkvalue);
        Assert.AreEqual(finalvalue, checkvalue, msg3);
      }
    }

    // this method for the ThinClientDurableTests
    public void validate(int keys, int total)
    {
      string msg1 = string.Format("Expected {0} keys but got {1}", keys, m_map.Count);
      Assert.AreEqual(keys, m_map.Count, msg1);

      string msg2 = string.Format("Expected {0} events but got {1}", total, m_ops);
      Assert.AreEqual(total, m_ops, msg2);

      int finalvalue = total/keys;

      foreach (KeyValuePair<object, object> item in m_map)
      {
        string key = (string)item.Key;
        int checkvalue = (int)item.Value;
        string msg3 = string.Format("Expected final value for key {0} to be {1} but was {2}", key, finalvalue, checkvalue);
        Assert.AreEqual(finalvalue, checkvalue, msg3);
      }
    }
    //Used for DurableAndNonDurableBasic 
    public void validateBasic(int keyCount, int eventCount, int durableValue, int nonDurableValue)
    {
      string msg1 = string.Format("Expected {0} keys but got {1}", keyCount, m_map.Count);
      Assert.AreEqual(keyCount, m_map.Count, msg1);

      string msg2 = string.Format("Expected {0} events but got {1}", eventCount, m_ops);
      Assert.AreEqual(eventCount, m_ops, msg2);

      foreach (KeyValuePair<object, object> item in m_map)
      {
        string key = (string)item.Key;
        int checkvalue = (int)item.Value;
  
        int finalvalue;
        if (key.StartsWith("D-") || key.StartsWith("LD-")) { // durable key 
          finalvalue = durableValue;
        }
        else {
          finalvalue = nonDurableValue;
        }

        string msg3 = string.Format("Expected final value for key {0} to be {1} but was {2}", key, finalvalue, checkvalue);

        Assert.AreEqual(finalvalue, checkvalue, msg3);
      }
    } 

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      Util.Log("Called AfterCreate()");
      check(ev);
    }

    public virtual void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      Util.Log("Called AfterUpdate()");
      check(ev);
    }

    public virtual void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      //Increment only count
      Util.Log("Called AfterDestroy()");
      m_ops++;
    }

    public virtual void AfterInvalidate(EntryEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionDestroy(RegionEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionClear(RegionEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      Util.Log("DurableListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public virtual void Close(IRegion<TKey, TVal> region) { }
    public void AfterRegionDisconnected(IRegion<TKey, TVal> region) { }
    #endregion
  }
}

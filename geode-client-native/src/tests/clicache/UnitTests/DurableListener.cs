//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  class DurableListener : ICacheListener
  {
    #region Private members

    private int m_ops = 0;
    private Dictionary<ICacheableKey, IGFSerializable> m_map = new Dictionary<ICacheableKey,IGFSerializable>();
    
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

    public static DurableListener Create()
    {
      Util.Log(" DurableListener Created");

      return new DurableListener();
    }

    private void check(EntryEvent ev)
    {
      m_ops++;

      CacheableString key = ev.Key as CacheableString;
      CacheableInt32 value = ev.NewValue as CacheableInt32;

      //if (m_map.ContainsKey(key))
      //{
      //  CacheableInt32 old = m_map[key] as CacheableInt32;
      //  Assert.AreEqual(value.Value, old.Value + 1, "Duplicate or older value received");
      //}

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

      foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in m_map)
      {
        CacheableString key = item.Key as CacheableString;
        CacheableInt32 checkvalue = item.Value as CacheableInt32;
        int finalvalue = durable;
        if (key.Value.StartsWith("Key"))
        {
          finalvalue = nondurable;
        }
        string msg3 = string.Format("Expected final value for key {0} to be {1} but was {2}", key.Value, finalvalue, checkvalue.Value);
        Assert.AreEqual(finalvalue, checkvalue.Value, msg3);
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

      foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in m_map)
      {
        CacheableString key = item.Key as CacheableString;
        CacheableInt32 checkvalue = item.Value as CacheableInt32;
        string msg3 = string.Format("Expected final value for key {0} to be {1} but was {2}", key.Value, finalvalue, checkvalue.Value);
        Assert.AreEqual(finalvalue, checkvalue.Value, msg3);
      }
    }
    //Used for DurableAndNonDurableBasic 
    public void validateBasic(int keyCount, int eventCount, int durableValue, int nonDurableValue)
    {
      string msg1 = string.Format("Expected {0} keys but got {1}", keyCount, m_map.Count);
      Assert.AreEqual(keyCount, m_map.Count, msg1);

      string msg2 = string.Format("Expected {0} events but got {1}", eventCount, m_ops);
      Assert.AreEqual(eventCount, m_ops, msg2);

      foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in m_map)
      {
        CacheableString key = item.Key as CacheableString;
        CacheableInt32 checkvalue = item.Value as CacheableInt32;
        int finalvalue;
        if (key.Value.StartsWith("D-") || key.Value.StartsWith("LD-")) { // durable key 
          finalvalue = durableValue;
        }
        else {
          finalvalue = nonDurableValue;
        }

        string msg3 = string.Format("Expected final value for key {0} to be {1} but was {2}", key.Value, finalvalue, checkvalue.Value);

        Assert.AreEqual(finalvalue, checkvalue.Value, msg3);
      }
    } 

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent ev)
    {
      check(ev);
    }

    public virtual void AfterUpdate(EntryEvent ev)
    {
      check(ev);
    }

    public virtual void AfterDestroy(EntryEvent ev)
    {
      //Increment only count
      m_ops++;
    }

    public virtual void AfterInvalidate(EntryEvent ev) { }

    public virtual void AfterRegionDestroy(RegionEvent ev) { }

    public virtual void AfterRegionClear(RegionEvent ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent ev) { }

    public virtual void AfterRegionLive(RegionEvent ev) {
      Util.Log("DurableListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public virtual void Close(Region region) { }
    public void AfterRegionDisconnected(Region region){ }
    #endregion
  }
}


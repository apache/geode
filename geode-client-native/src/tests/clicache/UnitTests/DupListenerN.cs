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
  using GemStone.GemFire.Cache.Generic;
  using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  class DupListener<TKey, TVal> : ICacheListener<TKey, TVal>
  {
    #region Private members

    private int m_ops = 0;
    private Dictionary<object, object> m_map = new Dictionary<object, object>();
    //ICacheableKey, IGFSerializable

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

    public static DupListener<TKey, TVal> Create()
    {
      return new DupListener<TKey, TVal>();
    }

    private void check(EntryEvent<TKey, TVal> ev)
    {
      m_ops++;

      object key = (object)ev.Key;
      object value = (object)ev.NewValue;

      //string key = ev.Key();
      //int value = ev.NewValue;
      if (m_map.ContainsKey(key))
      {
        int old = (int)m_map[key];
        Assert.AreEqual(value/*.Value*/, old/*.Value*/ + 1, "Duplicate or older value received");
      }

      m_map[key] = value;
    }

    public void validate()
    {
      Assert.AreEqual(4, m_map.Count, "Expected 4 keys for the region");
      Assert.AreEqual(400, m_ops, "Expected 400 events (100 per key) for the region");

      foreach (object item in m_map.Values)
      {
        //CacheableInt32 checkval = item as CacheableInt32;
        int checkval = (int)item;
        Assert.AreEqual(100, checkval, "Expected final value to be 100");
      }
    }

    #region ICacheListener Members

    public virtual void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    public virtual void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    public virtual void AfterDestroy(EntryEvent<TKey, TVal> ev) { }

    public virtual void AfterInvalidate(EntryEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionDestroy(RegionEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionClear(RegionEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev) { }

    public virtual void AfterRegionLive(RegionEvent<TKey, TVal> ev) { }

    public virtual void Close(IRegion<TKey, TVal> region) { }
    public virtual void AfterRegionDisconnected(IRegion<TKey, TVal> region) { }

    #endregion
  }
}


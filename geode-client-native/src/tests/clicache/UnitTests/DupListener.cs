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

  class DupListener : ICacheListener
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

    public static DupListener Create()
    {
      return new DupListener();
    }

    private void check(EntryEvent ev)
    {
      m_ops++;

      CacheableString key = ev.Key as CacheableString;
      CacheableInt32 value = ev.NewValue as CacheableInt32;

      if (m_map.ContainsKey(key))
      {
        CacheableInt32 old = m_map[key] as CacheableInt32;
        Assert.AreEqual(value.Value, old.Value + 1, "Duplicate or older value received");
      }

      m_map[key] = value;
    }
    
    public void validate()
    {
      Assert.AreEqual(4, m_map.Count, "Expected 4 keys for the region");
      Assert.AreEqual(400, m_ops, "Expected 400 events (100 per key) for the region");

      foreach (IGFSerializable item in m_map.Values)
      {
        CacheableInt32 checkval = item as CacheableInt32;
        Assert.AreEqual(100, checkval.Value, "Expected final value to be 100");
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

    public virtual void AfterDestroy(EntryEvent ev) { }

    public virtual void AfterInvalidate(EntryEvent ev) { }

    public virtual void AfterRegionDestroy(RegionEvent ev) { }

    public virtual void AfterRegionClear(RegionEvent ev) { }

    public virtual void AfterRegionInvalidate(RegionEvent ev) { }

    public virtual void AfterRegionLive(RegionEvent ev) { }

    public virtual void Close(Region region) { }
    public virtual void AfterRegionDisconnected(Region region){ }

    #endregion
  }
}


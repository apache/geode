//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;

  using GemStone.GemFire.Cache;
  using GemStone.GemFire.Cache.Generic;

  class TallyLoader<TKey, TVal> : ICacheLoader<TKey, TVal>
  {
    #region Private members

    private int m_loads = 0;

    #endregion

    #region Public accessors

    public int Loads
    {
      get
      {
        return m_loads;
      }
    }

    #endregion

    public int ExpectLoads(int expected)
    {
      int tries = 0;
      while ((m_loads < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_loads;
    }

    public void Reset()
    {
      m_loads = 0;
    }

    public void ShowTallies()
    {
      Util.Log("TallyLoader state: (loads = {0})", Loads);
    }

    public static TallyLoader<TKey, TVal> Create()
    {
      return new TallyLoader<TKey, TVal>();
    }

    public virtual int GetLoadCount()
    {
      return m_loads;
    }

    #region ICacheLoader<TKey, TVal> Members

    public virtual TVal Load(IRegion<TKey, TVal> region, TKey key, object helper)
    {
      m_loads++;
      Util.Log("TallyLoader Load: (m_loads = {0}) TYPEOF key={1} val={2} for region {3}",
        m_loads, typeof(TKey), typeof(TVal), region.Name);
      if (typeof(TVal) == typeof(string))
      {
        return (TVal) (object) m_loads.ToString();
      }
      if (typeof(TVal) == typeof(int))
      {
        return (TVal)(object) m_loads;
      }
      return default(TVal);
    }

    public virtual void Close(IRegion<TKey, TVal> region)
    {
      Util.Log("TallyLoader<TKey, TVal>::Close");
    }

    #endregion
  }
}

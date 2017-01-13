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

  class TallyResolver<TKey, TVal> : IPartitionResolver<TKey, TVal>
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
      Util.Log("TallyResolver state: (loads = {0})", Loads);
    }

    public static TallyResolver<TKey, TVal> Create()
    {
      return new TallyResolver<TKey, TVal>();
    }

    public virtual int GetLoadCount()
    {
      return m_loads;
    }

    #region IPartitionResolver<TKey, TVal> Members

    public virtual Object GetRoutingObject(EntryEvent<TKey, TVal> ev)
    {
      m_loads++;
      Util.Log("TallyResolver Load: (m_loads = {0}) TYPEOF key={1} val={2} for region {3}",
        m_loads, typeof(TKey), typeof(TVal), ev.Region.Name);
      return ev.Key;
    }

    public virtual string GetName()
    {
      Util.Log("TallyResolver<TKey, TVal>::GetName");
	    return "TallyResolver";
    }

    #endregion
  }
}

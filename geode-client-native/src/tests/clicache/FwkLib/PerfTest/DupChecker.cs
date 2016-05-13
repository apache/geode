//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class DupChecker : ICacheListener, IDisposable
  {
    /*
  
    Note:
  
    Currently, for failoverTestHAEventIDMap.xml, PerfTests.DoSerialPuts has a hardcoded
    keycount of 1000 and values are put serially from 1 to 1000.
  
    */

    #region Private members

    int m_ops = 0;
    Dictionary<ICacheableKey, IGFSerializable> m_map = new Dictionary<ICacheableKey,IGFSerializable>();

    #endregion

    #region Private methods

    private void check(EntryEvent ev)
    {
      m_ops++;

      CacheableInt32 key = ev.Key as CacheableInt32;
      CacheableInt32 value = ev.NewValue as CacheableInt32;

      FwkTest currTest = FwkTest.CurrentTest;

      if (m_map.ContainsKey(key))
      {
        CacheableInt32 old = m_map[key] as CacheableInt32;

        currTest.FwkAssert(value.Value == old.Value + 1, "DupChecker: Duplicate detected. Existing value is {0}, New value is {1}",
          old.Value, value.Value);
      }

      m_map[key] = value;
    }
    
    private void validate()
    {
      FwkTest currTest = FwkTest.CurrentTest;

      currTest.FwkInfo("DupChecker: got {0} keys.", m_map.Count);

      currTest.FwkAssert(m_map.Count == 1000, "DupChecker: Expected 1000 keys for the region, actual is {0}.", m_map.Count);

      currTest.FwkInfo("DupChecker: got {0} ops.", m_ops);

      currTest.FwkAssert(m_ops == 1000000, "DupChecker: Expected 1,000,000 events (1000 per key) for the region, actual is {0}.",
        m_ops);

      foreach (IGFSerializable item in m_map.Values)
      {
        CacheableInt32 checkval = item as CacheableInt32;
        currTest.FwkAssert(checkval.Value == 1000, "DupChecker: Expected 1000 as final value, actual is {0}.", checkval.Value);
      }
    }

    #endregion

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      check(ev);
    }

    public void AfterDestroy(EntryEvent ev)
    {
    }

    public void AfterInvalidate(EntryEvent ev)
    {
    }

    public void AfterRegionClear(RegionEvent ev)
    {
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
    }

    public void AfterRegionLive(RegionEvent ev)
    {
    }

    public void AfterUpdate(EntryEvent ev)
    {
      check(ev);
    }

    public void Close(Region region)
    {
    }
    public void AfterRegionDisconnected(Region region)
    {
    }
    #endregion

    protected virtual void Dispose(bool disposing)
    {
      FwkTest currTest = FwkTest.CurrentTest;
      currTest.FwkInfo("DupChecker: validating");
      validate();
    }

    #region IDisposable Members

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    #endregion

    public DupChecker()
    {
      FwkTest currTest = FwkTest.CurrentTest;
      currTest.FwkInfo("DupChecker: created");
    }
    
    ~DupChecker()
    {
      Dispose(false);
    }
  }
}

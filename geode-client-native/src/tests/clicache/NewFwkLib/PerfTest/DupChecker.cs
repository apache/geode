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
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;


  public class DupChecker<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
  {
    /*
  
    Note:
  
    Currently, for failoverTestHAEventIDMap.xml, PerfTests.DoSerialPuts has a hardcoded
    keycount of 1000 and values are put serially from 1 to 1000.
  
    */

    #region Private members

    int m_ops = 0;
    Dictionary<TKey, TVal> m_map = new Dictionary<TKey, TVal>();
    
    #endregion

    #region Private methods

    private void check(EntryEvent<TKey, TVal> ev)
    {
      m_ops++;

      TKey key = ev.Key;
      TVal value = ev.NewValue;

      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;

      if (m_map.ContainsKey((TKey)key))
      {
        TVal old = m_map[(TKey)key];

        currTest.FwkAssert(value.Equals(old) , "DupChecker: Duplicate detected. Existing value is {0}, New value is {1}",
          old, value);
      }

      m_map[(TKey)key] = value;
    }
    
    private void validate()
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;

      currTest.FwkInfo("DupChecker: got {0} keys.", m_map.Count);

      currTest.FwkAssert(m_map.Count == 1000, "DupChecker: Expected 1000 keys for the region, actual is {0}.", m_map.Count);

      currTest.FwkInfo("DupChecker: got {0} ops.", m_ops);

      currTest.FwkAssert(m_ops == 1000000, "DupChecker: Expected 1,000,000 events (1000 per key) for the region, actual is {0}.",
        m_ops);

      foreach (object item in m_map.Values)
      {
        int checkval = (int)item;
        currTest.FwkAssert(checkval == 1000, "DupChecker: Expected 1000 as final value, actual is {0}.", checkval);
      }
    }

    #endregion

    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    #endregion

    protected virtual void Dispose(bool disposing)
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
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
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      currTest.FwkInfo("DupChecker: created");
    }
    
    ~DupChecker()
    {
      Dispose(false);
    }
  }
}

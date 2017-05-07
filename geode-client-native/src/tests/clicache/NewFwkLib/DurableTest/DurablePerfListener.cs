//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;


  public class DurablePerfListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
  {
    #region Private members

    long m_ops = 0;
    long m_minTime;
    DateTime m_prevTime;
    DateTime m_startTime;
    
    #endregion

    #region Private methods

    private void recalculate(EntryEvent<TKey, TVal> ev)
    {
      if (m_ops == 0)
      {
        m_startTime = DateTime.Now;
      }

      m_ops++;

      if (m_ops % 1000 == 0)
      {
        FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
        currTest.FwkInfo("DurablePerfListener : m_ops = " + m_ops);
      }

      DateTime currTime = DateTime.Now;
      TimeSpan elapsedTime = currTime - m_prevTime;

      long diffTime = elapsedTime.Milliseconds;

      if (diffTime < m_minTime)
      {
        m_minTime = diffTime;
      }

      m_prevTime = currTime;
    }
    
    #endregion

    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      recalculate(ev);
    }


    public override void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      currTest.FwkInfo("DurablePerfListener: AfterRegionLive invoked.");
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      recalculate(ev);
    }

    public override void Close(IRegion<TKey, TVal> region)
    {
    }
    public override void AfterRegionDisconnected(IRegion<TKey, TVal> region)
    {
    }
    #endregion

    protected virtual void Dispose(bool disposing)
    {
    }

    #region IDisposable Members

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    #endregion

    public void logPerformance()
    {
      TimeSpan totalTime = m_prevTime - m_startTime;
      double averageRate = m_ops / totalTime.TotalSeconds;
      double maxRate = 1000.0 / m_minTime;
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      currTest.FwkInfo("DurablePerfListener: # events = {0}, max rate = {1} events per second.", m_ops, maxRate);
      currTest.FwkInfo("DurablePerfListener: average rate = {0} events per second, total time {1} seconds", averageRate, totalTime.TotalSeconds);
    }

    public DurablePerfListener()
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      currTest.FwkInfo("DurablePerfListener: created");
      m_minTime = 99999999;
      m_prevTime = DateTime.Now;
    }

    ~DurablePerfListener()
    {
      Dispose(false);
    }
  }
}

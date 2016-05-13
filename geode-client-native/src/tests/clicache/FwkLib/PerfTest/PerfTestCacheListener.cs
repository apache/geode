//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class PerfTestCacheListener : ICacheListener, IDisposable
  {
    #region Private members

    int m_numAfterCreate;
    int m_numAfterUpdate;
    int m_numAfterInvalidate;
    int m_numAfterDestroy;
    int m_numAfterRegionInvalidate;
    int m_numAfterRegionClear;
    int m_numAfterRegionDestroy;
    int m_numClose;
    int m_sleep;

    #endregion

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterCreate;
    }

    public void AfterDestroy(EntryEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterDestroy;

    }

    public void AfterInvalidate(EntryEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterInvalidate;
    }

    public void AfterRegionClear(RegionEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterRegionClear;
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterRegionDestroy;
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterRegionInvalidate;
    }

    public void AfterRegionLive(RegionEvent ev)
    {
      // TODO: VJR: increment a counter here if needed
    }

    public void AfterUpdate(EntryEvent ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterUpdate;
    }

    public void Close(Region region)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numClose;
    }
    public void AfterRegionDisconnected(Region region)
    {
      //Implement if needed
    }
    public void Reset(int sleepTime)
    {
      m_sleep = sleepTime;
      m_numAfterCreate = m_numAfterUpdate = m_numAfterInvalidate = 0;
      m_numAfterDestroy = m_numAfterRegionClear = m_numAfterRegionInvalidate =
        m_numAfterRegionDestroy = m_numClose = 0;
    }

    #endregion

    protected virtual void Dispose(bool disposing)
    {
      FwkTest.CurrentTest.FwkInfo("PerfTestCacheListener invoked afterCreate: {0}," +
        " afterUpdate: {1}, afterInvalidate: {2}, afterDestroy: {3}," +
        " afterRegionInvalidate: {4}, afterRegionDestroy: {5}, region close: {6}, afterRegionClear: {7}",
        m_numAfterCreate, m_numAfterUpdate, m_numAfterInvalidate, m_numAfterDestroy,
        m_numAfterRegionInvalidate, m_numAfterRegionDestroy, m_numClose,
	m_numAfterRegionClear);
    }

    #region IDisposable Members

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    #endregion

    ~PerfTestCacheListener()
    {
      Dispose(false);
    }
  }
  public class ConflationTestCacheListener : ICacheListener
  {
    int m_numAfterCreate;
    int m_numAfterUpdate;
    int m_numAfterInvalidate;
    int m_numAfterDestroy;
    
    public ConflationTestCacheListener() 
    {
      FwkTest.CurrentTest.FwkInfo("Calling non durable client ConflationTestCacheListener");
      m_numAfterCreate = 0;
      m_numAfterUpdate = 0;
      m_numAfterInvalidate = 0;
      m_numAfterDestroy = 0;
      
    } 
    
    public static ICacheListener Create()
    {
      return new ConflationTestCacheListener();
    }
    ~ConflationTestCacheListener() { }
    
    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      ++m_numAfterCreate;
    }

    public void AfterUpdate(EntryEvent ev)
    {
      ++m_numAfterUpdate;
     }
    public void AfterRegionLive(RegionEvent ev)
    {
      FwkTest.CurrentTest.FwkInfo("ConflationTestCacheListener: AfterRegionLive invoked");
    }
    public void AfterDestroy(EntryEvent ev)
    {
      ++m_numAfterDestroy;
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      ++m_numAfterInvalidate;
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      dumpToBB(ev.Region);
    }

    public void AfterRegionClear(RegionEvent ev)
    {
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
    }

    public void Close(Region region)
    {
    }
    public void AfterRegionDisconnected(Region region)
    {
    }
    private void dumpToBB(Region region)
    {
      FwkTest.CurrentTest.FwkInfo("dumping non durable client data on BB for ConflationTestCacheListener");
      Util.BBSet("ConflationCacheListener", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterCreate);
      Util.BBSet("ConflationCacheListener", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterUpdate);
      Util.BBSet("ConflationCacheListener", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterDestroy);
      Util.BBSet("ConflationCacheListener", "AFTER_INVALIDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterInvalidate);
    }
    #endregion
  }
}

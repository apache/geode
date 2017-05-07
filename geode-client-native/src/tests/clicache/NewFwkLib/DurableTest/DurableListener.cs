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

  public class DurableListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
  {
    #region Private members

    int m_ops = 0;
    string m_clientName = null;
    Int32 m_prevValue = 0;
    bool m_result = true;
    string m_err;

    #endregion

    #region Private methods

    private void check(EntryEvent<TKey, TVal> ev)
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;

     // CacheableInt32 value = ev.NewValue as CacheableInt32;
      Int32 value = (Int32)(object)ev.NewValue;

      if (value <= m_prevValue) // duplicate
      {
        //currTest.FwkInfo("DurableListener : duplicate value: " + value.Value);
      }
      else if (value == m_prevValue + 1) // desired
      {
        m_ops++;
        m_prevValue++;
      }
      else // event missed
      {
        currTest.FwkInfo("Missed event, expected {0}, actual {1}", m_prevValue + 1, value);
        m_prevValue = value;
        m_ops++;
        if (m_result)
        {
          m_result = false;
          m_err = String.Format("Missed event with value {0}", value);
          currTest.FwkInfo("Missed event, expected {0}, actual {1}", m_prevValue + 1, value);
        }
      }
    }
    
    private void dumpToBB()
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      currTest.FwkInfo("DurableListener: updating blackboard, ops = {0}, prev = {1}", m_ops, m_prevValue);

      // increment count
      string bbkey = m_clientName + "_Count";
      Int32 current = 0;
      try
      {
        current = (Int32)Util.BBGet("DURABLEBB", bbkey);
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        currTest.FwkInfo("Key not found for DURABLEBB {0}", bbkey);
      }
      current += m_ops;
      Util.BBSet("DURABLEBB", bbkey, current);
      currTest.FwkInfo("Current count for " + bbkey + " is " + current);

      // set current index
      string clientIndexKey = m_clientName + "_IDX";
      Util.BBSet("DURABLEBB", clientIndexKey, m_prevValue);

      // store error message
      if (!m_result && m_err != null && m_err.Length > 0)
      {
        string clientErrorKey = m_clientName + "_ErrMsg";
        string clientErrorVal = null;
        try
        {
          clientErrorVal = (string)Util.BBGet("DURABLEBB", clientErrorKey);
        }
        catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
        {
          currTest.FwkInfo("Key not found for DURABLEBB {0}", clientErrorKey);
        }
        if (clientErrorVal == null || clientErrorVal.Length <= 0)
        {
          Util.BBSet("DURABLEBB", clientErrorKey, m_err);
        }
      }
    }

    #endregion

    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey,TVal> ev)
    {
      check(ev);
    }

    public override void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      currTest.FwkInfo("DurableListener: AfterRegionLive invoked");
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    public override void Close(IRegion<TKey, TVal> region)
    {
      dumpToBB();
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

    public DurableListener()
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest; 
      m_clientName = String.Format("ClientName_{0}", Util.ClientNum);
      string clientIndexKey = m_clientName + "_IDX";
      try
      {
        m_prevValue = (Int32)Util.BBGet("DURABLEBB", clientIndexKey);
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        m_prevValue = 0;
        currTest.FwkInfo("Key not found for DURABLEBB {0}", clientIndexKey);
      }
      currTest.FwkInfo("DurableListener created for client {0} with prev val {1}", m_clientName, m_prevValue);
    }

    ~DurableListener()
    {
      Dispose(false);
    }
  }

  public class ConflationTestCacheListenerDC<TKey, TVal> : ICacheListener<TKey, TVal>
  {
    int m_numAfterCreate;
    int m_numAfterUpdate;
    int m_numAfterInvalidate;
    int m_numAfterDestroy;

    public ConflationTestCacheListenerDC()
    {
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Calling durable client ConflationTestCacheListener");
      m_numAfterCreate = 0;
      m_numAfterUpdate = 0;
      m_numAfterInvalidate = 0;
      m_numAfterDestroy = 0;
    }

    public static ICacheListener<TKey, TVal> Create()
    {
      return new ConflationTestCacheListenerDC<TKey, TVal>();
    }
    ~ConflationTestCacheListenerDC() { }
    
    #region ICacheListener Members

    public void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterCreate;
    }

    public void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterUpdate;
    }
    public void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("ConflationTestCacheListener: AfterRegionLive invoked");
    }
    public void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterDestroy;
    }

    public void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterInvalidate;
    }

    public void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      dumpToBB(ev.Region);
    }

    public void AfterRegionClear(RegionEvent<TKey, TVal> ev)
    {
    }

    public void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev)
    {
    }

    public void Close(IRegion<TKey, TVal> region)
    {
    }
    public void AfterRegionDisconnected(IRegion<TKey, TVal> region)
    {
    }
    private void dumpToBB(IRegion<TKey, TVal> region)
    {
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("dumping durable client data on BB for ConflationTestCacheListener");
      Util.BBSet("ConflationCacheListener", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterCreate);
      Util.BBSet("ConflationCacheListener", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterUpdate);
      Util.BBSet("ConflationCacheListener", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterDestroy);
      Util.BBSet("ConflationCacheListener", "AFTER_INVALIDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterInvalidate);
    }
    #endregion
  }
}

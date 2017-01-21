/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Threading;

namespace Apache.Geode.Client.FwkLib
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;
  //using Region = Apache.Geode.Client.IRegion<Object, Object>;

  public class PerfTestCacheListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
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

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterCreate;
    }

    public override void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterDestroy;

    }

    public override void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterInvalidate;
    }

    public override void AfterRegionClear(RegionEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterRegionClear;
    }

    public override void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterRegionDestroy;
    }

    public override void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterRegionInvalidate;
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numAfterUpdate;
    }

    public override void Close(IRegion<TKey, TVal> region)
    {
      if (m_sleep > 0)
      {
        Thread.Sleep(m_sleep);
      }
      ++m_numClose;
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
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("PerfTestCacheListener invoked afterCreate: {0}," +
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
  public class ConflationTestCacheListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>
  {
    int m_numAfterCreate;
    int m_numAfterUpdate;
    int m_numAfterInvalidate;
    int m_numAfterDestroy;
    
    public ConflationTestCacheListener() 
    {
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Calling non durable client ConflationTestCacheListener");
      m_numAfterCreate = 0;
      m_numAfterUpdate = 0;
      m_numAfterInvalidate = 0;
      m_numAfterDestroy = 0;
      
    }

    public static ICacheListener<TKey, TVal> Create()
    {
      return new ConflationTestCacheListener<TKey, TVal>();
    }
    ~ConflationTestCacheListener() { }
    
    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterCreate;
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterUpdate;
     }
    public override void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("ConflationTestCacheListener: AfterRegionLive invoked");
    }
    public override void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterDestroy;
    }

    public override void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterInvalidate;
    }

    public override void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      dumpToBB(ev.Region);
    }

    private void dumpToBB(IRegion<TKey, TVal> region)
    {
      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("dumping non durable client data on BB for ConflationTestCacheListener");
      Util.BBSet("ConflationCacheListener", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterCreate);
      Util.BBSet("ConflationCacheListener", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterUpdate);
      Util.BBSet("ConflationCacheListener", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterDestroy);
      Util.BBSet("ConflationCacheListener", "AFTER_INVALIDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterInvalidate);
    }
    #endregion
  }
}

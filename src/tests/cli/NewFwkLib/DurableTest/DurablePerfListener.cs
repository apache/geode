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
using System.Collections.Generic;
using System.Threading;

namespace Apache.Geode.Client.FwkLib
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client.Generic;


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

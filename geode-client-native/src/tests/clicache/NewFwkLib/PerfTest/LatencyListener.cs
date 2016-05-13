//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  public class LatencyListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
  {
    #region Private members

    long m_maxLatency;
    long m_minLatency = long.MaxValue;
    long m_totLatency;
    int m_samples;
    int m_numAfterCreate;
    int m_numAfterUpdate;

    #endregion

    #region Private methods

    private void UpdateLatency(EntryEvent<TKey, TVal> ev)
    {
      long now = DateTime.Now.Ticks;
      TVal newVal = ev.NewValue;
      byte[] buffer = newVal as byte[];
      if (buffer != null && buffer.Length >= (int)(sizeof(int) + sizeof(long)))
      {
        uint mark = BitConverter.ToUInt32(buffer, 0);
        if ((mark == LatencyPutsTask<TKey, TVal>.LatMark))
        {
          long sendTime = BitConverter.ToInt64(buffer, 4);
          long latency = Math.Abs(DateTime.Now.Ticks - sendTime);
          m_minLatency = Math.Min(latency, m_minLatency);
          m_maxLatency = Math.Max(latency, m_maxLatency);
          m_totLatency += latency;
          m_samples++;
          //FwkTest.CurrentTest.FwkInfo("LatencyListener::Average: {0}",
          //  (int)(m_totLatency / (long)m_samples));
        }
      }
    }

    #endregion

    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterCreate;
      UpdateLatency(ev);
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      ++m_numAfterUpdate;
      UpdateLatency(ev);
    }

    
    #endregion

    protected virtual void Dispose(bool disposing)
    {
      string tag;
      try
      {
        FwkTaskData taskData = (FwkTaskData)Util.BBGet(
          "LatencyBB", "LatencyTag");
        tag = taskData.GetLogString();
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        tag = null;
      }
      if (tag == null)
      {
        tag = "No tag found";
      }
      long avgLatency = 0;
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      if (m_samples != 0)
      {
        avgLatency = m_totLatency / (long)m_samples;
        currTest.FwkInfo("LatencyCSV,MinMaxAvgSamples,{0},{1},{2},{3},{4}",
          tag, m_minLatency, m_maxLatency, avgLatency, m_samples);
        currTest.FwkInfo("LatencySuite: {0} results: {1} min, {2} max, {3} avg, {4} samples. {5}",
          tag, m_minLatency, m_maxLatency, avgLatency, m_samples, m_totLatency);
        currTest.FwkInfo("Latency listener counters for {0}  afterCreate: {1}, afterUpdate: {2}",
          tag, m_numAfterCreate, m_numAfterUpdate);
      }
      else
      {
        currTest.FwkInfo("LatencySuite: {0} results: NO DATA SAMPLES TO REPORT ON.", tag);
        currTest.FwkInfo("Latency listener counters for {0}  afterCreate: {1}, afterUpdate: {2}",
          tag, m_numAfterCreate, m_numAfterUpdate);
      }
    }

    #region IDisposable Members

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    #endregion

    ~LatencyListener()
    {
      Dispose(false);
    }
  }
}

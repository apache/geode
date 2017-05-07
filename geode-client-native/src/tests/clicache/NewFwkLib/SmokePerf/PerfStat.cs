//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using GemStone.GemFire.Cache.Tests;
using GemStone.GemFire.DUnitFramework;
namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.Cache.Generic;
  public static class PerfOps {
    public const string PERF_CREATES = "creates";
    public const string PERF_CREATE_TIME = "createTime";
    public const string PERF_PUTS="puts";
    public const string PERF_PUT_TIME="putTime";
    public const string PERF_UPDATE_EVENTS="updateEvents";
    public const string PERF_UPDATE_LATENCY="updateLatency";
    public const string PERF_LATENCY_SPIKES = "latencySpikes";
    public const string PERF_NEGATIVE_LATENCIES = "negativeLatencies";
    public const string PERF_OPS = "operations";
    public const string PERF_OP_TIME = "operationTime";
    public const string PERF_CONNECTS = "connects";
    public const string PERF_CONNECT_TIME = "connectTime";
    public const string PERF_DISCONNECTS = "disconnects";
    public const string PERF_DISCONNECT_TIME = "disconnectTime";
    public const string PERF_GETS = "gets";
    public const string PERF_GET_TIME = "getTime";
    public const string PERF_QUERIES = "queries";
    public const string PERF_QUERY_TIME = "queryTime";
    public const string PERF_UPDATES = "updates";
    public const string PERF_UPDATES_TIME = "updateTime";
  }
  public class PerfStat
  {
    private Statistics testStat;
    private StatisticsType statsType;
    private long ReturnTime()
    {
      DateTime startTime = DateTime.Now;
      //Stopwatch System.Diagnostics.Stopwatch;
      
      return startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
    }
    public PerfStat(int threadID)
    {
      PerfStatType regStatType = PerfStatType.GetInstance();
      statsType = regStatType.GetStatType();
      StatisticsFactory factory = StatisticsFactory.GetExistingInstance();
      string buf = String.Format("ThreadId-{0}", threadID);
      testStat = factory.CreateStatistics(statsType, buf);
    }
    public long StartCreate()
    {
      return ReturnTime();
    }
    public void EndCreate(long start, bool isMainWorkload)
    {
      EndCreate(start, 1, isMainWorkload);
    }
    public void EndCreate(long start, int amount, bool isMainWorkload)
    {
      long elapsed = ReturnTime() - start;
      if (isMainWorkload)
      {
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_OPS), amount);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_OP_TIME), elapsed);
      }
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_CREATES), amount);
      testStat.IncLong(statsType.NameToId(PerfOps.PERF_CREATE_TIME), elapsed);
    }
    public long StartPut()
    {
      return ReturnTime();
    }
    public void EndPut(long start, bool isMainWorkload)
    {
      EndPut(start, 1, isMainWorkload);
    }
    public void EndPut(long start, int amount, bool isMainWorkload)
    {
      long elapsed = ReturnTime() - start;
      if(isMainWorkload){
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_OPS), amount);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_OP_TIME), elapsed);
      }
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_PUTS), amount);
      testStat.IncLong(statsType.NameToId(PerfOps.PERF_PUT_TIME), elapsed);
    }
    public long StartGet()
    {
       return ReturnTime();
    }
    public void EndGet(long start, bool isMainWorkload)
    {
      EndGet(start,1,isMainWorkload);
    }
    public void EndGet(long start, int amount, bool isMainWorkload)
    {
      long elapsed = ReturnTime() - start;
      if(isMainWorkload){
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_OPS), amount);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_OP_TIME), elapsed);
      }
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_GETS), amount);
      testStat.IncLong(statsType.NameToId(PerfOps.PERF_GET_TIME), elapsed);
    }
    public void Close(){
      testStat.Close();
    }
    public void IncUpdateLatency(long amount)
    {
      long nonZeroAmount = amount;
      if (nonZeroAmount == 0) { // make non-zero to ensure non-flatline
        nonZeroAmount = 1; // nanosecond
      }
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_UPDATE_EVENTS), 1);
      testStat.IncLong(statsType.NameToId(PerfOps.PERF_UPDATE_LATENCY), nonZeroAmount);
    }

    public void IncLatencySpikes(int amount)
    {
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_LATENCY_SPIKES), amount);
    }

    public void IncNegativeLatencies(int amount)
    {
     testStat.IncInt(statsType.NameToId(PerfOps.PERF_NEGATIVE_LATENCIES), amount);
    }
    public void SetOps(int amount)
    {
      testStat.SetInt(statsType.NameToId(PerfOps.PERF_OPS), amount);
    }
    public void SetOpTime(long amount)
    {
      testStat.SetLong(statsType.NameToId(PerfOps.PERF_OP_TIME), amount);
    }
    public long StartConnect()
    {
       return ReturnTime();
    }
    public void EndConnect(long start, bool isMainWorkload)
    {
      long elapsed = ReturnTime() - start;
      if(isMainWorkload){
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_OPS), 1);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_OP_TIME), elapsed);
      }
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_CONNECTS), 1);
      testStat.IncLong(statsType.NameToId(PerfOps.PERF_CONNECT_TIME), elapsed);
    }
    public Int64 GetConnectTime()
    {
       return testStat.GetLong(PerfOps.PERF_CONNECT_TIME);
    }
    public long StartQuery()
    {
      return ReturnTime();
    }
    public void EndQuery(long start, bool isMainWorkload)
    {
      EndQuery(start,1,isMainWorkload);
    }
    public void EndQuery(long start, int amount, bool isMainWorkload)
    {
      long elapsed = ReturnTime() - start;
      if(isMainWorkload){
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_OPS), amount);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_OP_TIME), elapsed);
      }
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_QUERIES), amount);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_QUERY_TIME), elapsed);
      }
    public void IncUpdateEvents(int amount)
    {
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_UPDATE_EVENTS), amount);
    }
    public int GetOps()
    {
       return testStat.GetInt(PerfOps.PERF_OPS);
    }
    /**
     * @return the timestamp that marks the start of the update
     */
    public long StartUpdate()
    {
      return ReturnTime();
    }
    public void EndUpdate(long start, bool isMainWorkload)
    {
      EndUpdate(start, 1, isMainWorkload);
    }
    public void EndUpdate(long start, int amount, bool isMainWorkload)
    {
      long elapsed = ReturnTime() - start;
      if (isMainWorkload) {
        testStat.IncInt(statsType.NameToId(PerfOps.PERF_OPS), amount);
        testStat.IncLong(statsType.NameToId(PerfOps.PERF_OP_TIME), elapsed);
      }
      testStat.IncInt(statsType.NameToId(PerfOps.PERF_UPDATES), amount);
      testStat.IncLong(statsType.NameToId(PerfOps.PERF_UPDATES_TIME), elapsed);
    }
  }
  public class PerfStatType
  {
    private static PerfStatType single = null;
    private StatisticDescriptor[] statDes = new StatisticDescriptor[20];
    private PerfStatType()
    {
    }
    public static PerfStatType GetInstance()
    { 
      if(single == null)
      {
        single = new PerfStatType();
      }
      return single;
    }
    public StatisticsType GetStatType()
    {
      StatisticsFactory m_factory = StatisticsFactory.GetExistingInstance();
      StatisticsType statsType = m_factory.FindType("cacheperf.CachePerfStats");
      if (statsType == null)
      {
        statDes[0] = m_factory.CreateIntCounter(PerfOps.PERF_PUTS, "Number of puts completed.", "operations", 1);
        statDes[1] = m_factory.CreateLongCounter(PerfOps.PERF_PUT_TIME,
          "Total time spent doing puts.", "nanoseconds", 0);
        statDes[2] = m_factory.CreateIntCounter(PerfOps.PERF_UPDATE_EVENTS,
            "Number of update events.", "events", 1);
        statDes[3] = m_factory.CreateLongCounter(PerfOps.PERF_UPDATE_LATENCY,
            "Latency of update operations.", "nanoseconds", 0);
        statDes[4] = m_factory.CreateIntCounter(PerfOps.PERF_CREATES,
            "Number of creates completed.", "operations", 1);
        statDes[5] = m_factory.CreateLongCounter(PerfOps.PERF_CREATE_TIME,
            "Total time spent doing creates.", "nanoseconds", 0);
        statDes[6] = m_factory.CreateIntCounter(PerfOps.PERF_LATENCY_SPIKES,
            "Number of latency spikes.", "spikes", 0);
        statDes[7]
            = m_factory.CreateIntCounter(
                PerfOps.PERF_NEGATIVE_LATENCIES,
                "Number of negative latencies (caused by insufficient clock skew correction).",
                "negatives", 0);
        statDes[8] = m_factory.CreateIntCounter(PerfOps.PERF_OPS,
            "Number of operations completed.", "operations", 1);
        statDes[9] = m_factory.CreateLongCounter(PerfOps.PERF_OP_TIME,
            "Total time spent doing operations.", "nanoseconds", 0);
        statDes[10] = m_factory.CreateIntCounter(PerfOps.PERF_CONNECTS,
            "Number of connects completed.", "operations", 1);
        statDes[11] = m_factory.CreateLongCounter(PerfOps.PERF_CONNECT_TIME,
            "Total time spent doing connects.", "nanoseconds", 0);
        statDes[12] = m_factory.CreateIntCounter(PerfOps.PERF_DISCONNECTS,
            "Number of disconnects completed.", "operations", 1);
        statDes[13] = m_factory.CreateLongCounter(PerfOps.PERF_DISCONNECT_TIME,
            "Total time spent doing disconnects.", "nanoseconds", 0);
        statDes[14] = m_factory.CreateIntCounter(PerfOps.PERF_GETS,
            "Number of gets completed.", "operations", 1);
        statDes[15] = m_factory.CreateLongCounter(PerfOps.PERF_GET_TIME,
            "Total time spent doing gets.", "nanoseconds", 0);
        statDes[16] = m_factory.CreateIntCounter(PerfOps.PERF_QUERIES,
                "Number of queries completed.", "operations", 1);
        statDes[17] = m_factory.CreateLongCounter(PerfOps.PERF_QUERY_TIME,
                "Total time spent doing queries.", "nanoseconds", 0);
        statDes[18] = m_factory.CreateIntCounter(PerfOps.PERF_UPDATES,
                    "Number of updates completed.", "operations", 1);
        statDes[19] = m_factory.CreateLongCounter(PerfOps.PERF_UPDATES_TIME,
                    "Total time spent doing updates.", "nanoseconds", 0);
        statsType = m_factory.CreateType("cacheperf.CachePerfStats", "Application statistics.", statDes, 20);
      }
      return statsType;
    }
    public static void Clean()
    {
      if (single != null)
      {
        single = null;
      }
    }
    
  }
}

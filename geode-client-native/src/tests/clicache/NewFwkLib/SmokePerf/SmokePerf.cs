//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using GemStone.GemFire.Cache.Tests.NewAPI;
namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;
  public class PerfCacheListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
  {
    public static Int64 LAT_MARK = 0x55667788;
    public static Int64 LATENCY_SPIKE_THRESHOLD = 10000000;
    protected PerfStat statistics = null;
    public PerfCacheListener(PerfStat perfstat)
    {
      statistics = perfstat;
    }
   
    public void RecordLatency(object objValue)
    {
      DateTime startTime = DateTime.Now;
      long now = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      long then;
      if (objValue is byte[])
      {
        then = ArrayOfByte.GetTimestamp(objValue as byte[]);
      }
      else
      {
        then = ((TimeStampdObject)objValue).GetTimestamp();
      }
      long latency = now - then;
      if (latency > LATENCY_SPIKE_THRESHOLD)
      {
        statistics.IncLatencySpikes(1);
      }
      if (latency < 0)
      {
        statistics.IncNegativeLatencies(1);
      }
      else
      {
        statistics.IncUpdateLatency(latency);
      }
    }
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

    ~PerfCacheListener()
    {
      Dispose(false);
    }
  }

  public class LatencyListeners<TKey, TVal> : PerfCacheListener<TKey, TVal>
  {
    
    public LatencyListeners(PerfStat perfstat)
      : base(perfstat)
    {
      Util.Log("LatencyListeners constructor");
    }
   
    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {

      FwkTest<TKey, TVal>.CurrentTest.FwkInfo("rjk LatencyListeners AfterUpdate invoked ");
      TKey key = ev.Key;
      TVal value = ev.NewValue;
      Util.Log("rjk: LatencyListeners AfterUpdate - 11");
      RecordLatency(value);
      Util.Log("rjk: LatencyListeners AfterUpdate - 12");
     
    }
  }

  public class CQLatencyListener<TKey, TVal> : PerfCacheListener<TKey, TVal>, ICqListener<TKey, object>
  {
    public CQLatencyListener(PerfStat perfstat)
      : base(perfstat)
    {
    }
    public void OnEvent(CqEvent<TKey, object> ev)
    {
      /*TVal*/ object value = ev.getNewValue();
      RecordLatency(value);
    }
    public void OnError(CqEvent<TKey, object> ev)
    {
    }
    public void Close()
    {
    }
  }

  public class PerfCacheLoader<TKey, TVal> : ICacheLoader<TKey, TVal>
  {
    private Int32 m_loads = 0;
    public PerfCacheLoader()
      : base()
    {
    }
    #region Public accessors
    public Int32 Loads
    {
      get
      {
        return m_loads;
      }
    }
    #endregion
    public TVal Load(IRegion<TKey, TVal> region, TKey key, object helper)
    {
      m_loads++;
      return default(TVal);
    }
    public virtual void Close(IRegion<TKey, TVal> region) { }

    // VJR: added dummy load/close placeholders.
    /*
    public IGFSerializable Load( GemStone.GemFire.Cache.Region region, ICacheableKey key, IGFSerializable helper)
    {
      return new CacheableInt32(m_loads++);
    }
    public virtual void Close( GemStone.GemFire.Cache.Region region) { }
    */
  }

  public class DurableCacheListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>, IDisposable
  {
    private Int32 m_ops = 0;
    private string m_clntName;
    
    private void check(EntryEvent<TKey, TVal> ev)
    {
      TKey key = ev.Key;
      TVal value = ev.NewValue;
      m_ops++;
    }

    public DurableCacheListener()
    {
      m_ops = 0;
      m_clntName = String.Format("ClientName_{0}", Util.ClientNum);
      Util.BBSet("DURABLEBB", m_clntName, 0);
    }

    ~DurableCacheListener()
    {
      Dispose(false);
    }

    void dumpToBB()
    {
      FwkTest<TKey, TVal> currTest = FwkTest<TKey, TVal>.CurrentTest;
      string bbkey = m_clntName;
      int current = 0;
      try
      {
        current = (int)Util.BBGet("DURABLEBB", bbkey);
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        currTest.FwkInfo("Key not found for DURABLEBB {0}", bbkey);
      }
      current += m_ops;
      Util.BBSet("DURABLEBB", bbkey, current);
      currTest.FwkInfo("Current count for " + bbkey + " is " + current);
    }

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      check(ev);
    }

    public override void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      dumpToBB();
    }
    
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
  }

  public class SmokePerf<TKey, TVal> : FwkTest<TKey, TVal>
  {
    //private string bb = "Trim_BB";
    private static readonly DateTime EpochTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime EpochTimeLocal = EpochTime.ToLocalTime();
    protected TKey[] m_keysA;
    protected int m_maxKeys;
    protected int m_keyIndexBegin;

    protected TVal[] m_cValues;
    protected int m_maxValues;
    protected char m_keyType = 'i';
    protected bool m_isObjectRegistered = false;
    protected static List<IDictionary<TKey,TVal>> maps = new List<IDictionary<TKey,TVal>>();

    protected const string ClientCount = "clientCount";
    protected const string TimedInterval = "timedInterval";
    protected const string DistinctKeys = "distinctKeys";
    protected const string NumThreads = "numThreads";
    protected const string ValueSizes = "valueSizes";
    protected const string OpsSecond = "opsSecond";
    protected const string KeyType = "keyType";
    protected const string KeySize = "keySize";
    protected const string KeyIndexBegin = "keyIndexBegin";
    protected const string RegisterKeys = "registerKeys";
    protected const string RegisterRegex = "registerRegex";
    protected const string UnregisterRegex = "unregisterRegex";
    protected const string ExpectedCount = "expectedCount";
    protected const string InterestPercent = "interestPercent";
    protected const string KeyStart = "keyStart";
    protected const string KeyEnd = "keyEnd";

    #region Protected methods

    protected void ClearKeys()
    {
      if (m_keysA != null)
      {
        for (int i = 0; i < m_keysA.Length; i++)
        {
          if (m_keysA[i] != null)
          {
            //m_keysA[i].Dispose();
            m_keysA[i] = default(TKey);
          }
        }
        m_keysA = null;
        m_maxKeys = 0;
      }
    }
    protected int InitKeys(bool useDefault, bool useAllClientID)
    {
      string typ = GetStringValue(KeyType); // int is only value to use
      char newType = (typ == null || typ.Length == 0) ? 's' : typ[0];

      int low = GetUIntValue(KeyIndexBegin);
      low = (low > 0) ? low : 0;
      //ResetKey(DistinctKeys);
      int numKeys = GetUIntValue(DistinctKeys);  // check distinct keys first
      if (numKeys <= 0)
      {
        if (useDefault)
        {
          numKeys = 5000;
        }
        else
        {
          //FwkSevere("Failed to initialize keys with numKeys: {0}", numKeys);
          return numKeys;
        }
      }
      ResetKey("clientCount");
      int numClients = GetUIntValue("clientCount");
      //Int32 id = 0;
      string id = null;
      if (numClients > 0)
      {
        id = Util.ClientId;
        //if (id < 0)
        //  id = -id;
        numKeys = numKeys / numClients;
      }
      if (numKeys < 1)
        FwkException("SmokePerf::InitKeys:Key is less than 0 for each client. Provide max number of distinctKeys");

      int high = numKeys + low;
      FwkInfo("InitKeys:: numKeys: {0}; low: {1}", numKeys, low);
      if ((newType == m_keyType) && (numKeys == m_maxKeys) &&
        (m_keyIndexBegin == low))
      {
        return numKeys;
      }

      ClearKeys();
      m_maxKeys = numKeys;
      m_keyIndexBegin = low;
      m_keyType = newType;
      if (m_keyType == 'i')
      {
        InitIntKeys(low, high);
      }
      else
      {
        int keySize = GetUIntValue(KeySize);
        keySize = (keySize > 0) ? keySize : 10;
        string keyBase = new string('A', keySize);
        InitStrKeys(low, high, keyBase, id, useAllClientID);
      }
      for (int j = 0; j < numKeys; j++)
      {
        int randIndx = Util.Rand(numKeys);
        if (randIndx != j)
        {
          TKey tmp = m_keysA[j];
          m_keysA[j] = m_keysA[randIndx];
          m_keysA[randIndx] = tmp;
        }
      }
      return m_maxKeys;
    }

    protected int InitKeys()
    {
      return InitKeys(true, false);
    }

    protected void InitStrKeys(int low, int high, string keyBase, string clientId, bool useAllClientID)
    {
      m_keysA = (TKey[])(object) new String[m_maxKeys];
      ResetKey("clientCount");
      int numClients = GetUIntValue("clientCount");
      if (numClients < 0)
        numClients = 0;
      string id = clientId.Substring(clientId.LastIndexOf('.') + 1);
      FwkInfo("m_maxKeys: {0}; low: {1}; high: {2} Client id {3} numClient {4}",
        m_maxKeys, low, high, id, numClients);
      //string id = clientId.Substring(0, clientId.LastIndexOf("."));
      //int epCount = (int)Util.BBGet(FwkTest.JavaServerBB, FwkTest.JavaServerEPCountKey); 
      for (int i = low; i < high; i++)
      {
        if (useAllClientID)
        {
          id = Convert.ToString(Util.Rand(1, (numClients + 1)));
        }
        m_keysA[i - low] = (TKey)(object)(keyBase.ToString() +id + i.ToString("D10"));
        //FwkInfo("rjk: generating key {0}", m_keysA[i - low]);
      }
    }

    protected void InitIntKeys(int low, int high)
    {
      m_keysA = (TKey[])(object)new Int32[m_maxKeys];
      FwkInfo("m_maxKeys: {0}; low: {1}; high: {2}",
        m_maxKeys, low, high);
      for (int i = low; i < high; i++)
      {
        m_keysA[i - low] = (TKey)(object)i;
      }
    }
    protected int InitBatchKeys(bool useDefault)
    {
      int low = 0;
      //ResetKey(DistinctKeys);
      int numKeys = GetUIntValue(DistinctKeys);  // check distinct keys first
      if (numKeys <= 0)
      {
        if (useDefault)
        {
          numKeys = 5000;
        }
        else
        {
          //FwkSevere("Failed to initialize keys with numKeys: {0}", numKeys);
          return numKeys;
        }
      }
      int batchSize = GetUIntValue("BatchSize");
      batchSize = (batchSize <= 0) ? 500 : batchSize;
      int high = 0;
      ClearKeys();
      m_maxKeys = numKeys;
      int batches = numKeys / batchSize;
      m_keysA = (TKey[])(object) new String[m_maxKeys];
      high = batchSize;
      FwkInfo("m_MaxKeys: {0} low: {1} high: {2}", m_maxKeys, low, high);
      for (int i = 0; i < batches; i++)
      {
        for (int j = low; j < high; j++)
        {
          string buf = String.Format("_{0}_{1}", i, j);
          m_keysA[j] = (TKey)(object)buf;
        }
        low += batchSize;
        high += batchSize;
        FwkInfo("low: {0} high: {1}", low, high);
      }
      for (int j = 0; j < numKeys; j++)
      {
        int randIndx = Util.Rand(numKeys);
        if (randIndx != j)
        {
          TKey tmp = m_keysA[j];
          m_keysA[j] = m_keysA[randIndx];
          m_keysA[randIndx] = tmp;
        }
      }

      return m_maxKeys;
    }

    protected int InitValues(int numKeys)
    {
      return InitValues(numKeys, 0, true);
    }

    protected int InitValues(int numKeys, int size, bool useDefault)
    {
      if (size == 0)
      {
        size = GetUIntValue(ValueSizes);
      }
      if (size <= 0)
      {
        if (useDefault)
        {
          size = 55;
        }
        else
        {
          return size;
        }
      }
      return size;
    }
    protected IRegion<TKey, TVal> GetRegion()
    {
      return (IRegion<TKey, TVal>)GetRegion(null);

    }
    protected IRegion<TKey, TVal> GetRegion(string regionName)
    {
      IRegion<TKey, TVal> region;
      if (regionName == null)
      {
        regionName = GetStringValue("regionName");
      }
      if (regionName == null)
      {
        region = (IRegion<TKey, TVal>)GetRootRegion();
        if (region == null)
        {
          IRegion<TKey, TVal>[] rootRegions = CacheHelper<TKey, TVal>.DCache.RootRegions<TKey, TVal>();
          if (rootRegions != null && rootRegions.Length > 0)
          {
            region = rootRegions[Util.Rand(rootRegions.Length)];
          }
        }
      }
      else
      {
        region = CacheHelper<TKey, TVal>.GetRegion(regionName);
      }
      return region;
    }
    #endregion

    #region private utility methods

    public static long GetDateTimeMillis(DateTime dt)
    {
      long numTicks;
      long numMillis, residualTicks;

      if (dt.Kind != DateTimeKind.Utc)
      {
        numTicks = dt.Ticks - EpochTimeLocal.Ticks;
      }
      else
      {
        numTicks = dt.Ticks - EpochTime.Ticks;
      }
      numMillis = numTicks / TimeSpan.TicksPerMillisecond;
      residualTicks = numTicks % TimeSpan.TicksPerMillisecond;
      // round-off to nearest millisecond in case of residual ticks
      if ((residualTicks * 2) >= TimeSpan.TicksPerMillisecond)
      {
        ++numMillis;
      }
      return numMillis;
    }

    private object SafeBBGet(string bb, string key)
    {
      try
      {
        return Util.BBGet(bb, key);
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        return null;
      }
    }
    private void checkTrimForOps(string msg,StreamWriter sw)
    {
      String st = "";

      if (File.Exists("trim.spec"))
      {
        StreamReader sr = File.OpenText("trim.spec");
        st = sr.ReadToEnd();
        sr.Close();
      }
      string regMatch = "trimspec operations start=";
      if (!(Regex.IsMatch(st, regMatch)))
      {
        sw.WriteLine(msg);
      }
    }

    private void SetTrimTime(string op)
    {
      SetTrimTime(op, false);
    }
    private void SetTrimTime(string op, bool endTime)
    {
      DateTime startTime;
      string trTime = null;
      string TemptrTime = null;
      TimeSpan diff = new TimeSpan(0, 0, 30);
      if (endTime)
      {
        startTime = DateTime.Now.Subtract(diff);
        //startTime = DateTime.Now;
        trTime = op + "_" + "EndTime";
        TemptrTime = op + "_" + "TempEndTime";
      }
      else
      {
        startTime = DateTime.Now.Add(diff);
        trTime = op + "_" + "StartTime";
        TemptrTime = op + "_" + "TempStartTime";
      }
      //long tnanoSec = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      //long tnanoSec = startTime.ToFileTimeUtc();
      long curruntMillis = GetDateTimeMillis(startTime);
      long trim_Time = 0;
      try
      {
        trim_Time = (long)Util.BBGet("Trim_BB", TemptrTime);
      }
      catch (GemStone.GemFire.DUnitFramework.KeyNotFoundException)
      {
        FwkInfo("Key not found for Trim_BB {0}", TemptrTime);
      }
      string timeZone = TimeZone.CurrentTimeZone.IsDaylightSavingTime(DateTime.Now) ? TimeZone.CurrentTimeZone.DaylightName : TimeZone.CurrentTimeZone.StandardName;
      string shortTZ = " ";
      for (Int32 i = 0; i < timeZone.Length; i++)
      {
        if (Char.IsUpper(timeZone[i]))
          shortTZ += timeZone[i];
      }
      string timeFormat = startTime.ToString("yyyy/MM/dd HH:mm:ss.FFF") +
        shortTZ + " (" + curruntMillis.ToString() + ")";
      if (trim_Time > 0)
      {
        //if (((tnanoSec > Convert.ToInt64(trim_Time)) && !endTime) || ((tnanoSec < Convert.ToInt64(trim_Time)) && endTime))
        if (((curruntMillis > trim_Time) && !endTime) || ((curruntMillis < trim_Time) && endTime))
        {
          Util.BBSet("Trim_BB", trTime, timeFormat);
          Util.BBSet("Trim_BB", TemptrTime, curruntMillis);
        }
      }
      else
      {
        Util.BBSet("Trim_BB", trTime, timeFormat);
        Util.BBSet("Trim_BB", TemptrTime, curruntMillis);
      }
    }
    private string GetQuery(int i)
    {
      IRegion<TKey,TVal> region = GetRegion();
      int strBatchSize = GetUIntValue("BatchSize");
      int maxkeys = GetUIntValue("distinctKeys");
      if ((maxkeys % strBatchSize) != 0)
        FwkException("Keys does not evenly divide");
      int batches = maxkeys / strBatchSize;
      int batchNum = (i + 1) % batches;
      string query = "SELECT * FROM " + region.FullPath + " obj WHERE obj.batch = " + Convert.ToString(batchNum);
      return query;
    }
    #endregion

    #region Public methods
    public static ICacheLoader<TKey,TVal> createCacheLoader()
    {
      return new PerfCacheLoader<TKey, TVal>();
    }
    public static ICacheListener<TKey, TVal> CreateDurableCacheListenerSP()
    {
      return new DurableCacheListener<TKey, TVal>();
    }
    public static ICacheListener<TKey, TVal> CreateLatencyListener()
    {
      return new LatencyListeners<TKey, TVal>(InitPerfStat.perfstat[0]);
    }

    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        if (!m_isObjectRegistered)
        {
          Serializable.RegisterTypeGeneric(PSTObject.CreateDeserializable);
          Serializable.RegisterTypeGeneric(FastAssetAccount.CreateDeserializable);
          Serializable.RegisterTypeGeneric(FastAsset.CreateDeserializable);
          Serializable.RegisterTypeGeneric(BatchObject.CreateDeserializable);
          Serializable.RegisterTypeGeneric(DeltaFastAssetAccount.CreateDeserializable);
          Serializable.RegisterTypeGeneric(DeltaPSTObject.CreateDeserializable);
          m_isObjectRegistered = true;
        }
        IRegion<TKey, TVal> region = CreateRootRegion();
        if (region == null)
        {
          FwkException("DoCreateRegion()  could not create region.");
        }
        FwkInfo("DoCreateRegion()  Created region '{0}'", region.Name);
      }
      catch (Exception ex)
      {
        FwkException("DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreateRegion() complete.");
    }

    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      CacheHelper<TKey, TVal>.Close();
    }

    public void DoGenerateTrimSpec()
    {
      FwkInfo("In DoGenerateTrimSpec()");

      try
      {
        StreamWriter sw = new StreamWriter("trim.spec");
        if (SafeBBGet("Trim_BB", "creates_EndTime") != null)
        {
          string msg = "trimspec creates end=" + (string)SafeBBGet("Trim_BB", "creates_EndTime") + "\n;";
          sw.WriteLine(msg);
        }
        if (SafeBBGet("Trim_BB", "reg_EndTime") != null)
        {
          string msg = "trimspec registerInterests end=" + (string)SafeBBGet("Trim_BB", "reg_EndTime") + "\n;";
          sw.WriteLine(msg);
        }
        if ((SafeBBGet("Trim_BB", "put_StartTime") != null) && (SafeBBGet(
          "Trim_BB", "put_EndTime") != null))
        {
          string msg = "trimspec puts start=" + (string)SafeBBGet("Trim_BB",
         "put_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
         "put_EndTime") + "\n;";
          sw.WriteLine(msg);
          string msg1 = "trimspec operations start="
            + (string)SafeBBGet("Trim_BB", "put_StartTime") + " end="
            + (string)SafeBBGet("Trim_BB", "put_EndTime") + "\n;";
          sw.WriteLine(msg1);
          //checkTrimForOps(msg1,sw);
        }
        if ((SafeBBGet("Trim_BB", "connects_StartTime") != null) && (SafeBBGet(
            "Trim_BB", "connects_EndTime") != null))
        {
          string msg = "trimspec connects start=" + (string)SafeBBGet("Trim_BB",
              "connects_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
              "connects_EndTime") + "\n;";
          sw.WriteLine(msg);
          string msg1 = "trimspec operations start=" + SafeBBGet("Trim_BB",
          "connects_StartTime") + " end=" + SafeBBGet("Trim_BB", "connects_EndTime") + "\n;";
          sw.WriteLine(msg1);
        }
        if ((SafeBBGet("Trim_BB", "get_StartTime") != null) && (SafeBBGet(
            "Trim_BB", "get_EndTime") != null))
        {
          string msg = "trimspec gets start=" + (string)SafeBBGet("Trim_BB",
              "get_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
              "get_EndTime") + "\n;";
          sw.WriteLine(msg);
          string msg1 = "trimspec operations start="
              + (string)SafeBBGet("Trim_BB", "get_StartTime") + " end="
              + (string)SafeBBGet("Trim_BB", "get_EndTime") + "\n;";
          sw.WriteLine(msg1);
        }
        if ((SafeBBGet("Trim_BB", "putgets_StartTime") != null) && (SafeBBGet(
            "Trim_BB", "putgets_EndTime") != null))
        {
          string msg = "trimspec putgets start=" + (string)SafeBBGet("Trim_BB",
              "putgets_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
              "putgets_EndTime") + "\n;";
          sw.WriteLine(msg);
          string msg1 = "trimspec operations start=" + (string)SafeBBGet("Trim_BB",
          "putgets_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
          "putgets_EndTime") + "\n;";
          sw.WriteLine(msg1);
        }
        if ((SafeBBGet("Trim_BB", "queries_StartTime") != null) && (SafeBBGet(
                "Trim_BB", "queries_EndTime") != null))
        {
          string msg = "trimspec queries start=" + (string)SafeBBGet("Trim_BB",
              "queries_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
              "queries_EndTime") + "\n;";
          sw.WriteLine(msg);
          string msg1 = "trimspec operations start="
                  + (string)SafeBBGet("Trim_BB", "queries_StartTime") + " end="
                  + (string)SafeBBGet("Trim_BB", "queries_EndTime") + "\n;";
          sw.WriteLine(msg1);
        }
        if ((SafeBBGet("Trim_BB", "updates_StartTime") != null) && (SafeBBGet(
                "Trim_BB", "updates_EndTime") != null))
        {
          string msg = "trimspec updates start=" + (string)SafeBBGet("Trim_BB",
              "updates_StartTime") + " end=" + (string)SafeBBGet("Trim_BB",
              "updates_EndTime") + "\n;";
          sw.WriteLine(msg);
          string msg1 = "trimspec operations start="
                  + (string)SafeBBGet("Trim_BB", "updates_StartTime") + " end="
                  + (string)SafeBBGet("Trim_BB", "updates_EndTime") + "\n;";
          sw.WriteLine(msg1);
        }
        sw.Close();
      }
      catch (Exception ex)
      {
        FwkException("DoGenerateTrimSpec() Caught Exception: {0}", ex);
      }

    }
    public void DoOpenStatistic()
    {
      FwkInfo("In DoOpenStatistic()");
      try
      {
        CreateCacheConnect();
        ResetKey(NumThreads);
        int numThreads = GetUIntValue(NumThreads);
        numThreads = (numThreads < 0) ? 1 : numThreads;
        InitPerfStat initStat = new InitPerfStat();
        RunTask(initStat, numThreads, 0, -1, -1, null);
        Thread.Sleep(3000);
      }
      catch (Exception ex)
      {
        FwkException("DoOpenStatistic() Caught Exception: {0}", ex);
      }
    }
    public void DoCloseStatistic()
    {
      FwkInfo("In DoCloseStatistic()");
      try
      {
        for (int i = 0; i < InitPerfStat.perfstat.Length; i++)
        {
          InitPerfStat.perfstat[i] = null;
        }
        FwkInfo("Closed statistics");
      }
      catch (Exception ex)
      {
        FwkException("DoCloseStatistic() Caught Exception: {0}", ex);
      }
    }
    public virtual void DoCreatePool()
    {
      FwkInfo("In DoCreatePool()");
      try
      {
        CreatePool();
      }
      catch (Exception ex)
      {
        FwkException("DoCreatePool() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreatePool() complete.");
    }
   
    public void DoRegisterAllKeys()
    {
      FwkInfo("In DoRegisterAllKeys()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
        ResetKey("getInitialValues");
        bool isGetInitialValues = GetBoolValue("getInitialValues");
        bool checkReceiveVal = GetBoolValue("checkReceiveVal");
        bool isReceiveValues = true;
        if (checkReceiveVal)
        {
          ResetKey("receiveValue");
          isReceiveValues = GetBoolValue("receiveValue");
        }
        region.GetSubscriptionService().RegisterAllKeys(false, null, isGetInitialValues, isReceiveValues);
        SetTrimTime("reg", true);
      }
      catch (Exception ex)
      {
        FwkException("DoRegisterAllKeys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterAllKeys() complete.");
    }

    public void DoPopulateRegion()
    {
      FwkInfo("In DoPopulateRegion()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        ResetKey(ValueSizes);
        int numKeys = InitKeys();
        int size = GetUIntValue(ValueSizes);
        ResetKey("ObjectType");
        string objectname = GetStringValue("ObjectType");
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey(NumThreads);
        ResetKey("AssetAccountSize");
        ResetKey("AssetMaxVal");
        ResetKey("isMainWorkLoad");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        int assetAccountSize = GetUIntValue("AssetAccountSize");
        if (assetAccountSize < 0)
          assetAccountSize = 0;
        int assetMaxVal = GetUIntValue("AssetMaxVal");
        if (assetMaxVal < 0)
          assetMaxVal = 0;
        CreateTasks<TKey, TVal> creates = new CreateTasks<TKey, TVal>(region, m_keysA, size, objectname, encodeKey,
              encodeTimestamp, mainworkLoad, assetAccountSize, assetMaxVal);
        FwkInfo("Populating region.");
        RunTask(creates, 1, m_maxKeys, -1, -1, null);
        FwkInfo("Populated region.");
        SetTrimTime("creates", true);
      }
      catch (Exception ex)
      {
        FwkException("DoPopulateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoPopulateRegion() complete.");
    }

    public void DoPuts()
    {
      FwkInfo("In DoPuts()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey("ObjectType");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        string objectname = GetStringValue("ObjectType");
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        ResetKey("distinctKeys");
        ResetKey("BatchSize");
        ResetKey("opsSecond");
        int opsSec = GetUIntValue("opsSecond");
        opsSec = (opsSec < 1) ? 0 : opsSec;
        int numKeys;
        ClientTask puts = null;
        while ((numKeys = InitKeys(false, true)) > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys, 0, false)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            {
              if (opsSec > 0)
              {
                puts = new MeteredPutTask<TKey, TVal>(region, m_keysA, valSize,
                              objectname, encodeKey, encodeTimestamp, mainworkLoad, opsSec);
              }
              else
              {
                puts = new PutTasks<TKey, TVal>(region, m_keysA, valSize,
                      objectname, encodeKey, encodeTimestamp, mainworkLoad);
              }
              try
              {
                SetTrimTime("put");
                RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
                SetTrimTime("put", true);
              }
              catch (ClientTimeoutException)
              {
                FwkException("In DoPuts()  Timed run timed out.");
              }

              // real work complete for this pass thru the loop

              Thread.Sleep(3000); // Put a marker of inactivity in the stats
            }
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          } // value loop
          Thread.Sleep(3000); // Put a marker of inactivity in the stats
        } // keys loop
      }
      catch (Exception ex)
      {
        FwkException("DoPuts() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoPuts() complete.");
    }

    public void DoGets()
    {
      FwkInfo("In DoGets()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        ResetKey(DistinctKeys);
        InitKeys(false, true);

        int valSize = GetUIntValue(ValueSizes);

        // Loop over threads
        ResetKey(NumThreads);
        int numThreads;
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");

        while ((numThreads = GetUIntValue(NumThreads)) > 0)
        { // thread loop

          // And we do the real work now
          GetTask<TKey, TVal> gets = new GetTask<TKey, TVal>(region, m_keysA, mainworkLoad);
          try
          {
            SetTrimTime("get");
            RunTask(gets, numThreads, -1, timedInterval, maxTime, null);
            SetTrimTime("get", true);
          }
          catch (ClientTimeoutException)
          {
            FwkException("In DoGets()  Timed run timed out.");
          }

          Thread.Sleep(3000);
        } // thread loop
      }
      catch (Exception ex)
      {
        FwkException("DoGets() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000);
      FwkInfo("DoGets() complete.");
    }

    public void DoCyclePoolTask()
    {

      FwkInfo("In Smokeperf::DoCyclePoolTask");
      try
      {
        int timedInterval = GetUIntValue("timedInterval");
        if (timedInterval <= 0)
        {
          timedInterval = 5;
        }
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        int sleepMs = GetTimeValue("sleepMs");
        DateTime now = DateTime.Now;
        DateTime end = now.AddSeconds(timedInterval);
        //PoolFactory pf = CreatePoolFactoryAndSetAttribute();
        ResetKey("poolSpec");
        string poolRegionData = GetStringValue("poolSpec");
        //poolRegionData = poolRegionData + "New";
        //Properties prop = GetNewPoolAttributes(poolRegionData);
        string poolName = null;
        PoolFactory pf = PoolManager.CreateFactory();
        SetPoolAttributes(pf, poolRegionData, ref poolName);
        long startTime;
        SetTrimTime("connects");
        while (now < end)
        {
          startTime = InitPerfStat.perfstat[0].StartConnect();
          Pool pool = pf.Create(poolName);
          //FwkInfo("rjk: durable client id is {0}", DistributedSystem.SystemProperties.DurableClientId);
          if (pool != null)
          {
            pool.Destroy();
          }
          InitPerfStat.perfstat[0].EndConnect(startTime, mainworkLoad);
          Thread.Sleep(sleepMs);
          now = DateTime.Now;
        }
        SetTrimTime("connects", true);
      }
      catch (Exception ex)
      {
        FwkException("Smokeperf::DoCyclePoolTask FAILED -- caught exception: {0}", ex);
      }
      FwkInfo("DoCyclePoolTask() complete.");
    }
    // BridgeConnection ( old endpoint) related task is depricated in the product. so no use of this Method
    // Used in perf073,075,100.
    public void DoCycleBridgeConnectionTask()
    {
      FwkInfo("In Smokeperf::DoCycleBridgeConnectionTask");
      string name = GetStringValue("regionName");
      if (name.Length <= 0)
      {
        FwkException("Region name not specified in test.");
      }
      ResetKey("isMainWorkLoad");
      bool mainworkLoad = GetBoolValue("isMainWorkLoad");
      try
      {
        int timedInterval = GetUIntValue("timedInterval");
        if (timedInterval <= 0)
        {
          timedInterval = 5;
        }
        int sleepMs = GetTimeValue("sleepMs");
        DateTime now = DateTime.Now;
        DateTime end = now.AddSeconds(timedInterval);
        long startTime;
        SetTrimTime("connects");
        while (now < end)
        {
          startTime = InitPerfStat.perfstat[0].StartConnect();
          IRegion<TKey, TVal> region = CreateRootRegion();
          region.GetLocalView().DestroyRegion();
          InitPerfStat.perfstat[0].EndConnect(startTime, mainworkLoad);
          Thread.Sleep(sleepMs);
          now = DateTime.Now;
        }
        SetTrimTime("connects", true);
      }
      catch (Exception ex)
      {
        FwkException("Smokeperf::DoCycleBridgeConnectionTask FAILED -- caught exception: {0}", ex);
      }
      FwkInfo("DoCycleBridgeConnectionTask() complete.");
    }

    public void DoMixPutGetDataTask()
    {
      FwkInfo("In DoMixPutGetDataTask()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey("ObjectType");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        string objectname = GetStringValue("ObjectType");
        ResetKey("putPercentage");
        int putPercentage = GetUIntValue("putPercentage");
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        ResetKey("distinctKeys");
        int numKeys;
        while ((numKeys = InitKeys(false, true)) > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys, 0, false)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            {
              PutGetMixTask<TKey, TVal> putGet = new PutGetMixTask<TKey, TVal>(region, m_keysA, valSize, objectname, encodeKey,
                encodeTimestamp, mainworkLoad, putPercentage);

              try
              {
                SetTrimTime("putgets");
                RunTask(putGet, numThreads, -1, timedInterval, maxTime, null);
                SetTrimTime("putgets", true);
              }
              catch (ClientTimeoutException)
              {
                FwkException("In DoMixPutGetDataTask()  Timed run timed out.");
              }

              // real work complete for this pass thru the loop

              Thread.Sleep(3000); // Put a marker of inactivity in the stats
            }
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          } // value loop
          Thread.Sleep(3000); // Put a marker of inactivity in the stats
        } // keys loop
      }
      catch (Exception ex)
      {
        FwkException("DoMixPutGetDataTask() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoMixPutGetDataTask() complete.");
    }

    public void DoQueryRegionDataTask()
    {
      FwkInfo("In Smokeperf::DoQueryRegionDataTask()");

      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;


        // Loop over key set sizes
        ResetKey("query");
        string queryStr = GetStringValue("query"); // set the query string in xml
        if (queryStr.Length <= 0)
          queryStr = "select distinct * from " + region.FullPath;
        ResetKey(NumThreads);
        int numThreads;
        while ((numThreads = GetUIntValue(NumThreads)) > 0)
        { // thread loop
          RegionQueryTask<TKey, TVal> query = new RegionQueryTask<TKey, TVal>(region, queryStr);
          SetTrimTime("queries");
          RunTask(query, numThreads, -1, timedInterval, maxTime, null);
          SetTrimTime("queries", true);
          Thread.Sleep(3000);
        } // thread loop
      }
      catch (Exception ex)
      {
        FwkException("Smokeperf::DoQueryRegionDataTask() Caught Exception: {0}", ex);
      }
      FwkInfo("Smokeperf::DoQueryRegionDataTask() complete.");
    }

    public void DoRegisterCQs()
    {
      FwkInfo("In Smokeperf::DoRegisterCQs()");

      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numCQ = GetUIntValue("numCQs");
        numCQ = (numCQ <= 0) ? 1 : numCQ;
        for (int i = 0; i < numCQ; i++)
        {
          string cqname = String.Format("cq{0}", i);
          string query = GetQuery(i);
          Pool pool = PoolManager.Find("_Test_Pool1");
          QueryService<TKey, object> qs = null;//  pool.GetQueryService<TKey, object>(); //rjk to do fixed
          CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
          ICqListener<TKey, object> cqLstner = new CQLatencyListener<TKey, TVal>(InitPerfStat.perfstat[0]);
          cqFac.AddCqListener(cqLstner);
          CqAttributes<TKey, object> cqAttr = cqFac.Create();
          FwkInfo("Registering CQ named {0} with query: {1}", cqname, query);
          CqQuery<TKey, object> qry = qs.NewCq(cqname, query, cqAttr, false);
          ISelectResults<object> results = qry.ExecuteWithInitialResults(300);

          FwkInfo("Successfully executed CQ named {0}", cqname);
        }

      }
      catch (Exception ex)
      {
        FwkException("Smokeperf::DoRegisterCQs() Caught Exception: {0}", ex);
      }
      FwkInfo("Smokeperf::DoRegisterCQs() complete.");
    }

    public void DoPutBatchObj()
    {
      FwkInfo("In DoPutBatchObj()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey("ObjectType");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        string objectname = GetStringValue("ObjectType");
        int putPercentage = GetUIntValue("putPercentage");
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        ResetKey("distinctKeys");
        ResetKey("BatchSize");
        int batchsize = GetUIntValue("BatchSize");
        int numKeys = 0;
        if (batchsize > 0)
          numKeys = InitBatchKeys(false);
        else
          numKeys = InitKeys(false, true);
        while (numKeys > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys, 0, false)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            {
              PutBatchObjectTask<TKey, TVal> puts = new PutBatchObjectTask<TKey, TVal>(region, m_keysA, valSize, objectname,
                   encodeKey, encodeTimestamp, mainworkLoad, batchsize, valSize);

              try
              {
                bool isCreate = GetBoolValue("isCreate");
                if (isCreate)
                {
                  FwkInfo("Creating entries.");
                  RunTask(puts, 1, m_maxKeys, -1, -1, null);
                }
                else
                {
                  SetTrimTime("put");
                  RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
                  SetTrimTime("put", true);
                }
              }
              catch (ClientTimeoutException)
              {
                FwkException("In DoPutBatchObj()  Timed run timed out.");
              }

              // real work complete for this pass thru the loop

            }

          } // value loop
          batchsize = GetUIntValue("BatchSize");
          if (batchsize > 0)
            numKeys = InitBatchKeys(false);
          else
            numKeys = InitKeys(false, true);
          if (numKeys > 0)
          {
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          }
        } // keys loop

      }
      catch (Exception ex)
      {
        FwkException("DoPutBatchObj() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoPutBatchObj() complete.");
    }

    public void DoCycleDurableClientTask()
    {
      FwkInfo("In Smokeperf::DoCycleDurableClientTask()");
      //resetValue("isMainWorkLoad");
      //bool mainworkLoad = getBoolValue("isMainWorkLoad");
      try
      {
        int timedInterval = GetTimeValue("timedInterval");
        if (timedInterval <= 0)
        {
          timedInterval = 5;
        }
        ResetKey("isDurableReg");
        //ResetKey("poolName");
        bool isDurable = GetBoolValue("isDurableReg");
        //string poolName = GetStringValue("poolName");
        //if (poolName.Length <= 0)
        //  poolName = "_Test_Pool1";
        DateTime now = DateTime.Now;
        DateTime end = now.AddSeconds(timedInterval);
        long startTime;
        SetTrimTime("connects");
        while (now < end)
        {
          startTime = InitPerfStat.perfstat[0].StartConnect();
          DoCreatePool();
          ResetKey("regionSpec");
          DoCreateRegion();
          IRegion<TKey, TVal> region = GetRegion();
          region.GetSubscriptionService().RegisterRegex(".*", isDurable);
          CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
          InitPerfStat.perfstat[0].EndConnect(startTime, false);
          string oper_cnt_key = string.Format("ClientName_{0}", Util.ClientNum);
          int cur_cnt = (int)Util.BBGet("DURABLEBB", oper_cnt_key);
          InitPerfStat.perfstat[0].IncUpdateEvents(cur_cnt);
          InitPerfStat.perfstat[0].SetOpTime(InitPerfStat.perfstat[0].GetConnectTime());
          InitPerfStat.perfstat[0].SetOps(cur_cnt + InitPerfStat.perfstat[0].GetOps());
          CacheHelper<TKey, TVal>.DCache.Close(true);
          //pool->destroy();
          region = null;
          CacheHelper<TKey, TVal>.DCache = null;
          //CacheHelper.SetDCacheNull();
          Thread.Sleep(10000);
          now = DateTime.Now;
        }
        SetTrimTime("connects", true);
      }
      catch (Exception ex)
      {
        FwkException("Smokeperf::DoCycleDurableClientTask FAILED -- caught exception: {0}", ex);
      }
    }
    public void DoCreateEntryMapTask()
    {

      FwkInfo("In Smokeperf::DoCreateEntryMapTask()");

      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int timedInterval = GetTimeValue("timedInterval");
        if (timedInterval <= 0)
        {
          timedInterval = 5;
        }
        // Loop over key set sizes
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey("ObjectType");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        string objectname = GetStringValue("ObjectType");
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        ResetKey("distinctKeys");
        int numKeys = InitKeys(false, true);
        ResetKey("valueSizes");
        int valSize = InitValues(numKeys, 0, false);
        ResetKey("numThreads");
        int numThreads = GetUIntValue("numThreads");
        CreatePutAllMap<TKey, TVal> createMap = new CreatePutAllMap<TKey, TVal>(region, m_keysA,
            valSize, objectname,  maps, encodeKey, encodeTimestamp,
            mainworkLoad);

        FwkInfo("Running timed task.");
        RunTask(createMap, numThreads, m_maxKeys, -1, -1, null);

      }
      catch (Exception ex)
      {
        FwkException("Smokeperf::DoCreateEntryMapTask() Caught Exception: {0}", ex);
      }
      ClearKeys();
      Thread.Sleep(3); // Put a marker of inactivity in the stats
      FwkInfo("Smokeperf::createEntryMapTask() complete.");
    }

    public void DoPutAllEntryMapTask()
    {
      FwkInfo("In DoPutAllEntryMapTask()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey("ObjectType");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        string objectname = GetStringValue("ObjectType");
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        ResetKey("distinctKeys");
        ResetKey("BatchSize");
        ResetKey("opsSecond");
        int opsSec = GetUIntValue("opsSecond");
        opsSec = (opsSec < 1) ? 0 : opsSec;
        int numKeys = InitKeys(false, true);
        ResetKey(ValueSizes);
        int valSize = InitValues(numKeys, 0, false);

        ResetKey(NumThreads);
        int numThreads = GetUIntValue(NumThreads);
        CreatePutAllMap<TKey, TVal> createMap = new CreatePutAllMap<TKey, TVal>(region, m_keysA,
           valSize, objectname, maps, encodeKey, encodeTimestamp,
           mainworkLoad);

        FwkInfo("Running timed task.");
        RunTask(createMap, numThreads, m_maxKeys, -1, -1, null);

        PutAllMap<TKey, TVal> putall = new PutAllMap<TKey, TVal>(region, m_keysA, valSize, objectname, maps, encodeKey,
          encodeTimestamp, mainworkLoad);


        try
        {
          SetTrimTime("put");
          RunTask(putall, numThreads, -1, timedInterval, maxTime, null);
          SetTrimTime("put", true);
        }
        catch (ClientTimeoutException)
        {
          FwkException("In DoPutAllEntryMapTask()  Timed run timed out.");
        }

      }
      catch (Exception ex)
      {
        FwkException("DoPutAllEntryMapTask() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoPutAllEntryMapTask() complete.");
    }
    public void DoUpdateDeltaData()
    {
      FwkInfo("In DoUpdateDeltaData()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey("encodeKey");
        ResetKey("encodeTimestamp");
        ResetKey("ObjectType");
        bool encodeKey = GetBoolValue("encodeKey");
        bool encodeTimestamp = GetBoolValue("encodeTimestamp");
        string objectname = GetStringValue("ObjectType");
        ResetKey("isMainWorkLoad");
        bool mainworkLoad = GetBoolValue("isMainWorkLoad");
        ResetKey("distinctKeys");
        ResetKey("BatchSize");
        ResetKey("opsSecond");
        ResetKey("AssetAccountSize");
        ResetKey("AssetMaxVal");
        int assetAccountSize = GetUIntValue("AssetAccountSize");
        if (assetAccountSize < 0)
          assetAccountSize = 0;
        int assetMaxVal = GetUIntValue("AssetMaxVal");
        if (assetMaxVal < 0)
          assetMaxVal = 0;
        int numKeys;
        while ((numKeys = InitKeys(false, true)) > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys, 0, false)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            {
              UpdateDeltaTask<TKey, TVal> puts = new UpdateDeltaTask<TKey, TVal>(region, m_keysA, valSize, objectname, encodeKey,
             encodeTimestamp, mainworkLoad, assetAccountSize, assetMaxVal);
              try
              {
                SetTrimTime("updates");
                RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
                SetTrimTime("updates", true);
              }
              catch (ClientTimeoutException)
              {
                FwkException("In DoUpdateDeltaData()  Timed run timed out.");
              }

              // real work complete for this pass thru the loop

              Thread.Sleep(3000); // Put a marker of inactivity in the stats
            }
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          } // value loop
          Thread.Sleep(3000); // Put a marker of inactivity in the stats
        } // keys loop
      }
      catch (Exception ex)
      {
        FwkException("DoUpdateDeltaData() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoUpdateDeltaData() complete.");
    }
    #endregion
  }
}

//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;
  //using IntRegion = GemStone.GemFire.Cache.Generic.IRegion<int, byte[]>;
  //using StringRegion = GemStone.GemFire.Cache.Generic.IRegion<string, byte[]>;
  public class PerfTests<TKey, TVal> : FwkTest<TKey, TVal>
  {
    #region Protected members
    protected TKey[] m_keysA;
    protected int m_maxKeys;
    protected int m_keyIndexBegin;

    protected TVal[] m_cValues;
    protected int m_maxValues;

    public char m_keyType = 'i';

    #endregion

    #region Protected constants

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

    #endregion
    
    #region Protected utility methods

    protected void ClearKeys()
    {
      if (m_keysA != null)
      {
        for (int i = 0; i < m_keysA.Length; i++)
        {
          if (m_keysA[i] != null)
          {
            //m_keysA[i];
            m_keysA[i] = default(TKey);
          }
        }
        m_keysA = null;
        m_maxKeys = 0;
      }
    }

    protected int InitKeys(bool useDefault)
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
        InitStrKeys(low, high, keyBase);
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
      return InitKeys(true);
    }

    protected void InitStrKeys(int low, int high, string keyBase)
    {
      m_keysA = (TKey[])(object) new String[m_maxKeys];
      FwkInfo("m_maxKeys: {0}; low: {1}; high: {2}",
        m_maxKeys, low, high);
      for (int i = low; i < high; i++)
      {
        m_keysA[i - low] = (TKey)(object)(keyBase.ToString()  + i.ToString("D10"));
      }
    }

    protected void InitIntKeys(int low, int high)
    {
      m_keysA = (TKey[])(object) new Int32[m_maxKeys];
      FwkInfo("m_maxKeys: {0}; low: {1}; high: {2}",
        m_maxKeys, low, high);
      for (int i = low; i < high; i++)
      {
        m_keysA[i - low] = (TKey)(object)i;
      }
    }

    protected int InitValues(int numKeys)
    {
      return InitValues(numKeys, 0);
    }

    protected int InitValues(int numKeys, int size)
    {
      if (size <= 0)
      {
        size = GetUIntValue(ValueSizes);
      }
      if (size <= 0)
      {
        //FwkSevere("Failed to initialize values with valueSize: {0}", size);
        return size;
      }
      if (numKeys <= 0)
      {
        numKeys = 500;
      }
      m_maxValues = numKeys;
      if (m_cValues != null)
      {
        for (int i = 0; i < m_cValues.Length; i++)
        {
          if (m_cValues[i] != null)
          {
            //m_cValues[i].Dispose();
            m_cValues[i] = default(TVal);
          }
        }
        m_cValues = null;
      }

      m_cValues = new TVal[m_maxValues];

      FwkInfo("InitValues()  payload size: {0}", size);
      //byte[] createPrefix = Encoding.ASCII.GetBytes("Create ");
      //byte[] buffer = new byte[size];
      for (int i = 0; i < m_maxValues; i++)
      {
        
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        char ch;
        for (int j = 0; j < size; j++)
        {
          ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));
          builder.Append(ch);
        }

        //FwkInfo("rjk: InitValues()   value {0}", builder.ToString());
        //Util.RandBytes(buffer);
        //createPrefix.CopyTo(buffer, 0);
        //string tmp = buffer;
        if (typeof(TVal) == typeof(string))
        {
          m_cValues[i] = (TVal)(object)builder.ToString();
          //FwkInfo("rjk: InitValues()   value {0} size is {1}", m_cValues[i].ToString(), m_cValues[i].ToString().Length);
        }
        else if (typeof(TVal) == typeof(byte[]))
        {
          //Encoding.ASCII.GetBytes(buffer);
          //Util.RandBytes(buffer);
          //createPrefix.CopyTo(buffer, 0);
          m_cValues[i] = (TVal)(object)(Encoding.ASCII.GetBytes(builder.ToString()));
          //FwkInfo("rjk: InitValues() type is byte[]  value {0} size is {1}", m_cValues[i].ToString(), ((byte[])(object)m_cValues[i]).Length);
        }
         
        /*
        Util.RandBytes(buffer);
        createPrefix.CopyTo(buffer, 0);
        if (typeof(TVal) == typeof(string))
        {
          String MyString;
          //Encoding ASCIIEncod = new Encoding();
          MyString = Encoding.ASCII.GetString(buffer);
          m_cValues[i] = (TVal)(MyString as object);
          FwkInfo("rjk: InitValues()   value {0} size is {1}", MyString.ToString(), m_cValues[i].ToString().Length);
        }
        else if (typeof(TVal) == typeof(byte[]))
        {
          Util.RandBytes(buffer);
          createPrefix.CopyTo(buffer, 0);
          m_cValues[i] = (TVal)(buffer as object);
        }
         * */
        //for (int ii = 0; ii < buffer.Length; ++ii)
        //{
        //  FwkInfo("rjk: InitValues() index is {0} value is {1} buffer = {2}", i, m_cValues[ii], buffer[ii]);
        //}
      }
      return size;
    }

    protected IRegion<TKey, TVal> GetRegion()
    {
      return GetRegion(null);
     
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
        region = GetRootRegion();
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

    #region Public methods

    public static ICacheListener<TKey, TVal> CreatePerfTestCacheListener()
    {
      return new PerfTestCacheListener<TKey, TVal>();
    }
    public static ICacheListener<TKey, TVal> CreateConflationTestCacheListener()
    {
      return new ConflationTestCacheListener<TKey, TVal>();
    }
    public static ICacheListener<TKey, TVal> CreateLatencyListenerP()
    {
      return new LatencyListener<TKey, TVal>();
    }

    public static ICacheListener<TKey, TVal> CreateDupChecker()
    {
      return new DupChecker<TKey, TVal>();
    }

    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        IRegion<TKey,TVal> region = null;

        FwkInfo("Tkey = {0} , Val = {1}", typeof(TKey).Name, typeof(TVal));
        
        region = CreateRootRegion();
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
      CacheHelper<TKey,TVal>.Close();
    }

    public void DoPuts()
    {
      FwkInfo("In DoPuts()");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey(DistinctKeys);
        int numKeys;
        while ((numKeys = InitKeys(false)) > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            {
              try
              {
                //if (m_keyType == 's')
                //{
                  //StringRegion sregion = region as StringRegion;
                  PutsTask<TKey, TVal> puts = new PutsTask<TKey, TVal>(region, m_keysA, m_cValues);
                  FwkInfo("Running warmup task for {0} iterations.", numKeys);
                  RunTask(puts, 1, numKeys, -1, -1, null);
                  // Running the warmup task
                  Thread.Sleep(3000);
                  // And we do the real work now
                  FwkInfo("Running timed task for {0} secs and {1} threads; numKeys[{2}]",
                    timedInterval / 1000, numThreads, numKeys);
                  SetTaskRunInfo(label, "Puts", m_maxKeys, numClients,
                    valSize, numThreads);
                  RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
                  AddTaskRunRecord(puts.Iterations, puts.ElapsedTime);

              }
              catch (ClientTimeoutException)
              {
                FwkException("In DoPuts()  Timed run timed out.");
              }
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
    public void DoPopulateRegion()
    {
      FwkInfo("In DoPopulateRegion()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        int numKeys = InitKeys();
        InitValues(numKeys);

        PutsTask<TKey, TVal> puts = new PutsTask<TKey, TVal>(region, m_keysA, m_cValues);
        FwkInfo("Populating region.");
        RunTask(puts, 1, m_maxKeys, -1, -1, null);
      }
      catch (Exception ex)
      {
        FwkException("DoPopulateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoPopulateRegion() complete.");
    }
    
    public void DoSerialPuts()
    {
      FwkInfo("In DoSerialPuts()");
      try
      {
        IRegion<int, int> region = (IRegion<int, int>)GetRegion();

        int keyStart = GetUIntValue(KeyStart);
        int keyEnd = GetUIntValue(KeyEnd);

        for (Int32 keys = keyStart; keys <= keyEnd; keys++)
        {
          if (keys % 50 == 1)
          {
            FwkInfo("DoSerialPuts() putting 1000 values for key " + keys);
          }

          Int32 key = keys;
          for (Int32 values = 1; values <= 1000; values++)
          {
            Int32 value = values;
            region[key] =  value;
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("DoSerialPuts() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoSerialPuts() complete.");
    }

    public void DoPutBursts()
    {
      FwkInfo("In DoPutBursts()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        string label = CacheHelper<TKey,TVal>.RegionTag(region.Attributes);

        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int burstMillis = GetUIntValue("burstMillis");
        if (burstMillis <= 0)
        {
          burstMillis = 500;
        }
        int burstPause = GetTimeValue("burstPause") * 1000;
        if (burstPause <= 0)
        {
          burstPause = 1000;
        }
        int opsSec = GetUIntValue(OpsSecond);
        if (opsSec <= 0)
        {
          opsSec = 100;
        }

        // Loop over key set sizes
        ResetKey(DistinctKeys);
        int numKeys;
        while ((numKeys = InitKeys(false)) > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            { // thread loop
              // And we do the real work now
              MeteredPutsTask<TKey, TVal> mputs = new MeteredPutsTask<TKey, TVal>(region, m_keysA,
                m_cValues, opsSec);
              FwkInfo("Running warmup metered task for {0} iterations.", m_maxKeys);
              RunTask(mputs, 1, m_maxKeys, -1, -1, null);
              Thread.Sleep(10000);
              PutsTask<TKey, TVal> puts = new PutsTask<TKey, TVal>(region, m_keysA, m_cValues);
              int loopIters = (timedInterval / burstMillis) + 1;
              FwkInfo("Running timed task for {0} secs and {1} threads.",
                timedInterval/1000, numThreads);
              SetTaskRunInfo(label, "PutBursts", numKeys, numClients,
                valSize, numThreads);
              int totIters = 0;
              TimeSpan totTime = TimeSpan.Zero;
              for (int i = loopIters; i > 0; i--)
              {
                try
                {
                  RunTask(puts, numThreads, -1, burstMillis, 30000, null);
                }
                catch (ClientTimeoutException)
                {
                  FwkException("In DoPutBursts()  Timed run timed out.");
                }
                totIters += puts.Iterations;
                totTime += puts.ElapsedTime;
                double psec = (totIters * 1000.0) / totTime.TotalMilliseconds;
                FwkInfo("PerfSuite interim: {0} {1} {2}", psec, totIters, totTime);
                Thread.Sleep(burstPause);
              }
              AddTaskRunRecord(totIters, totTime);
              // real work complete for this pass thru the loop

              Thread.Sleep(3000); // Put a marker of inactivity in the stats
            } // thread loop
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          } // value loop
          Thread.Sleep(3000); // Put a marker of inactivity in the stats
        } // keys loop
      }
      catch (Exception ex)
      {
        FwkException("DoPutBursts() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoPutBursts() complete.");
    }

    public void DoLatencyPuts()
    {
      FwkInfo("In DoLatencyPuts()");

      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        int opsSec = GetUIntValue(OpsSecond);
        if (opsSec < 0)
        {
          opsSec = 100;
        }
        // Loop over key set sizes
        ResetKey(DistinctKeys);
        int numKeys;
        while ((numKeys = InitKeys(false)) > 0)
        { // keys loop
          // Loop over value sizes
          ResetKey(ValueSizes);
          int valSize;
          while ((valSize = InitValues(numKeys)) > 0)
          { // value loop
            // Loop over threads
            ResetKey(NumThreads);
            int numThreads;
            while ((numThreads = GetUIntValue(NumThreads)) > 0)
            {
              LatencyPutsTask<TKey, TVal> puts = new LatencyPutsTask<TKey, TVal>(region, m_keysA,
                m_cValues, opsSec);
              // Running the warmup task
              FwkInfo("Running warmup task for {0} iterations.", numKeys);
              RunTask(puts, 1, numKeys, -1, -1, null);
              Thread.Sleep(3000);
              // And we do the real work now
              FwkInfo("Running timed task for {0} secs and {1} threads.",
                timedInterval/1000, numThreads);
              SetTaskRunInfo(label, "LatencyPuts", numKeys, numClients, valSize, numThreads);
              Util.BBSet("LatencyBB", "LatencyTag", TaskData);
              try
              {
                RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
              }
              catch (ClientTimeoutException)
              {
                FwkException("In DoLatencyPuts()  Timed run timed out.");
              }
              AddTaskRunRecord(puts.Iterations, puts.ElapsedTime);
              // real work complete for this pass thru the loop
              Thread.Sleep(3000); // Put a marker of inactivity in the stats
            } // thread loop
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          } // value loop
          Thread.Sleep(3000); // Put a marker of inactivity in the stats
        } // keys loop
      }
      catch (Exception ex)
      {
        FwkException("DoLatencyPuts() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("DoLatencyPuts() complete.");
    }

    public void DoGets()
    {
      FwkInfo("In DoGets()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        FwkInfo("rjk:DoGets region name is {0} ", region.Name); 
        int numClients = GetUIntValue(ClientCount);
        string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        ResetKey(DistinctKeys);
        InitKeys();

        int valSize = GetUIntValue(ValueSizes);
        FwkInfo("rjk:DoGets number of keys in region is {0} .", region.Keys.Count);
        // Loop over threads
        ResetKey(NumThreads);
        int numThreads;
        while ((numThreads = GetUIntValue(NumThreads)) > 0)
        { // thread loop

          // And we do the real work now
          GetsTask<TKey, TVal> gets = new GetsTask<TKey, TVal>(region, m_keysA);
          FwkInfo("Running warmup task for {0} iterations.", m_maxKeys);
          RunTask(gets, 1, m_maxKeys, -1, -1, null);
          region.GetLocalView().InvalidateRegion();
          Thread.Sleep(3000);
          FwkInfo("Running timed task for {0} secs and {1} threads.",
            timedInterval/1000, numThreads);
          SetTaskRunInfo(label, "Gets", m_maxKeys, numClients, valSize, numThreads);
          try
          {
            RunTask(gets, numThreads, -1, timedInterval, maxTime, null);
          }
          catch (ClientTimeoutException)
          {
            FwkException("In DoGets()  Timed run timed out.");
          }
          AddTaskRunRecord(gets.Iterations, gets.ElapsedTime);
          // real work complete for this pass thru the loop

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

    public void DoPopServers()
    {
      FwkInfo("In DoPopServers()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        int numKeys = InitKeys();

        ResetKey(ValueSizes);
        InitValues(numKeys);

        int opsSec = GetUIntValue(OpsSecond);

        MeteredPutsTask<TKey, TVal> mputs = new MeteredPutsTask<TKey, TVal>(region, m_keysA,
          m_cValues, opsSec);
        RunTask(mputs, 1, m_maxKeys, -1, -1, null);
      }
      catch (Exception ex)
      {
        FwkException("DoPopServers() Caught Exception: {0}", ex);
      }
      FwkInfo("DoPopServers() complete.");
    }

    public void DoPopClient()
    {
      FwkInfo("In DoPopClient()");

      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        InitKeys();

        GetsTask<TKey, TVal> gets = new GetsTask<TKey, TVal>(region, m_keysA);
        RunTask(gets, 1, m_maxKeys, -1, -1, null);
      }
      catch (Exception ex)
      {
        FwkException("DoPopClient() Caught Exception: {0}", ex);
      }
      FwkInfo("DopopClient() complete.");
    }

    public void DoPopClientMS()
    {
      FwkInfo("In DoPopClientMS()");

      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        int numKeys = GetUIntValue(DistinctKeys);
        int clientCount = GetUIntValue(ClientCount);
        int interestPercent = GetUIntValue(InterestPercent);
        if (interestPercent <= 0)
        {
          if (clientCount <= 0)
          {
            interestPercent = 100;
          }
          else
          {
            interestPercent = (100 / clientCount);
          }
        }
        int myNumKeys = (numKeys * interestPercent) / 100;
        int myNum = Util.ClientNum;
        int myStart = myNum * myNumKeys;
        int myValSize = 10;
        m_maxKeys = numKeys;
        InitIntKeys(myStart, myStart + myNumKeys);
        InitValues(myNumKeys, myValSize);

        FwkInfo("DoPopClientMS() Client number: {0}, Client count: {1}, " +
          "MaxKeys: {2}", myNum, clientCount, m_maxKeys);
        PutsTask<TKey, TVal> puts = new PutsTask<TKey, TVal>(region, m_keysA, m_cValues);
        RunTask(puts, 1, m_maxKeys, -1, -1, null);
      }
      catch (Exception ex)
      {
        FwkException("DoPopClientMS() Caught Exception: {0}", ex);
      }
      FwkInfo("DoPopClientMS() complete.");
    }

    public void DoDestroys()
    {
      FwkInfo("In DoDestroys()");

      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        FwkInfo("DoDestroys() numclients set to {0}", numClients);
        string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        if (timedInterval <= 0)
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // always use only one thread for destroys.
        int numKeys;
        // Loop over distinctKeys
        while ((numKeys = InitKeys(false)) > 0)
        { // thread loop
          int valSize = InitValues(numKeys);
          // And we do the real work now

          //populate the region
          PutsTask<TKey, TVal> puts = new PutsTask<TKey, TVal>(region, m_keysA, m_cValues);
          FwkInfo("DoDestroys() Populating region.");
          RunTask(puts, 1, m_maxKeys, -1, -1, null);
          DestroyTask<TKey, TVal> destroys = new DestroyTask<TKey, TVal>(region, m_keysA);
          FwkInfo("Running timed task for {0} iterations and {1} threads.",
            numKeys, 1);
          SetTaskRunInfo(label, "Destroys", numKeys, numClients, valSize, 1);
          try
          {
            RunTask(destroys, 1, numKeys, -1, maxTime, null);
          }
          catch (ClientTimeoutException)
          {
            FwkException("In DoDestroys()  Timed run timed out.");
          }
          AddTaskRunRecord(destroys.Iterations, destroys.ElapsedTime);
          // real work complete for this pass thru the loop

          Thread.Sleep(3000);
        } // distinctKeys loop
      }
      catch (Exception ex)
      {
        FwkException("DoDestroys() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000);
      FwkInfo("DoDestroys() complete.");
    }

    public void DoCheckValues()
    {
      FwkInfo("In DoCheckValues()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ICollection<TVal> vals = region.Values;
        int creates = 0;
        int updates = 0;
        int unknowns = 0;
        if (vals != null)
        {
          byte[] createPrefix = Encoding.ASCII.GetBytes("Create ");
          byte[] updatePrefix = Encoding.ASCII.GetBytes("Update ");
          foreach (object val in vals)
          {
            byte[] valBytes = val as byte[];
            if (Util.CompareArraysPrefix(valBytes, createPrefix))
            {
              creates++;
            }
            else if (Util.CompareArraysPrefix(valBytes, updatePrefix))
            {
              updates++;
            }
            else
            {
              unknowns++;
            }
          }
          FwkInfo("DoCheckValues()  Found {0} values from creates, " +
            "{1} values from updates, and {2} unknown values.",
            creates, updates, unknowns);
        }
      }
      catch (Exception ex)
      {
        FwkException("DoCheckValues() Caught Exception: {0}", ex);
      }
    }

    public void DoLocalDestroyEntries()
    {
      FwkInfo("In DoLocalDestroyEntries()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ICollection<TKey> keys = region.GetLocalView().Keys;
        if (keys != null)
        {
          foreach (TKey key in keys)
          {
            region.GetLocalView().Remove(key);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("DoLocalDestroyEntries() Caught Exception: {0}", ex);
      }
    }

    public void DoDestroyRegion()
    {
      FwkInfo("In DoDestroyRegion");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        region.DestroyRegion();
      }
      catch (Exception ex)
      {
        FwkException("DoDestroyRegion() caught exception: {0}" , ex);
      }
    }

    public void DoLocalDestroyRegion()
    {
      FwkInfo("In DoLocalDestroyRegion()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        region.GetLocalView().DestroyRegion();
      }
      catch (Exception ex)
      {
        FwkException("DoLocalDestroyRegion() Caught Exception: {0}", ex);
      }
    }

    public void DoPutAll()
    {
      FwkInfo("In DoPutAll()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        int numKeys = InitKeys();

        ResetKey(ValueSizes);
        InitValues(numKeys);
        IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();
        map.Clear();
        Int32 i = 0;
        while (i < numKeys)
        {
          map.Add(m_keysA[i], m_cValues[i]);
          i++;
        }
        DateTime startTime;
        DateTime endTime;
        TimeSpan elapsedTime;
        startTime = DateTime.Now;
        region.PutAll(map,60);
        endTime = DateTime.Now;
        elapsedTime = endTime - startTime;
        FwkInfo("PerfTests.DoPutAll: Time Taken to execute" +
                " the putAll for {0}: is {1}ms", numKeys,
                elapsedTime.TotalMilliseconds);
      }
      catch (Exception ex)
      {
        FwkException("DoPutAll() Caught Exception: {0}", ex);
      }
      FwkInfo("DoPutAll() complete.");
    }

    public void DoGetAllAndVerification()
    {
      FwkInfo("In DoGetAllAndVerification()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        ResetKey(ValueSizes);
        ResetKey("addToLocalCache");
        ResetKey("inValidateRegion");
        int numKeys = InitKeys(false);
        Int32 i = 0;
        bool isInvalidateRegion = GetBoolValue("isInvalidateRegion");
        if (isInvalidateRegion)
        {
          region.GetLocalView().InvalidateRegion();
        }
        List<TKey> keys = new List<TKey>();
        keys.Clear();
        while (i < numKeys)
        {
          keys.Add(m_keysA[i]);
          i++;
        }
        bool isAddToLocalCache = GetBoolValue("addToLocalCache");
        Dictionary<TKey, TVal> values = new Dictionary<TKey, TVal>();
        values.Clear();
        DateTime startTime;
        DateTime endTime;
        TimeSpan elapsedTime;
        startTime = DateTime.Now;
        region.GetAll(keys.ToArray(), values, null, isAddToLocalCache);
        endTime = DateTime.Now;
        elapsedTime = endTime - startTime;
        FwkInfo("PerfTests.DoGetAllAndVerification: Time Taken to execute" +
                " the getAll for {0}: is {1}ms", numKeys,
                elapsedTime.TotalMilliseconds);
        int payload = GetUIntValue("valueSizes");
        FwkInfo("PerfTests.DoGetAllAndVerification: keyCount = {0}" + " valueCount = {1} ",
                 keys.Count, values.Count);
        if (values.Count == keys.Count)
        {
          foreach (KeyValuePair<TKey, TVal> entry in values)
          {
            TVal item = entry.Value;
            //if (item != payload) // rjk Need to check how to verify
            //{
            //  FwkException("PerfTests.DoGetAllAndVerification: value size {0} is not equal to " +
            //                "expected payload size {1} for key : {2}", item.Length, payload, entry.Key);
            //}
          }
        }
        if (isAddToLocalCache)
        {
          if (keys.Count != region.Count)
          {
            FwkException("PerfTests.DoGetAllAndVerification: number of keys in region do not" +
                 " match expected number");
          }
        }
        else
        {
          if (region.Count != 0)
          {
            FwkException("PerfTests.DoGetAllAndVerification: expected zero keys in region");
          }
        }

      }
      catch (Exception ex)
      {
        FwkException("DoGetAllAndVerification() Caught Exception: {0}", ex);
      }
      FwkInfo("DoGetAllAndVerification() complete.");
    }
    public void DoResetListener()
    {
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        int sleepTime = GetUIntValue("sleepTime");
        PerfTestCacheListener<TKey,TVal> listener =
          region.Attributes.CacheListener as PerfTestCacheListener<TKey, TVal>;
        if (listener != null)
        {
          listener.Reset(sleepTime);
        }
      }
      catch (Exception ex)
      {
        FwkSevere("DoResetListener() Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000);
      FwkInfo("DoResetListener() complete.");
    }

    public void DoRegisterInterestList()
    {
      FwkInfo("In DoRegisterInterestList()");
      try
      {
        ResetKey(DistinctKeys);
        ResetKey(KeyIndexBegin);
        ResetKey(RegisterKeys);
        string typ = GetStringValue(KeyType); // int is only value to use
        char newType = (typ == null || typ.Length == 0) ? 's' : typ[0];

        IRegion<TKey,TVal> region = GetRegion();
        int numKeys = GetUIntValue(DistinctKeys);  // check distince keys first
        if (numKeys <= 0)
        {
          FwkSevere("DoRegisterInterestList() Failed to initialize keys " +
            "with numKeys: {0}", numKeys);
          return;
        }
        int low = GetUIntValue(KeyIndexBegin);
        low = (low > 0) ? low : 0;
        int numOfRegisterKeys = GetUIntValue(RegisterKeys);
        int high = numOfRegisterKeys + low;
        ClearKeys();
        m_maxKeys = numOfRegisterKeys;
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
          InitStrKeys(low, high, keyBase);
        }

        FwkInfo("DoRegisterInterestList() registering interest for {0} to {1}",
          low, high);
        TKey[] registerKeyList = new TKey[high - low];
        for (int j = low; j < high; j++)
        {
          if (m_keysA[j - low] != null)
          {
            registerKeyList[j - low] = m_keysA[j - low];
          }
          else
          {
            FwkInfo("DoRegisterInterestList() key[{0}] is null.", (j - low));
          }
        }
        bool isDurable = GetBoolValue("isDurableReg");
        ResetKey("getInitialValues"); 
        bool isGetInitialValues = GetBoolValue("getInitialValues");
        bool isReceiveValues = true;
        bool checkReceiveVal = GetBoolValue("checkReceiveVal");
        if (checkReceiveVal)
        {
          ResetKey("receiveValue");
          isReceiveValues = GetBoolValue("receiveValue");
        }
        region.GetSubscriptionService().RegisterKeys(registerKeyList, isDurable, isGetInitialValues, isReceiveValues);
        String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
        if (durableClientId.Length > 0)
        {
          CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
        }
      }
      catch (Exception ex)
      {
        FwkException("DoRegisterInterestList() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterInterestList() complete.");
    }

    public void DoRegisterAllKeys()
    {
      FwkInfo("In DoRegisterAllKeys()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
        bool isDurable = GetBoolValue("isDurableReg");
        ResetKey("getInitialValues");
        bool isGetInitialValues = GetBoolValue("getInitialValues");
        bool checkReceiveVal = GetBoolValue("checkReceiveVal");
        bool isReceiveValues = true;
        if (checkReceiveVal)
        {
          ResetKey("receiveValue");
          isReceiveValues = GetBoolValue("receiveValue");
        }
        region.GetSubscriptionService().RegisterAllKeys(isDurable, null, isGetInitialValues,isReceiveValues);
        String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
        if (durableClientId.Length > 0)
        {
          CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
        }
      }
      catch (Exception ex)
      {
        FwkException("DoRegisterAllKeys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterAllKeys() complete.");
    }

    public void DoVerifyInterestList()
    {
      FwkInfo("In DoVerifyInterestList()");
      try
      {
        int countUpdate = 0;
        IRegion<TKey,TVal> region = GetRegion();
        InitKeys();
        ResetKey(RegisterKeys);
        int numOfRegisterKeys = GetUIntValue(RegisterKeys);
        int payload = GetUIntValue(ValueSizes);

        
        ICollection<TKey> keys = region.GetLocalView().Keys;

        byte[] value;
        int valueSize;
        if (keys != null)
        {
          foreach (TKey key in keys)
          {
             bool containsValue = region.ContainsValueForKey(key);
             RegionEntry<TKey,TVal> entry = region.GetEntry(key);
             if(!containsValue)
             {
                FwkInfo("Skipping check for key {0}",key);
             }
             else
             {
               if (entry == null)
               {
                 FwkException("Failed to find entry for key [{0}] in local cache", key);
               }
               value = entry.Value as byte[];
               if (value == null)
               {
                 FwkException("Failed to find value for key [{0}] in local cache", key);
               }
               valueSize = (value == null ? -1 : value.Length);
               if (valueSize == payload)
               {
                 ++countUpdate;
               }
             }   
             GC.KeepAlive(entry);
          }
        }
        if (countUpdate != numOfRegisterKeys)
        {
          FwkException("DoVerifyInterestList() update interest list " +
            "count {0} is not equal to number of register keys {1}",
            countUpdate, numOfRegisterKeys);
        }
      }
      catch (Exception ex)
      {
        FwkException("DoVerifyInterestList() Caught Exception: {0} : {1}",ex.GetType().Name, ex);
      }
      FwkInfo("DoVerifyInterestList() complete.");
    }

    public void DoRegisterRegexList()
    {
      FwkInfo("In DoRegisterRegexList()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        string regex = GetStringValue(RegisterRegex);
        FwkInfo("DoRegisterRegexList() region name is {0}; regex is {1}",
          region.Name, regex);
        bool isDurable = GetBoolValue("isDurableReg");
        ResetKey("getInitialValues");
        bool isGetInitialValues = GetBoolValue("getInitialValues");
        bool isReceiveValues = true;
        bool checkReceiveVal = GetBoolValue("checkReceiveVal");
        if (checkReceiveVal)
        {
          ResetKey("receiveValue");
          isReceiveValues = GetBoolValue("receiveValue");
        }
        region.GetSubscriptionService().RegisterRegex(regex, isDurable, null, isGetInitialValues, isReceiveValues);
        String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
        if (durableClientId.Length > 0)
        {
          CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
        }
      }
      catch (Exception ex)
      {
        FwkException("DoRegisterRegexList() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterRegexList() complete.");
    }

    public void DoUnRegisterRegexList()
    {
      FwkInfo("In DoUnRegisterRegexList()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        string regex = GetStringValue(UnregisterRegex);
        FwkInfo("DoUnRegisterRegexList() region name is {0}; regex is {1}",
          region.Name, regex);
        region.GetSubscriptionService().UnregisterRegex(regex);
      }
      catch (Exception ex)
      {
        FwkException("DoUnRegisterRegexList() Caught Exception: {0}", ex);
      }
      FwkInfo("DoUnRegisterRegexList() complete.");
    }

    public void DoDestroysKeys()
    {
      FwkInfo("In PerfTest::DoDestroyKeys()");
      try
      {
        IRegion<TKey,TVal> region=GetRegion();
        ResetKey("distinctKeys");
        InitValues(InitKeys());
        DestroyTask<TKey, TVal> destroys = new DestroyTask<TKey, TVal>(region, m_keysA);
        RunTask(destroys,1,m_maxKeys,-1,-1,null);
      }
      catch(Exception e)
      {
        FwkException("PerfTest caught exception: {0}", e);
      }
      FwkInfo("In PerfTest::DoDestroyKeys()complete");
    }

    public void DoServerKeys()
    {
      FwkInfo("In DoServerKeys()");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        FwkAssert(region != null,
          "DoServerKeys() No region to perform operations on.");
        FwkInfo("DoServerKeys() region name is {0}", region.Name);

        int expectedKeys = GetUIntValue(ExpectedCount);
        ICollection<TKey> serverKeys = region.Keys;
        int foundKeys = (serverKeys == null ? 0 : serverKeys.Count);
        FwkAssert(expectedKeys == foundKeys,
          "DoServerKeys() expected {0} keys but found {1} keys.",
          expectedKeys, foundKeys);
      }
      catch (Exception ex)
      {
        FwkException("DoServerKeys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoServerKeys() complete.");
    }

    public void DoIterateInt32Keys() 
    {
      FwkInfo("DoIterateInt32Keys() called.");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        FwkAssert(region != null,
          "DoIterateInt32Keys() No region to perform operations on.");
        FwkInfo("DoIterateInt32Keys() region name is {0}", region.Name);

        ICollection<TKey> serverKeys = region.Keys;
        FwkInfo("DoIterateInt32Keys() GetServerKeys() returned {0} keys.",
          (serverKeys != null ? serverKeys.Count : 0));

        if (serverKeys != null)
        {
          foreach (TKey intKey in serverKeys)
          {
            FwkInfo("ServerKeys: {0}", intKey);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("DoIterateInt32Keys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoIterateInt32Keys() complete.");
    }
    public void DoValidateQConflation()
    {
      FwkInfo("DoValidateQConflation() called.");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        region.GetLocalView().DestroyRegion();
        int expectedAfterCreateEvent = GetUIntValue("expectedAfterCreateCount");
        int expectedAfterUpdateEvent = GetUIntValue("expectedAfterUpdateCount");
        bool isServerConflateTrue = GetBoolValue("isServerConflateTrue");
        Int32 eventAfterCreate = (Int32)Util.BBGet("ConflationCacheListener", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name);
        Int32 eventAfterUpdate = (Int32)Util.BBGet("ConflationCacheListener", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name);
        
        FwkInfo("DoValidateQConflation() -- eventAfterCreate {0} and eventAfterUpdate {1}", eventAfterCreate, eventAfterUpdate);
        String conflateEvent = DistributedSystem.SystemProperties.ConflateEvents;
        String durableClientId = DistributedSystem.SystemProperties.DurableClientId;
        Int32 totalCount = 3500;
        if(durableClientId.Length > 0) {
          FwkInfo("DoValidateQConflation() Validation for Durable client .");
          if (conflateEvent.Equals("true") && ((eventAfterCreate + eventAfterUpdate) < totalCount +10))
          {  
            FwkInfo("DoValidateQConflation() Conflate Events is true complete.");
          }
          else if (conflateEvent.Equals("false") && ((eventAfterCreate + eventAfterUpdate) == totalCount +10))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is false complete.");
          }
          else if (conflateEvent.Equals("server") && isServerConflateTrue && ((eventAfterCreate + eventAfterUpdate) <= totalCount + 10))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is server=true complete.");
          }
          else if (conflateEvent.Equals("server") && !isServerConflateTrue && ((eventAfterCreate + eventAfterUpdate) == totalCount + 10))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is server=false complete.");
          }
          else
          {
            FwkException("DoValidateQConflation() ConflateEvent setting is {0} and Expected AfterCreateCount to have {1} keys and " +
               " found {2} . Expected AfterUpdateCount to have {3} keys, found {4} keys", conflateEvent, expectedAfterCreateEvent
             , eventAfterCreate, expectedAfterUpdateEvent, eventAfterUpdate);
          }
        }
        else {
          if (conflateEvent.Equals("true") && ((eventAfterCreate == expectedAfterCreateEvent) &&
            (((eventAfterUpdate >= expectedAfterUpdateEvent)) && eventAfterUpdate < totalCount)))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is true complete.");
          }
          else if (conflateEvent.Equals("false") && ((eventAfterCreate == expectedAfterCreateEvent) &&
            (eventAfterUpdate == expectedAfterUpdateEvent)))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is false complete.");
          }
          else if (conflateEvent.Equals("server") && isServerConflateTrue && ((eventAfterCreate == expectedAfterCreateEvent) &&
            (((eventAfterUpdate >= expectedAfterUpdateEvent)) && eventAfterUpdate < totalCount)))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is server=true complete.");
          }
          else if (conflateEvent.Equals("server") && !isServerConflateTrue && ((eventAfterCreate == expectedAfterCreateEvent) &&
            (eventAfterUpdate == expectedAfterUpdateEvent)))
          {
            FwkInfo("DoValidateQConflation() Conflate Events is server=false complete.");
          }
          else
          {
            FwkException("DoValidateQConflation() ConflateEvent setting is {0} and Expected AfterCreateCount to have {1} keys and " +
               " found {2} . Expected AfterUpdateCount to have {3} keys, found {4} keys" , conflateEvent,expectedAfterCreateEvent
             , eventAfterCreate,expectedAfterUpdateEvent, eventAfterUpdate);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("DoValidateQConflation() Caught Exception: {0}", ex);
      }
      FwkInfo("DoValidateQConflation() complete.");
    }

    public void DoCreateUpdateDestroy()
    {
      FwkInfo("DoCreateUpdateDestroy() called.");
      try
      {
        DoPopulateRegion();
        DoPopulateRegion();
        DoDestroysKeys();
      }
      catch (Exception ex)
      {
        FwkException("DoCreateUpdateDestroy() Caught Exception: {0}", ex);
      }
        FwkInfo("DoCreateUpdateDestroy() complete.");
    }

    public void DoValidateBankTest()
    {
      FwkInfo("DoValidateBankTest() called.");
      try
      {
        IRegion<TKey,TVal> region = GetRegion();
        region.GetLocalView().DestroyRegion();
        int expectedAfterCreateEvent = GetUIntValue("expectedAfterCreateCount");
        int expectedAfterUpdateEvent = GetUIntValue("expectedAfterUpdateCount");
        int expectedAfterInvalidateEvent = GetUIntValue("expectedAfterInvalidateCount");
        int expectedAfterDestroyEvent = GetUIntValue("expectedAfterDestroyCount");
        Int32 eventAfterCreate = (Int32)Util.BBGet("ConflationCacheListener", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name);
        Int32 eventAfterUpdate = (Int32)Util.BBGet("ConflationCacheListener", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name);
        Int32 eventAfterInvalidate = (Int32)Util.BBGet("ConflationCacheListener", "AFTER_INVALIDATE_COUNT_" + Util.ClientId + "_" + region.Name);
        Int32 eventAfterDestroy = (Int32)Util.BBGet("ConflationCacheListener", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name);


        FwkInfo("DoValidateBankTest() -- eventAfterCreate {0} ,eventAfterUpdate {1} ," +
          "eventAfterInvalidate {2} , eventAfterDestroy {3}", eventAfterCreate, eventAfterUpdate,eventAfterInvalidate,eventAfterDestroy);

        if (expectedAfterCreateEvent == eventAfterCreate && expectedAfterUpdateEvent == eventAfterUpdate &&
           expectedAfterInvalidateEvent == eventAfterInvalidate && expectedAfterDestroyEvent == eventAfterDestroy)
        {
          FwkInfo("DoValidateBankTest() Validation success.");
        }
        else
        {
          FwkException("Validation Failed() Region: {0} eventAfterCreate {1}, eventAfterUpdate {2} " +
             "eventAfterInvalidate {3}, eventAfterDestroy {4}  ",region.Name, eventAfterCreate, eventAfterUpdate
             , eventAfterInvalidate, eventAfterDestroy);
        }
              
      }
      catch (Exception ex)
      {
        FwkException("DoValidateBankTest() Caught Exception: {0}", ex);
      }
      FwkInfo("DoValidateBankTest() complete.");
    }
    #endregion 
  }
}

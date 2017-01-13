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
using GemStone.GemFire.Cache.Tests.NewAPI;
using GemStone.GemFire.DUnitFramework;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;
  public class InitPerfStat : ClientTask
  {
    public Int32 m_cnt;
    public static PerfStat[] perfstat = new PerfStat[10];
    public InitPerfStat()
      : base()
    {
      m_cnt = 0;
    }
    
    public override void DoTask(int iters, object data)
    {
      Int32 localcnt = m_cnt;
      Interlocked.Increment(ref m_cnt);
      perfstat[localcnt] = new PerfStat(Thread.CurrentThread.ManagedThreadId);
    }
  }


  public class PutAllTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private TVal[] m_values;

    #endregion

    #region Public accessors

    public TVal[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = value;
      }
    }

    #endregion

    public PutAllTask(IRegion<TKey, TVal> region, TKey[] keys,
      TVal[] values)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_values = values as TVal[];
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();
        //CacheableHashMap map = new CacheableHashMap();
        map.Clear();
        Util.Log("PutAllTask::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          try
          {
            map.Add(m_keys[idx], m_values[idx]);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
        }
        DateTime startTime = DateTime.Now;
        m_region.PutAll(map, 60);
        DateTime endTime = DateTime.Now;
        TimeSpan elapsedTime = endTime - startTime;
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo("Time Taken to execute putAll for {0}" +
                " is {1}ms", numKeys, elapsedTime.TotalMilliseconds);
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }
  public class GetTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    bool m_isMainWorkLoad;
    public Int32 m_cnt;

    #endregion

    public GetTask(IRegion<TKey, TVal> region, TKey[] keys, bool isMainWorkLoad)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_isMainWorkLoad = isMainWorkLoad;
      m_cnt = 0;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        while (Running && (iters-- != 0))
        {
          object val = null;
          int idx = count % numKeys;
          try
          {
            startTime = InitPerfStat.perfstat[localcnt].StartGet();
            val = m_region[m_keys[idx]];
            //val = m_region.Get(m_keys[idx],null);
            InitPerfStat.perfstat[localcnt].EndGet(startTime, m_isMainWorkLoad);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while getting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          if (val == null)
          {
            string exStr = string.Format("Key[{0}] not found in region {1}",
              m_keys[idx], m_region.Name);
            Util.Log(Util.LogLevel.Error, exStr);
            throw new EntryNotFoundException(exStr);
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class CreateTasks<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private TVal[] m_values;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_assetAcSize;
    private Int32 m_assetmaxVal;

    #endregion

    #region Public accessors

    public TVal[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = value;
      }
    }

    #endregion

    public CreateTasks(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad, Int32 assetACsize, Int32 assetMaxVal)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_values = null;
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
      m_assetAcSize = assetACsize;
      m_assetmaxVal = assetMaxVal;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        Util.Log("CreateTasks::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          try
          {
            TVal obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey, 
              m_encodeTimestamp, m_assetAcSize, m_assetmaxVal, idx);
            startTime = InitPerfStat.perfstat[localcnt].StartCreate();
            //Util.Log("Create Keys is {0} object is {1}", m_keys[idx],obj.ToString());
            m_region.Add(m_keys[idx], obj);
            InitPerfStat.perfstat[localcnt].EndCreate(startTime, m_isMainWorkLoad);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class PutTasks<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;

    #endregion

    public PutTasks(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        Util.Log("PutTasks::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          try
          {
            TVal obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey,
              m_encodeTimestamp, 0, 0, 0);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region[m_keys[idx]] = obj;//.Put(m_keys[idx], obj);
            InitPerfStat.perfstat[localcnt].EndPut(startTime, m_isMainWorkLoad);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
         
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class MeteredPutTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private int m_opsSec;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;

    #endregion


    public MeteredPutTask(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad, int opsSec)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_opsSec = opsSec;
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        int idx;
        PaceMeter pm = new PaceMeter(m_opsSec);
        while (Running && (iters-- != 0))
        {
          idx = count % numKeys;
          try
          {
            TVal obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey,
              m_encodeTimestamp, 0, 0, 0);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region[m_keys[idx]] = obj;//.Put(m_keys[idx], obj);
            InitPerfStat.perfstat[localcnt].EndPut(startTime, m_isMainWorkLoad);
            pm.CheckPace();
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class PutGetMixTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_putPercentage;

    #endregion

    public PutGetMixTask(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad,Int32 putpercentage)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
      m_putPercentage = putpercentage;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        Util.Log("PutGetMixTask::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int n = Util.Rand(1, 100);
          int idx = count % numKeys;

          if (n < m_putPercentage)
          {
            TVal obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey,
              m_encodeTimestamp, 0, 0, 0);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region[m_keys[idx]] = obj;//.Put(m_keys[idx], obj);
            InitPerfStat.perfstat[localcnt].EndPut(startTime, m_isMainWorkLoad);
          }
          else
          {
            TVal val = default(TVal);
            startTime = InitPerfStat.perfstat[localcnt].StartGet();
            val = m_region[m_keys[idx]];
            InitPerfStat.perfstat[localcnt].EndGet(startTime, m_isMainWorkLoad);
          
            if (val == null)
            {
              string exStr = string.Format("Key[{0}] not found in region {1}",
                m_keys[idx], m_region.Name);
              Util.Log(Util.LogLevel.Error, exStr);
              throw new EntryNotFoundException(exStr);
            }
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class RegionQueryTask<TKey, TVal> : ClientTask
  {
    private IRegion<TKey, TVal> m_region;
    private Int32 m_cnt;
    private string m_queryString;
    public RegionQueryTask(IRegion<TKey, TVal> region, string queryString)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_cnt = 0;
      m_queryString = queryString;
    }

    public override void DoTask(int iters, object data)
    {
      Int32 localcnt = m_cnt;
      Interlocked.Increment(ref m_cnt);
      int offset = Util.Rand(100);
      int count = offset;
      long startTime;
      while (Running && (iters-- != 0))
      {
        startTime = InitPerfStat.perfstat[localcnt].StartQuery();
        ISelectResults<object> sptr = m_region.Query<object>(m_queryString, 600);
        InitPerfStat.perfstat[localcnt].EndQuery(startTime, false);
        count++;
      }
      Interlocked.Add(ref m_iters, count - offset);
    }
  }

  public class PutBatchObjectTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_batchSize;
    private Int32 m_batchObjSize;

    #endregion

    public PutBatchObjectTask(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad,Int32 batchSize, Int32 objsize)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
      m_batchSize = batchSize;
      m_batchObjSize = objsize;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        Util.Log("PutBatchObjectTask::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          try
          {
            TVal obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey, m_encodeTimestamp, 
              m_batchSize, m_batchObjSize, idx);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region[m_keys[idx]] = obj;
            InitPerfStat.perfstat[localcnt].EndPut(startTime, m_isMainWorkLoad);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class CreatePutAllMap<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private List<IDictionary<TKey, TVal>> m_maps;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    
    #endregion

    public CreatePutAllMap(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
       List<IDictionary<TKey, TVal>> maps, bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_maps = maps;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
     }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        IDictionary<TKey,TVal> hmoc = new Dictionary<TKey,TVal>();
        lock (m_maps)
        {
          m_maps.Add(hmoc);
        }
        Util.Log("CreatePutAllMap::DoTask: starting {0} iterations. size of map list {1}", iters,m_maps.Count);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          try
          {
            TVal obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey, m_encodeTimestamp, 0, 0, 0);
            //Util.Log("rjk CreatePutAllMap key[{0}] is {1}", idx, m_keys[idx]);
           ((IDictionary<object,object>)(m_maps[localcnt])).Add(m_keys[idx], obj);
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putting key[{0}] for region {1} in iteration " +
              "{2}: {3}", idx, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class PutAllMap<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private List<IDictionary<TKey, TVal>> m_maps;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
   

    #endregion

    public PutAllMap(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      List<IDictionary<TKey, TVal>> maps, bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_maps = maps as List<IDictionary<TKey, TVal>>;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
     }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        Util.Log("PutAllMap::DoTask: starting {0} iterations. size of map list {1}", iters,m_maps.Count);
        while (Running && (iters-- != 0))
        {
          try
          {
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            /*
            foreach (CacheableHashMap map in m_maps)
            {
              Util.Log("PutAllMap:: mape keys = {0} size ={1}", map.Keys,map.Count);
            }
            CacheableHashMap putAllmap;
            lock (m_maps)
            {
              putAllmap = m_maps[localcnt];
            }
            foreach (ICacheableKey key in putAllmap.Keys)
            {
              Util.Log("PutAllMap:: key = {0} ", key);
            }
            foreach (IGFSerializable val in putAllmap.Values)
            {
              Util.Log("PutAllMap:: value = {0} ", val);
            }
            
            foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in putAllmap)
            {
              Util.Log("PutAllMap:: key = {0} value = {1} localcont = {2}", item.Key, item.Value, localcnt);
            }
            */
            m_region.PutAll(m_maps[localcnt], 60);
            InitPerfStat.perfstat[localcnt].EndPut(startTime, m_isMainWorkLoad);
            
          }
          catch (Exception ex)
          {
            Util.Log(Util.LogLevel.Error,
              "Exception while putAll map[{0}] for region {1} in iteration " +
              "{2}: {3}", localcnt, m_region.Name, (count - offset), ex);
            throw;
          }
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class UpdateDeltaTask<TKey, TVal> : ClientTask
  {
    #region Private members

    private IRegion<TKey, TVal> m_region;
    private TKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_assetAcSize;
    private Int32 m_assetmaxVal;

    #endregion

    public UpdateDeltaTask(IRegion<TKey, TVal> region, TKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad, Int32 assetACsize, Int32 assetMaxVal)
      : base()
    {
      m_region = region as IRegion<TKey, TVal>;
      m_keys = keys as TKey[];
      m_cnt = 0;
      m_size = size;
      m_objectType = objectType;
      m_encodeKey = encodeKey;
      m_encodeTimestamp = encodeTimestamp;
      m_isMainWorkLoad = isMainWorkLoad;
      m_assetAcSize = assetACsize;
      m_assetmaxVal = assetMaxVal;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        Int32 localcnt = m_cnt;
        Interlocked.Increment(ref m_cnt);
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        long startTime;
        TVal obj = default(TVal);
        Util.Log("UpdateDeltaTask::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          startTime = InitPerfStat.perfstat[localcnt].StartUpdate();
          if (m_encodeKey)
          {
            obj = m_region[m_keys[idx]];
            if (obj == null)
            {
              string exStr = string.Format("Key[{0}] has not been created in region {1}",
                m_keys[idx], m_region.Name);
              Util.Log(Util.LogLevel.Error, exStr);
              throw new EntryNotFoundException(exStr);
            }
          }
          else {
             obj = ObjectHelper<TKey, TVal>.CreateObject(m_objectType, m_size, m_encodeKey, m_encodeTimestamp, m_assetAcSize, m_assetmaxVal, idx);
          }
          DeltaFastAssetAccount obj1 = obj as DeltaFastAssetAccount;
          if(obj1 == null)
          {
            DeltaPSTObject obj2 = obj as DeltaPSTObject;
            if (obj2 == null)
            {
              m_region[m_keys[idx]] = obj;
            }
            else{
              obj2.Update();
            }
          }
          else
          {
              obj1.Update();
          }
          InitPerfStat.perfstat[localcnt].EndUpdate(startTime, m_isMainWorkLoad);
          count++;
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

}

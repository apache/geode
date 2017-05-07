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

  
  public class PutAllTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Serializable[] m_values;

    #endregion

    #region Public accessors

    public Serializable[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = (Serializable[])value;
      }
    }

    #endregion

    public PutAllTask(Region region, CacheableKey[] keys,
      Serializable[] values)
      : base()
    {
      m_region = region;
      m_keys = keys;
      m_values = values;
    }

    public override void DoTask(int iters, object data)
    {
      if (m_keys != null && m_keys.Length > 0)
      {
        int numKeys = m_keys.Length;
        int offset = Util.Rand(numKeys);
        int count = offset;
        CacheableHashMap map = new CacheableHashMap();
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
        FwkTest.CurrentTest.FwkInfo("Time Taken to execute putAll for {0}" +
                " is {1}ms", numKeys, elapsedTime.TotalMilliseconds);
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }
  public class GetTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    bool m_isMainWorkLoad;
    public Int32 m_cnt;

    #endregion

    public GetTask(Region region, CacheableKey[] keys, bool isMainWorkLoad)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
          IGFSerializable val = null;
          int idx = count % numKeys;
          try
          {
            startTime = InitPerfStat.perfstat[localcnt].StartGet();
            val = m_region.Get(m_keys[idx]);
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

  public class CreateTasks : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Serializable[] m_values;
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

    public Serializable[] Values
    {
      get
      {
        return m_values;
      }
      set
      {
        m_values = (Serializable[])value;
      }
    }

    #endregion

    public CreateTasks(Region region, CacheableKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad, Int32 assetACsize, Int32 assetMaxVal)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
            IGFSerializable obj = ObjectHelper.CreateObject(m_objectType, m_size, m_encodeKey, 
              m_encodeTimestamp, m_assetAcSize, m_assetmaxVal, idx);
            startTime = InitPerfStat.perfstat[localcnt].StartCreate();
            //Util.Log("Create Keys is {0} object is {1}", m_keys[idx],obj.ToString());
            m_region.Create(m_keys[idx], obj);
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

  public class PutTasks : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;

    #endregion

    public PutTasks(Region region, CacheableKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
            IGFSerializable obj = ObjectHelper.CreateObject(m_objectType, m_size, m_encodeKey,
              m_encodeTimestamp, 0, 0, 0);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region.Put(m_keys[idx], obj);
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
          /*
          if (count % 1000 == 0)
          {
            System.GC.Collect();
            System.GC.WaitForPendingFinalizers();
          }
           * */
        }
        Interlocked.Add(ref m_iters, count - offset);
      }
    }
  }

  public class MeteredPutTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private int m_opsSec;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;

    #endregion

    
    public MeteredPutTask(Region region, CacheableKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad, int opsSec)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
            IGFSerializable obj = ObjectHelper.CreateObject(m_objectType, m_size, m_encodeKey,
              m_encodeTimestamp, 0, 0, 0);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region.Put(m_keys[idx], obj);
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

  public class PutGetMixTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_putPercentage;

    #endregion

    public PutGetMixTask(Region region, CacheableKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad,Int32 putpercentage)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
            IGFSerializable obj = ObjectHelper.CreateObject(m_objectType, m_size, m_encodeKey,
              m_encodeTimestamp, 0, 0, 0);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region.Put(m_keys[idx], obj);
            InitPerfStat.perfstat[localcnt].EndPut(startTime, m_isMainWorkLoad);
          }
          else
          {
            IGFSerializable val = null;
            startTime = InitPerfStat.perfstat[localcnt].StartGet();
            val = m_region.Get(m_keys[idx]);
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

  public class RegionQueryTask : ClientTask
  {
    private Region m_region;
    private Int32 m_cnt;
    private string m_queryString;
    public RegionQueryTask(Region region, string queryString)
      : base()
    {
      m_region = region;
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
        ISelectResults sptr = m_region.Query(m_queryString, 600);
        InitPerfStat.perfstat[localcnt].EndQuery(startTime, false);
        count++;
      }
      Interlocked.Add(ref m_iters, count - offset);
    }
  }

  public class PutBatchObjectTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_batchSize;
    private Int32 m_batchObjSize;

    #endregion

    public PutBatchObjectTask(Region region, CacheableKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad,Int32 batchSize, Int32 objsize)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
            IGFSerializable obj = ObjectHelper.CreateObject(m_objectType, m_size, m_encodeKey, m_encodeTimestamp, 
              m_batchSize, m_batchObjSize, idx);
            startTime = InitPerfStat.perfstat[localcnt].StartPut();
            m_region.Put(m_keys[idx], obj);
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

  public class CreatePutAllMap : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private List<CacheableHashMap> m_maps;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    //private Int32 m_batchSize;
    //private Int32 m_batchObjSize;
    
    #endregion

    public CreatePutAllMap(Region region, CacheableKey[] keys, Int32 size, string objectType,
       List<CacheableHashMap> maps, bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
        CacheableHashMap hmoc = new CacheableHashMap();
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
            IGFSerializable obj = ObjectHelper.CreateObject(m_objectType,m_size,m_encodeKey,m_encodeTimestamp,0,0,0);
            //Util.Log("rjk CreatePutAllMap key[{0}] is {1}", idx, m_keys[idx]);
            m_maps[localcnt].Add(m_keys[idx], obj);
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

  public class PutAllMap : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private List<CacheableHashMap> m_maps;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    //private Int32 m_batchSize;
    //private Int32 m_batchObjSize;

    #endregion

    public PutAllMap(Region region, CacheableKey[] keys, Int32 size, string objectType,
      List<CacheableHashMap> maps, bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad)
      : base()
    {
      m_region = region;
      m_keys = keys;
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

  public class UpdateDeltaTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private CacheableKey[] m_keys;
    private Int32 m_cnt;
    private Int32 m_size;
    private string m_objectType;
    private bool m_encodeKey;
    private bool m_encodeTimestamp;
    private bool m_isMainWorkLoad;
    private Int32 m_assetAcSize;
    private Int32 m_assetmaxVal;

    #endregion

    public UpdateDeltaTask(Region region, CacheableKey[] keys, Int32 size, string objectType,
      bool encodeKey, bool encodeTimestamp, bool isMainWorkLoad, Int32 assetACsize, Int32 assetMaxVal)
      : base()
    {
      m_region = region;
      m_keys = keys;
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
        IGFSerializable obj = null;
        Util.Log("UpdateDeltaTask::DoTask: starting {0} iterations.", iters);
        while (Running && (iters-- != 0))
        {
          int idx = count % numKeys;
          startTime = InitPerfStat.perfstat[localcnt].StartUpdate();
          if (m_encodeKey)
          {
            obj = m_region.Get(m_keys[idx]);
            if (obj == null)
            {
              string exStr = string.Format("Key[{0}] has not been created in region {1}",
                m_keys[idx], m_region.Name);
              Util.Log(Util.LogLevel.Error, exStr);
              throw new EntryNotFoundException(exStr);
            }
          }
          else {
            obj = ObjectHelper.CreateObject(m_objectType, m_size, m_encodeKey, m_encodeTimestamp, m_assetAcSize, m_assetmaxVal, idx);
          }
          DeltaFastAssetAccount obj1 = obj as DeltaFastAssetAccount;
          if(obj1 == null)
          {
            DeltaPSTObject obj2 = obj as DeltaPSTObject;
            if (obj2 == null)
            {
              m_region.Put(m_keys[idx], obj);
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

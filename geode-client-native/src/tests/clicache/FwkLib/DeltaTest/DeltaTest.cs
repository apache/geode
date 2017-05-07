using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Xml.Serialization;
using GemStone.GemFire.Cache.Tests;


namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class PutTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private int m_MaxKeys;
    private List<CacheableHashMap> m_maps;
    private Int32 m_update;
    private Int32 m_cnt;
    private bool m_isCreate;
    
    #endregion

    public PutTask(Region region, int keyCnt, List<CacheableHashMap> maps, bool isCreate)
      : base()
    {
      m_region = region;
      m_MaxKeys = keyCnt;
      m_maps = maps;
      m_update = 0;
      m_cnt = 0;
      m_isCreate = isCreate;
    }

    public override void DoTask(int iters, object data)
    {
      FwkTest.CurrentTest.FwkInfo("PutTask::DoTask:");
      Int32 localcnt = m_cnt;
      Interlocked.Increment(ref m_cnt);
      int offset = Util.Rand(m_MaxKeys);
      int count = offset;
      while (Running && (iters-- != 0))
      {
        int idx = count % m_MaxKeys;
        CacheableKey key = null;
        try
        {
          key = new CacheableString("AAAAAA" + localcnt + idx.ToString("D10"));
          DeltaTestImpl oldVal = (m_maps[localcnt])[key] as DeltaTestImpl;
          if (oldVal == null)
          {
            Util.Log(Util.LogLevel.Error, "oldDelta Cannot be null");
          }
          DeltaTestImpl obj = new DeltaTestImpl(oldVal);
          obj.SetIntVar(oldVal.GetIntVar() + 1);
          m_region.Put(key, obj);
          Interlocked.Increment(ref m_update);
          Util.BBSet("ToDeltaBB", key.ToString(), oldVal.GetToDeltaCounter());
          bool removeKey = (m_maps[localcnt]).Remove(key);
          if (removeKey)
          {
            (m_maps[localcnt]).Add(key, obj);
          }
        }
        catch (Exception ex)
        {
          Util.Log(Util.LogLevel.Error,
            "Exception while putting key[{0}] for region {1} in iteration " +
            "{2}: {3}", key, m_region.Name, (count - offset), ex);
          throw;
        }
        count++;
        //if ((count % 1000) == 0)
        //{
        //  Util.Log("PutsTask::DoTask: Intermediate: Ran for 1000 iterations.");
        //}
      }
      //Util.Log("PutsTask::DoTask: Ran for {0} iterations.", count);
      Interlocked.Add(ref m_iters, count - offset);
    }
    public void dumpToBB()
    {
      Int32 localcnt = m_cnt;
      Int32 size = m_maps.Count;
      Int32 count = 0;
      Int32 i = 0;
      while (i < size)
      {
        count += m_maps[i].Count;
        foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in m_maps[i])
        {
          CacheableKey key = item.Key as CacheableKey;
          DeltaTestImpl value = item.Value as DeltaTestImpl;
          Util.BBSet("ToDeltaBB", key.ToString(), value.GetToDeltaCounter());
        }
        i++;
      }
      Util.BBSet("MapCount", "size", count);
      Util.BBSet("DeltaBB", "UPDATECOUNT", m_update);
    }
  }

  public class CreateTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private int m_MaxKeys;
    private List<CacheableHashMap> m_maps;
    private Int32 m_create;
    private Int32 m_cnt;


    #endregion

    public CreateTask(Region region, int keyCnt, List<CacheableHashMap> maps)
      : base()
    {
      m_region = region;
      m_MaxKeys = keyCnt;
      m_maps = maps;
      m_create = 0;
      m_cnt = 0;
    }

    public override void DoTask(int iters, object data)
    {
      Int32 localcnt = m_cnt;
      Interlocked.Increment(ref m_cnt);
      CacheableHashMap hmoc = new CacheableHashMap();
      lock (m_maps)
      {
        m_maps.Add(hmoc);
      }
      int offset = Util.Rand(m_MaxKeys);
      int count = offset;
      Util.Log("CreateTask::DoTask: starting {0} iterations.", iters);
      while (Running && (iters-- != 0))
      {
        int idx = count % m_MaxKeys;
        CacheableKey key = null;
        try
        {
          key = new CacheableString("AAAAAA" + localcnt + idx.ToString("D10"));
          DeltaTestImpl obj = new DeltaTestImpl(0, CacheableString.Create("delta"));
          m_region.Create(key, obj);
          Interlocked.Increment(ref m_create);
          (m_maps[localcnt]).Add(key, obj);
        }
        catch (Exception ex)
        {
          Util.Log(Util.LogLevel.Error,
            "Exception while creating key[{0}] for region {1} in iteration " +
            "{2}: {3}", key, m_region.Name, (count - offset), ex);
          throw;
        }
        count++;
      }
      Interlocked.Add(ref m_iters, count - offset);
    }
    public void dumpToBB()
    {
      Util.BBSet("DeltaBB", "CREATECOUNT", m_create);
      Util.BBSet("DeltaBB", "DESTROYCOUNT", 0);
    }
  }

  public class EntryTask : ClientTask
  {
    #region Private members

    private Region m_region;
    private int m_MaxKeys;
    private List<CacheableHashMap> m_maps;
    private Int32 m_create;
    private Int32 m_update;
    private Int32 m_destroy;
    private Int32 m_invalidate;
    private Int32 m_cnt;
    bool m_isDestroy;
    private object CLASS_LOCK = new object();
    
    #endregion

    public EntryTask(Region region, int keyCnt, List<CacheableHashMap> maps)
      : base()
    {
      m_region = region;
      m_MaxKeys = keyCnt;
      m_maps = maps;
      m_create = 0;
      m_update = 0;
      m_destroy = 0;
      m_invalidate = 0;
      m_cnt = 0;
      m_isDestroy = true;
    }

    DeltaTestImpl getLatestDelta(CacheableKey key, Int32 localcnt, bool isCreate)
    {
      DeltaTestImpl oldValue = (m_maps[localcnt])[key] as DeltaTestImpl;
      if (oldValue == null)
      {
        FwkTest.CurrentTest.FwkInfo("oldDelta cannot be null");
      }
      DeltaTestImpl obj = new DeltaTestImpl(oldValue.GetIntVar() + 1, CacheableString.Create("delta"));
      if (!isCreate)
      {
        obj.SetIntVar(oldValue.GetIntVar() + 1);
      }
      return obj;
    }

    public override void DoTask(int iters, object data)
    {
      Int32 localcnt = m_cnt;
      Interlocked.Increment(ref m_cnt);
      CacheableHashMap hmoc = new CacheableHashMap();
      lock (m_maps)
      {
        m_maps.Add(hmoc);
      }
      int offset = Util.Rand(m_MaxKeys);
      int count = offset;
      CacheableKey key = null;
      Util.Log("EntryTask::DoTask: starting {0} iterations.", iters);
      while (Running && (iters-- != 0))
      {
        int idx = count % m_MaxKeys;
        key = new CacheableString("AAAAAA" + localcnt + idx.ToString("D10"));
        string opcode = FwkTest.CurrentTest.GetStringValue("entryOps");
        if (opcode == null) opcode = "no-opcode";
        if (opcode == "put")
        {
          lock (CLASS_LOCK)
          {
            DeltaTestImpl newValue = null;
            if (m_region.ContainsKey(key))
            {
              DeltaTestImpl oldValue = m_region.Get(key) as DeltaTestImpl;
              if (oldValue == null)
              {
                newValue = getLatestDelta(key, localcnt, false);
                m_region.Put(key, newValue);
              }
              else
              {
                newValue = new DeltaTestImpl(oldValue);
                newValue.SetIntVar(oldValue.GetIntVar() + 1);
                m_region.Put(key, newValue);
              }
              Interlocked.Increment(ref m_update);
              //Util.BBSet("ToDeltaBB", key.ToString(), newValue.GetToDeltaCounter());
            }
            else
            {
              newValue = getLatestDelta(key, localcnt, true);
              m_region.Create(key, newValue);
              Interlocked.Increment(ref m_create);
            }
            //(m_maps[localcnt]).Add(key, newValue);
            m_maps[localcnt][key] = newValue;
          }
        }
        else if (opcode == "destroy")
        {
          DeltaTestImpl oldValue = null;
          if (m_region.ContainsKey(key))
          {
            if ((oldValue = m_region.Get(key) as DeltaTestImpl) == null)
            {
              if (m_isDestroy)
              {
                 m_region.Destroy(key);
                (m_maps[localcnt]).Remove(key);
              }
            }
            else
            {
              m_maps[localcnt][key] = oldValue;
              m_region.Destroy(key);
              //(m_maps[localcnt]).Remove(key);
            }
            Interlocked.Increment(ref m_destroy);
           } 
        }
        else if (opcode == "invalidate")
        {
          DeltaTestImpl oldValue = null;
          if (m_region.ContainsKey(key))
          {
            if ((oldValue = m_region.Get(key) as DeltaTestImpl) != null)
            {
              m_maps[localcnt].Add(key, oldValue);
              m_region.Invalidate(key);
              Interlocked.Increment(ref m_invalidate);
              m_maps[localcnt].Add(key, null);
            }
          }
        }
      }
      Interlocked.Add(ref m_iters, count - offset);
    }

    public void dumpToBB()
    {
      Int32 localcnt = m_cnt;
      Int32 size = m_maps.Count;
      Int32 count = 0;
      Int32 i = 0;
      while(i < size)
      {
        count += m_maps[i].Count;
        foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in m_maps[i])
        {
          CacheableKey key = item.Key as CacheableKey;
          DeltaTestImpl value = item.Value as DeltaTestImpl;
          Util.BBSet("ToDeltaBB", key.ToString(), value.GetToDeltaCounter());
         }
         i++;
      }
      Util.BBSet("MapCount", "size", count);
      Int32 createCnt = (Int32)Util.BBGet("DeltaBB", "CREATECOUNT");
      Util.BBSet("DeltaBB", "CREATECOUNT", createCnt + m_create);
      Util.BBSet("DeltaBB", "UPDATECOUNT", m_update);
      Util.BBSet("DeltaBB", "DESTROYCOUNT", m_destroy);
    }

  }

  public class DeltaTest : FwkTest
  {
    protected CacheableKey[] m_keysA;
    protected int m_maxKeys;
    protected int m_keyIndexBegin;

    protected CacheableBytes[] m_cValues;
    protected int m_maxValues;

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
    protected char m_keyType = 'i';
    protected static List<CacheableHashMap> mapList = new List<CacheableHashMap>();
    private static bool isObjectRegistered = false;


    protected void ClearKeys()
    {
      if (m_keysA != null)
      {
        for (int i = 0; i < m_keysA.Length; i++)
        {
          if (m_keysA[i] != null)
          {
            m_keysA[i].Dispose();
            m_keysA[i] = null;
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
          CacheableKey tmp = m_keysA[j];
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
      m_keysA = new CacheableString[m_maxKeys];
      FwkInfo("m_maxKeys: {0}; low: {1}; high: {2}",
        m_maxKeys, low, high);
      for (int i = low; i < high; i++)
      {
        m_keysA[i - low] = new CacheableString(keyBase + i.ToString("D10"));
      }
    }

    protected void InitIntKeys(int low, int high)
    {
      m_keysA = new CacheableInt32[m_maxKeys];
      FwkInfo("m_maxKeys: {0}; low: {1}; high: {2}",
        m_maxKeys, low, high);
      for (int i = low; i < high; i++)
      {
        m_keysA[i - low] = new CacheableInt32(i);
      }
    }
    protected Region GetRegion()
    {
      return GetRegion(null);
    }

    protected Region GetRegion(string regionName)
    {
      Region region;
      if (regionName == null)
      {
        region = GetRootRegion();
        if (region == null)
        {
          Region[] rootRegions = CacheHelper.DCache.RootRegions();
          if (rootRegions != null && rootRegions.Length > 0)
          {
            region = rootRegions[Util.Rand(rootRegions.Length)];
          }
        }
      }
      else
      {
        region = CacheHelper.GetRegion(regionName);
      }
      return region;
    }
    public DeltaTest()
    {
      //FwkInfo("In DeltaTest()");
    }
    public static ICacheListener CreateDeltaValidationCacheListener()
    {
      return new DeltaClientValidationListener();
    }

   public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        if (!isObjectRegistered)
        {
          Serializable.RegisterType(DeltaTestImpl.CreateDeserializable);
          Serializable.RegisterType(TestObject1.CreateDeserializable);
          isObjectRegistered = true;
        }
          Region region = CreateRootRegion();
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
        Region region = GetRegion();
        FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
        bool isDurable = GetBoolValue("isDurableReg");
        ResetKey("getInitialValues");
        bool isGetInitialValues = GetBoolValue("getInitialValues");
        region.RegisterAllKeys(isDurable, null, isGetInitialValues);
      }
      catch (Exception ex)
      {
        FwkException("DoRegisterAllKeys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterAllKeys() complete.");
    }
    public void DoPuts()
    {
      FwkInfo("In DoPuts()");
      try
      {
        Region region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        string label = CacheHelper.RegionTag(region.Attributes);
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

          ResetKey(NumThreads);
          int numThreads;
          while ((numThreads = GetUIntValue(NumThreads)) > 0)
          {
            PutTask puts = new PutTask(region, numKeys / numThreads, mapList, true);
            FwkInfo("Running timed task ");
            try
            {
              RunTask(puts, numThreads, -1, timedInterval, maxTime, null);
            }
            catch (ClientTimeoutException)
            {
              FwkException("In DoPuts()  Timed run timed out.");
            }
            puts.dumpToBB();
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          }
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
        Region region = GetRegion();
        ResetKey(DistinctKeys);
        int numKeys = InitKeys();
        ResetKey(NumThreads);
        int numThreads = GetUIntValue(NumThreads);
        CreateTask creates = new CreateTask(region, (numKeys / numThreads), mapList);
        FwkInfo("Populating region.");
        RunTask(creates, numThreads, (numKeys / numThreads), -1, -1, null);
        creates.dumpToBB();
      }
      catch (Exception ex)
      {
        FwkException("DoPopulateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoPopulateRegion() complete.");
    }

    public void DoEntryOperation()
    {
      FwkInfo("In DoEntryOperation");
      try
      {
        Region region = GetRegion();
        int numClients = GetUIntValue(ClientCount);
        string label = CacheHelper.RegionTag(region.Attributes);
        int timedInterval = GetTimeValue(TimedInterval) * 1000;
        {
          timedInterval = 5000;
        }
        int maxTime = 10 * timedInterval;

        // Loop over key set sizes
        ResetKey(DistinctKeys);
        int numKeys = GetUIntValue(DistinctKeys);
        ResetKey(NumThreads);
        int numThreads;
        while ((numThreads = GetUIntValue(NumThreads)) > 0)
        {
          EntryTask entrytask = new EntryTask(region, numKeys / numThreads, mapList);
          FwkInfo("Running timed task ");
          try
          {
            RunTask(entrytask, numThreads, -1, timedInterval, maxTime, null);
          }
          catch (ClientTimeoutException)
          {
            FwkException("In DoPuts()  Timed run timed out.");
          }
          Thread.Sleep(3000);
          entrytask.dumpToBB();
        }
      }
      catch (Exception ex)
      {
        FwkException("DoEntryOperation() Caught Exception: {0}", ex);
      }
      FwkInfo("DoEntryOperation() complete.");
    }

    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      CacheHelper.Close();
    }
    public void DoValidateDeltaTest()
    {
      FwkInfo("DoValidateDeltaTest() called.");
      try
      {
        Region region = GetRegion();
        region.LocalDestroyRegion();
        CacheableKey key = null;
        Int32 expectedAfterCreateEvent = (Int32)Util.BBGet("DeltaBB", "CREATECOUNT");
        Int32 expectedAfterUpdateEvent = (Int32)Util.BBGet("DeltaBB", "UPDATECOUNT");
        Int32 expectedAfterDestroyEvent = (Int32)Util.BBGet("DeltaBB", "DESTROYCOUNT");
        long eventAfterCreate = (long)Util.BBGet("DeltaBB", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name);
        long eventAfterUpdate = (long)Util.BBGet("DeltaBB", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name);
        long eventAfterDestroy = (long)Util.BBGet("DeltaBB", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name);
        FwkInfo("DoValidateDeltaTest() -- eventAfterCreate {0} ,eventAfterUpdate {1} ,eventAfterDestroy {2}", eventAfterCreate, eventAfterUpdate, eventAfterDestroy);
        FwkInfo("DoValidateDeltaTest() -- expectedAfterCreateEvent {0} ,expectedAfterUpdateEvent {1}, expectedAfterDestroyEvent {2} ", expectedAfterCreateEvent, expectedAfterUpdateEvent, expectedAfterDestroyEvent);
        if (expectedAfterCreateEvent == eventAfterCreate && expectedAfterUpdateEvent == eventAfterUpdate && expectedAfterDestroyEvent == eventAfterDestroy)
        {
          DeltaClientValidationListener cs = (region.Attributes.CacheListener)as DeltaClientValidationListener;
          CacheableHashMap map = cs.getMap();
          Int32 mapCount = map.Count;
          Int32 toDeltaMapCount = (Int32)Util.BBGet("MapCount", "size");
          if (mapCount == toDeltaMapCount)
          {
            foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in map)
            {
              key = item.Key as CacheableKey;
              CacheableInt64 value = item.Value as CacheableInt64;
              long fromDeltaCount = (long)value;
              long toDeltaCount = (long)Util.BBGet("ToDeltaBB", key.ToString());
              if (toDeltaCount == fromDeltaCount)
              {
                FwkInfo("DoValidateDeltaTest() Delta Count Validation success with fromDeltaCount: {0} = toDeltaCount: {1}", fromDeltaCount, toDeltaCount);
              }
            }
            FwkInfo("DoValidateDeltaTest() Validation success.");
          }
          else
          {
            FwkException("Validation Failed() as fromDeltaMapCount: {0} is not equal to toDeltaMapCount: {1}",mapCount,toDeltaMapCount);
          }
        }
        else
        {
          FwkException("Validation Failed()for Region: {0} Expected were expectedAfterCreateEvent {1} expectedAfterUpdateEvent {2} expectedAfterDestroyEvent {3} eventAfterCreate {4}, eventAfterUpdate {5} ", region.Name, expectedAfterCreateEvent, expectedAfterUpdateEvent,expectedAfterDestroyEvent, eventAfterCreate, eventAfterUpdate ,eventAfterDestroy);
        }

      }
      catch (Exception ex)
      {
        FwkException("DoValidateDeltaTest() Caught Exception: {0}", ex);
        FwkInfo("DoValidateDeltaTest() complete.");
      }
    }
  }
}//DeltaTest end
